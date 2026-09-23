import { after, before, describe, test } from 'node:test'
import assert from 'node:assert'
import { RedisClientType } from 'redis'
import { TestRunner, duplicateNodeRedisClient } from '../../test-config'
import { randomKey } from '../../utils'

const testRunner = new TestRunner()

type KeyspaceEvent = { channel: string; message: string }

// node-redis delivers pattern messages to a single per-subscribe listener, so
// fan them out to per-test waiters/collectors through a small bus.
class EventBus {
  private readonly listeners = new Set<(event: KeyspaceEvent) => void>()

  readonly handler = (message: string, channel: string): void => {
    for (const listener of this.listeners) listener({ channel, message })
  }

  waitForEvent(
    channel: string,
    message: string,
    timeout = 1000,
  ): Promise<true> {
    return new Promise((resolve, reject) => {
      const onEvent = (event: KeyspaceEvent) => {
        if (event.channel !== channel || event.message !== message) return
        cleanup()
        resolve(true)
      }
      const cleanup = () => {
        clearTimeout(timer)
        this.listeners.delete(onEvent)
      }
      const timer = setTimeout(() => {
        cleanup()
        reject(new Error(`Timed out waiting for ${channel} = ${message}`))
      }, timeout)
      this.listeners.add(onEvent)
    })
  }

  collect(): KeyspaceEvent[] {
    const events: KeyspaceEvent[] = []
    this.listeners.add(event => events.push(event))
    return events
  }
}

describe(`Keyspace notifications (node-redis, ${testRunner.getBackendName()})`, () => {
  // Every client is a duplicate() of one standalone client: a new connection to
  // the same server (mock/real), or to the same in-memory keyspace (socketless).
  let base: RedisClientType
  const clients: RedisClientType[] = []

  before(async () => {
    base = await testRunner.setupNodeRedisStandalone()
  })

  after(async () => {
    for (const client of clients) client.destroy()
    clients.length = 0
    await testRunner.cleanup()
  })

  test('publishes set keyspace and keyevent notifications', async () => {
    const actor = await connect()
    const { subscriber, bus } = await subscribe([
      '__keyspace@0__:*',
      '__keyevent@0__:*',
    ])
    await actor.configSet('notify-keyspace-events', 'KEA')
    const key = randomKey()

    const keyspace = bus.waitForEvent(`__keyspace@0__:${key}`, 'set')
    const keyevent = bus.waitForEvent(`__keyevent@0__:set`, key)
    await actor.set(key, 'v')

    assert.strictEqual(await keyspace, true)
    assert.strictEqual(await keyevent, true)
    void subscriber
  })

  test('publishes del, expire and persist generic notifications', async () => {
    const actor = await connect()
    await actor.configSet('notify-keyspace-events', 'KEA')
    const key = randomKey()
    await actor.set(key, 'v')

    const { bus } = await subscribe(['__keyevent@0__:*'])

    const expired = bus.waitForEvent(`__keyevent@0__:expire`, key)
    await actor.expire(key, 100)
    assert.strictEqual(await expired, true)

    const persisted = bus.waitForEvent(`__keyevent@0__:persist`, key)
    await actor.persist(key)
    assert.strictEqual(await persisted, true)

    const deleted = bus.waitForEvent(`__keyevent@0__:del`, key)
    await actor.del(key)
    assert.strictEqual(await deleted, true)
  })

  test('names write events after the originating command', async () => {
    const actor = await connect()
    await actor.configSet('notify-keyspace-events', 'KEA')
    const { bus } = await subscribe(['__keyevent@0__:*'])

    const listKey = randomKey()
    const hashKey = randomKey()
    const setKey = randomKey()
    const zsetKey = randomKey()
    const counterKey = randomKey()

    const lpush = bus.waitForEvent(`__keyevent@0__:lpush`, listKey)
    await actor.lPush(listKey, 'a')
    assert.strictEqual(await lpush, true)

    const hset = bus.waitForEvent(`__keyevent@0__:hset`, hashKey)
    await actor.hSet(hashKey, 'f', 'v')
    assert.strictEqual(await hset, true)

    const sadd = bus.waitForEvent(`__keyevent@0__:sadd`, setKey)
    await actor.sAdd(setKey, 'm')
    assert.strictEqual(await sadd, true)

    const zadd = bus.waitForEvent(`__keyevent@0__:zadd`, zsetKey)
    await actor.zAdd(zsetKey, { score: 1, value: 'm' })
    assert.strictEqual(await zadd, true)

    // INCR reports as `incrby`, matching real Redis.
    const incrby = bus.waitForEvent(`__keyevent@0__:incrby`, counterKey)
    await actor.incr(counterKey)
    assert.strictEqual(await incrby, true)
  })

  test('publishes expired event when a key lazily expires', async () => {
    const actor = await connect()
    await actor.configSet('notify-keyspace-events', 'KEA')
    const key = randomKey()

    const { bus } = await subscribe(['__keyevent@0__:*'])

    const expired = bus.waitForEvent(`__keyevent@0__:expired`, key, 3000)
    await actor.set(key, 'v', { expiration: { type: 'PX', value: 50 } })
    await new Promise(resolve => setTimeout(resolve, 120))
    await actor.get(key)

    assert.strictEqual(await expired, true)
  })

  test('publishes expired event from active expiry without a forcing read', async () => {
    const actor = await connect()
    await actor.configSet('notify-keyspace-events', 'KEA')
    const key = randomKey()

    const { bus } = await subscribe(['__keyevent@0__:*'])

    const expired = bus.waitForEvent(`__keyevent@0__:expired`, key, 3000)
    await actor.set(key, 'v', { expiration: { type: 'PX', value: 50 } })

    assert.strictEqual(await expired, true)
  })

  test('translates RENAME into rename_from and rename_to', async () => {
    const actor = await connect()
    await actor.configSet('notify-keyspace-events', 'KEA')
    const src = randomKey()
    const dst = randomKey()
    await actor.set(src, 'v')

    const { bus } = await subscribe(['__keyevent@0__:*'])

    const renameFrom = bus.waitForEvent(`__keyevent@0__:rename_from`, src)
    const renameTo = bus.waitForEvent(`__keyevent@0__:rename_to`, dst)
    await actor.rename(src, dst)

    assert.strictEqual(await renameFrom, true)
    assert.strictEqual(await renameTo, true)
  })

  test('does not name a cross-database write after an earlier SELECT', async () => {
    // SELECT runs against the database selected *before* it, so that is the
    // one tagged with the active command name. Restoring the tag through the
    // live `ctx.db` getter hit the newly selected database instead, leaving the
    // old one tagged `select` — and since every later command restores the tag
    // it saved, the stale value was never cleared. COPY ... DB and MOVE write
    // into a database the executor never tags, so their events there were
    // published as `select` (#359). Real Redis names them copy_to / move_to.
    const actor = await connect()
    const sentinelWriter = await connect()
    await actor.configSet('notify-keyspace-events', 'KEA')
    const source = randomKey()
    const copied = randomKey()
    const moved = randomKey()
    const sentinel = randomKey()

    const { bus } = await subscribe(['__keyspace@0__:*', '__keyevent@0__:*'])
    const events = bus.collect()

    await actor.select(1)
    await actor.set(source, 'v')
    assert.strictEqual(await actor.copy(source, copied, { DB: 0 }), 1)
    await actor.set(moved, 'v')
    assert.strictEqual(await actor.move(moved, 0), 1)

    // Pub/sub delivery to one subscriber is ordered: once this db0 event
    // arrives, every event published before it has too. It also proves the
    // subscription is live, so the assertion below cannot pass vacuously.
    const flushed = bus.waitForEvent(`__keyevent@0__:set`, sentinel)
    await sentinelWriter.set(sentinel, 'v')
    assert.strictEqual(await flushed, true)

    assert.deepStrictEqual(
      events.filter(
        e =>
          e.channel === `__keyevent@0__:select` ||
          (e.channel.startsWith('__keyspace@0__:') && e.message === 'select'),
      ),
      [],
    )
  })

  test('delivers nothing when notify-keyspace-events is disabled', async () => {
    const actor = await connect()
    await actor.configSet('notify-keyspace-events', '')
    const key = randomKey()

    const { bus } = await subscribe(['__keyspace@0__:*', '__keyevent@0__:*'])

    const events = bus.collect()
    await actor.set(key, 'v')
    await actor.del(key)
    await new Promise(resolve => setTimeout(resolve, 300))

    assert.deepStrictEqual(events, [])
  })

  test('gates events by configured class', async () => {
    const actor = await connect()
    await actor.configSet('notify-keyspace-events', 'Ex')
    const setKey = randomKey()
    const expiringKey = randomKey()

    const { bus } = await subscribe(['__keyevent@0__:*'])

    const setEvents = bus.collect()
    await actor.set(setKey, 'v')
    await new Promise(resolve => setTimeout(resolve, 250))
    assert.strictEqual(
      setEvents.some(e => e.channel === `__keyevent@0__:set`),
      false,
      'set event must be gated out under class "x"',
    )

    const expired = bus.waitForEvent(
      `__keyevent@0__:expired`,
      expiringKey,
      3000,
    )
    await actor.set(expiringKey, 'v', { expiration: { type: 'PX', value: 50 } })
    await new Promise(resolve => setTimeout(resolve, 120))
    await actor.get(expiringKey)
    assert.strictEqual(await expired, true)
  })

  test('CONFIG normalizes flags and rejects invalid characters', async () => {
    const actor = await connect()

    await actor.configSet('notify-keyspace-events', 'KEA')
    assert.deepStrictEqual(await actor.configGet('notify-keyspace-events'), {
      'notify-keyspace-events': 'AKE',
    })

    await actor.configSet('notify-keyspace-events', 'KEg$')
    assert.deepStrictEqual(await actor.configGet('notify-keyspace-events'), {
      'notify-keyspace-events': 'g$KE',
    })

    await assert.rejects(
      () => actor.configSet('notify-keyspace-events', 'Z'),
      /Invalid event class character/,
    )

    await actor.configSet('notify-keyspace-events', '')
  })

  async function connect(): Promise<RedisClientType> {
    const client = await duplicateNodeRedisClient(base)
    clients.push(client)
    return client
  }

  async function subscribe(
    patterns: string[],
  ): Promise<{ subscriber: RedisClientType; bus: EventBus }> {
    const subscriber = await connect()
    const bus = new EventBus()
    await subscriber.pSubscribe(patterns, bus.handler)
    await new Promise(resolve => setTimeout(resolve, 50))
    return { subscriber, bus }
  }
})
