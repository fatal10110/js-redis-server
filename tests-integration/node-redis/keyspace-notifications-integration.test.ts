import { after, before, describe, test } from 'node:test'
import assert from 'node:assert'
import { createClient, RedisClientType } from 'redis'
import { TestRunner } from '../test-config'
import { randomKey } from '../utils'

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
  let port: number
  const clients: RedisClientType[] = []

  before(async () => {
    port = await testRunner.setupRawStandalone()
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
    await actor.sendCommand(['CONFIG', 'SET', 'notify-keyspace-events', 'KEA'])
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
    await actor.sendCommand(['CONFIG', 'SET', 'notify-keyspace-events', 'KEA'])
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
    await actor.sendCommand(['CONFIG', 'SET', 'notify-keyspace-events', 'KEA'])
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
    await actor.sendCommand(['CONFIG', 'SET', 'notify-keyspace-events', 'KEA'])
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
    await actor.sendCommand(['CONFIG', 'SET', 'notify-keyspace-events', 'KEA'])
    const key = randomKey()

    const { bus } = await subscribe(['__keyevent@0__:*'])

    const expired = bus.waitForEvent(`__keyevent@0__:expired`, key, 3000)
    await actor.set(key, 'v', { expiration: { type: 'PX', value: 50 } })

    assert.strictEqual(await expired, true)
  })

  test('translates RENAME into rename_from and rename_to', async () => {
    const actor = await connect()
    await actor.sendCommand(['CONFIG', 'SET', 'notify-keyspace-events', 'KEA'])
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

  test('delivers nothing when notify-keyspace-events is disabled', async () => {
    const actor = await connect()
    await actor.sendCommand(['CONFIG', 'SET', 'notify-keyspace-events', ''])
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
    await actor.sendCommand(['CONFIG', 'SET', 'notify-keyspace-events', 'Ex'])
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

    await actor.sendCommand(['CONFIG', 'SET', 'notify-keyspace-events', 'KEA'])
    assert.strictEqual(
      configValue(
        await actor.sendCommand(['CONFIG', 'GET', 'notify-keyspace-events']),
      ),
      'AKE',
    )

    await actor.sendCommand(['CONFIG', 'SET', 'notify-keyspace-events', 'KEg$'])
    assert.strictEqual(
      configValue(
        await actor.sendCommand(['CONFIG', 'GET', 'notify-keyspace-events']),
      ),
      'g$KE',
    )

    await assert.rejects(
      () => actor.sendCommand(['CONFIG', 'SET', 'notify-keyspace-events', 'Z']),
      /Invalid event class character/,
    )

    await actor.sendCommand(['CONFIG', 'SET', 'notify-keyspace-events', ''])
  })

  async function connect(): Promise<RedisClientType> {
    const client = createClient({
      url: `redis://127.0.0.1:${port}`,
    }) as RedisClientType
    client.on('error', () => {})
    await client.connect()
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

// CONFIG GET is a flat array on RESP2 and an object on RESP3.
function configValue(reply: unknown): string {
  if (Array.isArray(reply)) return String(reply[1])
  return String((reply as Record<string, unknown>)['notify-keyspace-events'])
}
