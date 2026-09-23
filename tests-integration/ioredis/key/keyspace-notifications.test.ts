import { after, before, describe, test } from 'node:test'
import assert from 'node:assert'
import { Redis } from 'ioredis'
import { TestRunner } from '../../test-config'
import { randomKey } from '../../utils'

const testRunner = new TestRunner()

describe(`Keyspace notifications (${testRunner.getBackendName()})`, () => {
  // Every client is a duplicate() of one standalone client: a new connection to
  // the same server (mock/real), or to the same in-memory keyspace (socketless).
  let base: Redis
  const clients: Redis[] = []

  before(async () => {
    base = await testRunner.setupIoredisStandalone()
  })

  after(async () => {
    for (const client of clients) {
      client.disconnect()
    }
    clients.length = 0
    await testRunner.cleanup()
  })

  test('publishes set keyspace and keyevent notifications', async () => {
    const actor = await connect()
    const subscriber = await connect()
    await actor.config('SET', 'notify-keyspace-events', 'KEA')
    const key = randomKey()

    await subscriber.psubscribe(`__keyspace@0__:*`, `__keyevent@0__:*`)
    await settle()

    const keyspace = waitForEvent(subscriber, `__keyspace@0__:${key}`, 'set')
    const keyevent = waitForEvent(subscriber, `__keyevent@0__:set`, key)
    await actor.set(key, 'v')

    assert.strictEqual(await keyspace, true)
    assert.strictEqual(await keyevent, true)
  })

  test('publishes del, expire and persist generic notifications', async () => {
    const actor = await connect()
    const subscriber = await connect()
    await actor.config('SET', 'notify-keyspace-events', 'KEA')
    const key = randomKey()
    await actor.set(key, 'v')

    await subscriber.psubscribe(`__keyevent@0__:*`)
    await settle()

    const expired = waitForEvent(subscriber, `__keyevent@0__:expire`, key)
    await actor.expire(key, 100)
    assert.strictEqual(await expired, true)

    const persisted = waitForEvent(subscriber, `__keyevent@0__:persist`, key)
    await actor.persist(key)
    assert.strictEqual(await persisted, true)

    const deleted = waitForEvent(subscriber, `__keyevent@0__:del`, key)
    await actor.del(key)
    assert.strictEqual(await deleted, true)
  })

  test('names write events after the originating command', async () => {
    const actor = await connect()
    const subscriber = await connect()
    await actor.config('SET', 'notify-keyspace-events', 'KEA')
    await subscriber.psubscribe(`__keyevent@0__:*`)
    await settle()

    const listKey = randomKey()
    const hashKey = randomKey()
    const setKey = randomKey()
    const zsetKey = randomKey()
    const counterKey = randomKey()

    const lpush = waitForEvent(subscriber, `__keyevent@0__:lpush`, listKey)
    await actor.lpush(listKey, 'a')
    assert.strictEqual(await lpush, true)

    const hset = waitForEvent(subscriber, `__keyevent@0__:hset`, hashKey)
    await actor.hset(hashKey, 'f', 'v')
    assert.strictEqual(await hset, true)

    const sadd = waitForEvent(subscriber, `__keyevent@0__:sadd`, setKey)
    await actor.sadd(setKey, 'm')
    assert.strictEqual(await sadd, true)

    const zadd = waitForEvent(subscriber, `__keyevent@0__:zadd`, zsetKey)
    await actor.zadd(zsetKey, '1', 'm')
    assert.strictEqual(await zadd, true)

    // INCR reports as `incrby`, matching real Redis.
    const incrby = waitForEvent(subscriber, `__keyevent@0__:incrby`, counterKey)
    await actor.incr(counterKey)
    assert.strictEqual(await incrby, true)
  })

  test('publishes expired event when a key lazily expires', async () => {
    const actor = await connect()
    const subscriber = await connect()
    await actor.config('SET', 'notify-keyspace-events', 'KEA')
    const key = randomKey()

    await subscriber.psubscribe(`__keyevent@0__:*`)
    await settle()

    const expired = waitForEvent(
      subscriber,
      `__keyevent@0__:expired`,
      key,
      3000,
    )
    await actor.set(key, 'v', 'PX', 50)
    // Force a read after the TTL so a lazy backend evicts and notifies.
    await new Promise(resolve => setTimeout(resolve, 120))
    await actor.get(key)

    assert.strictEqual(await expired, true)
  })

  test('publishes expired event from active expiry without a forcing read', async () => {
    const actor = await connect()
    const subscriber = await connect()
    await actor.config('SET', 'notify-keyspace-events', 'KEA')
    const key = randomKey()

    await subscriber.psubscribe(`__keyevent@0__:*`)
    await settle()

    const expired = waitForEvent(
      subscriber,
      `__keyevent@0__:expired`,
      key,
      3000,
    )
    await actor.set(key, 'v', 'PX', 50)

    assert.strictEqual(await expired, true)
  })

  test('translates RENAME into rename_from and rename_to', async () => {
    const actor = await connect()
    const subscriber = await connect()
    await actor.config('SET', 'notify-keyspace-events', 'KEA')
    const src = randomKey()
    const dst = randomKey()
    await actor.set(src, 'v')

    await subscriber.psubscribe(`__keyevent@0__:*`)
    await settle()

    const renameFrom = waitForEvent(
      subscriber,
      `__keyevent@0__:rename_from`,
      src,
    )
    const renameTo = waitForEvent(subscriber, `__keyevent@0__:rename_to`, dst)
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
    const subscriber = await connect()
    const sentinelWriter = await connect()
    await actor.config('SET', 'notify-keyspace-events', 'KEA')
    const source = randomKey()
    const copied = randomKey()
    const moved = randomKey()
    const sentinel = randomKey()

    await subscriber.psubscribe(`__keyspace@0__:*`, `__keyevent@0__:*`)
    await settle()
    const events = collect(subscriber)

    await actor.select(1)
    await actor.set(source, 'v')
    assert.strictEqual(await actor.copy(source, copied, 'DB', 0), 1)
    await actor.set(moved, 'v')
    assert.strictEqual(await actor.move(moved, 0), 1)

    // Pub/sub delivery to one subscriber is ordered: once this db0 event
    // arrives, every event published before it has too. It also proves the
    // subscription is live, so the assertion below cannot pass vacuously.
    const flushed = waitForEvent(subscriber, `__keyevent@0__:set`, sentinel)
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

  test('emits the type-specific event before del when the last element goes (#379)', async () => {
    // Removing the last element deletes the key. Real Redis still announces
    // the removal itself (hdel/lpop/srem/zrem/spop) and only then `del`.
    const actor = await connect()
    const subscriber = await connect()
    const sentinelWriter = await connect()
    await actor.config('SET', 'notify-keyspace-events', 'KEA')
    await subscriber.psubscribe(`__keyevent@0__:*`)
    await settle()
    const events = collect(subscriber)

    const hash = randomKey()
    const list = randomKey()
    const set = randomKey()
    const zset = randomKey()
    const popped = randomKey()

    await actor.hset(hash, 'f', 'v')
    assert.strictEqual(await actor.hdel(hash, 'f'), 1)
    await actor.rpush(list, 'a')
    assert.strictEqual(await actor.lpop(list), 'a')
    await actor.sadd(set, 'm')
    assert.strictEqual(await actor.srem(set, 'm'), 1)
    await actor.zadd(zset, '1', 'm')
    assert.strictEqual(await actor.zrem(zset, 'm'), 1)
    await actor.sadd(popped, 'm')
    assert.strictEqual(await actor.spop(popped), 'm')
    await drain(subscriber, sentinelWriter, 0)

    assert.deepStrictEqual(eventsFor(events, 0, hash), ['hset', 'hdel', 'del'])
    assert.deepStrictEqual(eventsFor(events, 0, list), ['rpush', 'lpop', 'del'])
    assert.deepStrictEqual(eventsFor(events, 0, set), ['sadd', 'srem', 'del'])
    assert.deepStrictEqual(eventsFor(events, 0, zset), ['zadd', 'zrem', 'del'])
    assert.deepStrictEqual(eventsFor(events, 0, popped), [
      'sadd',
      'spop',
      'del',
    ])
  })

  test('stream group metadata commands notify without dirtying WATCH (#379)', async () => {
    // Real Redis keeps signalModifiedKey (WATCH) and notifyKeyspaceEvent
    // independent: these commands fire a notification but leave a WATCH on the
    // stream intact. No-ops (existing consumer, missing consumer/group) and
    // XREADGROUP/XACK on an existing consumer fire nothing.
    const actor = await connect()
    const subscriber = await connect()
    const watcher = await connect()
    const sentinelWriter = await connect()
    await actor.config('SET', 'notify-keyspace-events', 'KEA')
    await subscriber.psubscribe(`__keyevent@0__:*`)
    await settle()
    const events = collect(subscriber)
    const stream = randomKey()

    await actor.xadd(stream, '1-1', 'f', 'v')
    await watcher.watch(stream)
    await actor.xgroup('CREATE', stream, 'g', '0')
    assert.strictEqual(
      await actor.xgroup('CREATECONSUMER', stream, 'g', 'c1'),
      1,
    )
    assert.strictEqual(
      await actor.xgroup('CREATECONSUMER', stream, 'g', 'c1'),
      0,
    )
    await actor.xgroup('SETID', stream, 'g', '0')
    await actor.xreadgroup('GROUP', 'g', 'c1', 'STREAMS', stream, '>')
    assert.strictEqual(await actor.xack(stream, 'g', '1-1'), 1)
    assert.strictEqual(await actor.xgroup('DELCONSUMER', stream, 'g', 'c1'), 0)
    assert.strictEqual(
      await actor.xgroup('DELCONSUMER', stream, 'g', 'missing'),
      0,
    )
    await actor.xsetid(stream, '5-0')
    assert.strictEqual(await actor.xgroup('DESTROY', stream, 'g'), 1)
    assert.strictEqual(await actor.xgroup('DESTROY', stream, 'g'), 0)

    assert.deepStrictEqual(await watcher.multi().ping().exec(), [
      [null, 'PONG'],
    ])

    await drain(subscriber, sentinelWriter, 0)
    // Real Redis names these after the subcommand (xgroup-create, ...); the
    // subcommand naming is #381, so only the `xgroup` family is pinned here.
    const names = eventsFor(events, 0, stream).map(name =>
      name.startsWith('xgroup-') ? 'xgroup' : name,
    )
    assert.deepStrictEqual(names, [
      'xadd',
      'xgroup', // CREATE
      'xgroup', // CREATECONSUMER c1
      'xgroup', // SETID
      'xgroup', // DELCONSUMER c1
      'xsetid',
      'xgroup', // DESTROY
    ])
  })

  test('a parked blocking command does not name writes into its database (#444)', async () => {
    // BLPOP parked on db1 must not lend its name to an unrelated write that
    // lands in db1 meanwhile (MOVE from db0).
    const blocker = await connect()
    const actor = await connect()
    const subscriber = await connect()
    const sentinelWriter = await connect()
    await actor.config('SET', 'notify-keyspace-events', 'KEA')
    await sentinelWriter.select(1)
    await subscriber.psubscribe(`__keyspace@1__:*`, `__keyevent@1__:*`)
    await settle()
    const events = collect(subscriber)
    const queue = randomKey()
    const moved = randomKey()

    await blocker.select(1)
    const blocked = blocker.blpop(queue, 0)
    await settle()

    await actor.set(moved, 'v')
    assert.strictEqual(await actor.move(moved, 1), 1)
    await drain(subscriber, sentinelWriter, 1)

    assertNotNamedBlpop(events, 1, moved)

    await sentinelWriter.rpush(queue, 'x')
    assert.deepStrictEqual(await blocked, [queue, 'x'])
    await sentinelWriter.del(moved)
  })

  test('blocking commands resumed out of nesting order leave no stale name (#444)', async () => {
    // Two BLPOPs park on db1 and are served in arrival order — not LIFO. A
    // later write into db1 must still be named after its own command.
    const first = await connect()
    const second = await connect()
    const actor = await connect()
    const subscriber = await connect()
    const pusher = await connect()
    await actor.config('SET', 'notify-keyspace-events', 'KEA')
    await pusher.select(1)
    await subscriber.psubscribe(`__keyspace@1__:*`, `__keyevent@1__:*`)
    await settle()
    const events = collect(subscriber)
    const firstQueue = randomKey()
    const secondQueue = randomKey()
    const moved = randomKey()

    await first.select(1)
    await second.select(1)
    const firstBlocked = first.blpop(firstQueue, 0)
    await settle()
    const secondBlocked = second.blpop(secondQueue, 0)
    await settle()

    await pusher.rpush(firstQueue, 'a')
    assert.deepStrictEqual(await firstBlocked, [firstQueue, 'a'])
    await pusher.rpush(secondQueue, 'b')
    assert.deepStrictEqual(await secondBlocked, [secondQueue, 'b'])

    await actor.set(moved, 'v')
    assert.strictEqual(await actor.move(moved, 1), 1)
    await drain(subscriber, pusher, 1)

    assertNotNamedBlpop(events, 1, moved)
    await pusher.del(moved)
  })

  test('delivers nothing when notify-keyspace-events is disabled', async () => {
    const actor = await connect()
    const subscriber = await connect()
    await actor.config('SET', 'notify-keyspace-events', '')
    const key = randomKey()

    await subscriber.psubscribe(`__keyspace@0__:*`, `__keyevent@0__:*`)
    await settle()

    const events = collect(subscriber)
    await actor.set(key, 'v')
    await actor.del(key)
    await new Promise(resolve => setTimeout(resolve, 300))

    assert.deepStrictEqual(events, [])
  })

  test('gates events by configured class', async () => {
    const actor = await connect()
    const subscriber = await connect()
    // Keyevent channel + expired class only — string `set` must not deliver.
    await actor.config('SET', 'notify-keyspace-events', 'Ex')
    const setKey = randomKey()
    const expiringKey = randomKey()

    await subscriber.psubscribe(`__keyevent@0__:*`)
    await settle()

    const setEvents = collect(subscriber)
    await actor.set(setKey, 'v')
    await new Promise(resolve => setTimeout(resolve, 250))
    assert.strictEqual(
      setEvents.some(e => e.channel === `__keyevent@0__:set`),
      false,
      'set event must be gated out under class "x"',
    )

    const expired = waitForEvent(
      subscriber,
      `__keyevent@0__:expired`,
      expiringKey,
      3000,
    )
    await actor.set(expiringKey, 'v', 'PX', 50)
    await new Promise(resolve => setTimeout(resolve, 120))
    await actor.get(expiringKey)
    assert.strictEqual(await expired, true)
  })

  test('CONFIG normalizes flags and rejects invalid characters', async () => {
    const actor = await connect()

    await actor.config('SET', 'notify-keyspace-events', 'KEA')
    assert.deepStrictEqual(
      await actor.config('GET', 'notify-keyspace-events'),
      ['notify-keyspace-events', 'AKE'],
    )

    await actor.config('SET', 'notify-keyspace-events', 'KEg$')
    assert.deepStrictEqual(
      await actor.config('GET', 'notify-keyspace-events'),
      ['notify-keyspace-events', 'g$KE'],
    )

    await assert.rejects(
      actor.config('SET', 'notify-keyspace-events', 'Z'),
      /Invalid event class character/,
    )

    await actor.config('SET', 'notify-keyspace-events', '')
  })

  async function connect(): Promise<Redis> {
    const client = base.duplicate({ lazyConnect: true })
    await client.connect()
    clients.push(client)
    return client
  }
})

function settle(): Promise<void> {
  return new Promise(resolve => setTimeout(resolve, 50))
}

/**
 * Wait until a fresh `set` published by `writer` (already on database `db`)
 * reaches `subscriber`. Delivery to one subscriber is ordered, so every event
 * published before it has arrived too — and the subscription is proven live.
 */
async function drain(subscriber: Redis, writer: Redis, db: number) {
  const sentinel = randomKey()
  const seen = waitForEvent(subscriber, `__keyevent@${db}__:set`, sentinel)
  await writer.set(sentinel, 'v')
  assert.strictEqual(await seen, true)
  await writer.del(sentinel)
}

/** Keyevent names published for `key` on database `db`, in order. */
function eventsFor(
  events: { channel: string; message: string }[],
  db: number,
  key: string,
): string[] {
  const prefix = `__keyevent@${db}__:`
  return events
    .filter(e => e.channel.startsWith(prefix) && e.message === key)
    .map(e => e.channel.slice(prefix.length))
}

/**
 * The MOVE into `db` is not published under the parked BLPOP's name. Real
 * Redis publishes it as `move_to`; the mock does not name MOVE's target write
 * yet (#445), so only the absence of the stale `blpop` name is pinned here.
 */
function assertNotNamedBlpop(
  events: { channel: string; message: string }[],
  db: number,
  key: string,
): void {
  assert.deepStrictEqual(
    events.filter(
      e =>
        (e.channel === `__keyevent@${db}__:blpop` && e.message === key) ||
        (e.channel === `__keyspace@${db}__:${key}` && e.message === 'blpop'),
    ),
    [],
  )
}

function collect(client: Redis): { channel: string; message: string }[] {
  const events: { channel: string; message: string }[] = []
  client.on('pmessage', (_pattern, channel, message) => {
    events.push({ channel, message })
  })
  return events
}

function waitForEvent(
  client: Redis,
  channel: string,
  message: string,
  timeout = 1000,
): Promise<boolean> {
  return new Promise((resolve, reject) => {
    const timer = setTimeout(() => {
      cleanup()
      reject(new Error(`Timed out waiting for ${channel} = ${message}`))
    }, timeout)

    const onMessage = (
      _pattern: string,
      actualChannel: string,
      actualMessage: string,
    ) => {
      if (actualChannel !== channel || actualMessage !== message) {
        return
      }

      cleanup()
      resolve(true)
    }

    const cleanup = () => {
      clearTimeout(timer)
      client.off('pmessage', onMessage)
    }

    client.on('pmessage', onMessage)
  })
}
