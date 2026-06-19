import { after, before, describe, test } from 'node:test'
import assert from 'node:assert'
import { Redis } from 'ioredis'
import { TestRunner } from '../test-config'
import { randomKey } from '../utils'

const testRunner = new TestRunner()

describe(`Keyspace notifications (${testRunner.getBackendName()})`, () => {
  let port: number
  const clients: Redis[] = []

  before(async () => {
    port = await testRunner.setupRawStandalone()
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
    const client = new Redis({ host: '127.0.0.1', port, lazyConnect: true })
    await client.connect()
    clients.push(client)
    return client
  }
})

function settle(): Promise<void> {
  return new Promise(resolve => setTimeout(resolve, 50))
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
