import { describe, test, before, after } from 'node:test'
import assert from 'node:assert'
import { once } from 'node:events'
import type { Redis } from 'ioredis'
import { createIoredisMock } from '../../src/index'

describe('createIoredisMock — pub/sub over the virtual socket', () => {
  let publisher: Redis
  let subscriber: Redis

  before(async () => {
    publisher = (await createIoredisMock()) as Redis
    // duplicate() reuses the same Connector → same in-memory pipeline, so the
    // two clients share one keyspace + pub/sub broker (the idiomatic ioredis
    // pattern for a dedicated subscriber connection). It inherits
    // lazyConnect: false, so it auto-connects; just wait until it is ready.
    subscriber = publisher.duplicate()
    if (subscriber.status !== 'ready') {
      await once(subscriber, 'ready')
    }
  })

  after(async () => {
    await subscriber.quit()
    await publisher.quit()
  })

  test('subscribe receives a published message (push frame flows)', async () => {
    const received = once(subscriber, 'message')
    const count = await subscriber.subscribe('news')
    assert.strictEqual(count, 1)

    const delivered = await publisher.publish('news', 'hello')
    assert.strictEqual(delivered, 1)

    const [channel, message] = await received
    assert.strictEqual(channel, 'news')
    assert.strictEqual(message, 'hello')
  })

  test('psubscribe receives pmessage push frames', async () => {
    const received = once(subscriber, 'pmessage')
    await subscriber.psubscribe('ne*')

    await publisher.publish('news', 'world')

    const [pattern, channel, message] = await received
    assert.strictEqual(pattern, 'ne*')
    assert.strictEqual(channel, 'news')
    assert.strictEqual(message, 'world')
  })
})
