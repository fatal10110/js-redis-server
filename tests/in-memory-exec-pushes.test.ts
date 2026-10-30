import { test, describe, afterEach } from 'node:test'
import assert from 'node:assert'
import { createInMemoryRedis } from '../src'
import type { InMemoryRedis, RedisNativeReply } from '../src'

// Reads the next decoded push frame, failing instead of hanging if none arrives.
async function nextPush(
  pushes: AsyncIterator<RedisNativeReply>,
  timeoutMs = 1000,
): Promise<RedisNativeReply> {
  let timer: NodeJS.Timeout | undefined
  const timeout = new Promise<never>((_, reject) => {
    timer = setTimeout(
      () => reject(new Error('timed out waiting for push')),
      timeoutMs,
    )
  })
  try {
    const { value, done } = await Promise.race([pushes.next(), timeout])
    assert.ok(!done, 'push stream ended before a frame arrived')
    return value
  } finally {
    clearTimeout(timer)
  }
}

/**
 * EXEC hands each queued command's after-reply work to its own reply (#509):
 * the in-memory client delivers a queued multi-target SUBSCRIBE's other
 * confirmations, and the connection's own PUBLISH message, as pushes.
 */
describe('in-memory client: pushes made inside EXEC', () => {
  let instance: InMemoryRedis

  afterEach(() => {
    instance?.close()
  })

  test('MULTI; SUBSCRIBE a b; EXEC pushes the second confirmation', async () => {
    instance = await createInMemoryRedis()
    const sub = instance.connect()
    const pushes = sub.pushes()[Symbol.asyncIterator]()

    assert.strictEqual(await sub.command('MULTI'), 'OK')
    assert.strictEqual(await sub.command('SUBSCRIBE', 'a', 'b'), 'QUEUED')
    assert.deepStrictEqual(await sub.command('EXEC'), [['subscribe', 'a', 1]])
    assert.deepStrictEqual(await nextPush(pushes), ['subscribe', 'b', 2])
  })

  for (const compatibility of ['redis-6.2', 'redis-8.0'] as const) {
    test(`${compatibility}: own PUBLISH in EXEC is delivered and later pushes still flow`, async () => {
      instance = await createInMemoryRedis({ compatibility })
      const sub = instance.connect()
      const pub = instance.connect()
      const pushes = sub.pushes()[Symbol.asyncIterator]()

      await sub.command('HELLO', 3)
      assert.deepStrictEqual(await sub.command('SUBSCRIBE', 'c'), [
        'subscribe',
        'c',
        1,
      ])
      assert.strictEqual(await sub.command('MULTI'), 'OK')
      assert.strictEqual(await sub.command('PUBLISH', 'c', 'x'), 'QUEUED')
      assert.strictEqual(await sub.command('PUBLISH', 'c', 'y'), 'QUEUED')
      assert.deepStrictEqual(await sub.command('EXEC'), [1, 1])
      assert.deepStrictEqual(await nextPush(pushes), ['message', 'c', 'x'])
      assert.deepStrictEqual(await nextPush(pushes), ['message', 'c', 'y'])

      await pub.command('PUBLISH', 'c', 'z')
      assert.deepStrictEqual(await nextPush(pushes), ['message', 'c', 'z'])
    })
  }

  test('MULTI; SUBSCRIBE a; PUBLISH a x; EXEC delivers the message', async () => {
    instance = await createInMemoryRedis()
    const sub = instance.connect()
    const pushes = sub.pushes()[Symbol.asyncIterator]()

    await sub.command('MULTI')
    await sub.command('SUBSCRIBE', 'a')
    await sub.command('PUBLISH', 'a', 'x')
    assert.deepStrictEqual(await sub.command('EXEC'), [
      ['subscribe', 'a', 1],
      1,
    ])
    assert.deepStrictEqual(await nextPush(pushes), ['message', 'a', 'x'])
  })
})
