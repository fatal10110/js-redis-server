import { test, describe, afterEach } from 'node:test'
import assert from 'node:assert'
import { createInMemoryRedis } from '../src'
import type { InMemoryRedis, RedisNativeReply } from '../src'

// Reads the next decoded push frame, failing instead of hanging if none arrives.
async function nextPush(
  pushes: AsyncIterable<RedisNativeReply>,
  timeoutMs = 1000,
): Promise<RedisNativeReply> {
  const it = pushes[Symbol.asyncIterator]()
  const timer = new Promise<never>((_, reject) =>
    setTimeout(
      () => reject(new Error('timed out waiting for push')),
      timeoutMs,
    ),
  )
  const { value, done } = await Promise.race([it.next(), timer])
  assert.ok(!done, 'push stream ended before a frame arrived')
  return value
}

function yieldToEventLoop(): Promise<void> {
  return new Promise(resolve => setImmediate(resolve))
}

describe('in-memory instance — streaming & blocking across connections', () => {
  let instance: InMemoryRedis

  afterEach(() => {
    instance?.close()
  })

  test('pub/sub: a subscriber connection sees another connection PUBLISH', async () => {
    instance = await createInMemoryRedis()
    const sub = instance.connect()
    const pub = instance.connect()

    assert.deepStrictEqual(await sub.command('SUBSCRIBE', 'ch'), [
      'subscribe',
      'ch',
      1,
    ])

    const pushes = sub.pushes()
    await pub.command('PUBLISH', 'ch', 'hi')

    assert.deepStrictEqual(await nextPush(pushes), ['message', 'ch', 'hi'])
  })

  test('MONITOR: a monitoring connection sees another connection’s command', async () => {
    instance = await createInMemoryRedis()
    const mon = instance.connect()
    const other = instance.connect()

    assert.strictEqual(await mon.command('MONITOR'), 'OK')

    const pushes = mon.pushes()
    await other.command('SET', 'k', 'v')

    const line = await nextPush(pushes)
    assert.ok(typeof line === 'string')
    assert.match(line, /"SET" "k" "v"/)
  })

  test('blocking: BLPOP on one connection unblocks on another connection’s LPUSH', async () => {
    instance = await createInMemoryRedis()
    const a = instance.connect()
    const b = instance.connect()

    const blocked = a.command('BLPOP', 'q', '5')
    await yieldToEventLoop()

    await b.command('LPUSH', 'q', 'job')
    assert.deepStrictEqual(await blocked, ['q', 'job'])
  })

  test('connections over one instance share the keyspace', async () => {
    instance = await createInMemoryRedis()
    const a = instance.connect()
    const b = instance.connect()

    await a.command('SET', 'shared', 'yes')
    assert.strictEqual(await b.command('GET', 'shared'), 'yes')
  })
})
