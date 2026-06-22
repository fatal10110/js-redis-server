import { test, describe, before, after } from 'node:test'
import assert from 'node:assert'
import { Cluster } from 'ioredis'
import { TestRunner } from '../../test-config'
import { connectToSlotOwner, errorWithMessage, randomKey } from '../../utils'

const testRunner = new TestRunner()

describe(`Stream Commands Integration (${testRunner.getBackendName()})`, () => {
  let redisClient: Cluster | undefined

  before(async () => {
    redisClient = await testRunner.setupIoredisCluster('stream-integration')
  })

  after(async () => {
    await testRunner.cleanup()
  })

  // XTRIM

  // XADD with NOMKSTREAM and MAXLEN options

  // XREAD

  // Stream consumer groups

  test('XDEL removes entries, keeps the empty stream, and retains last id', async () => {
    const key = randomKey()
    await redisClient?.xadd(key, '1-1', 'a', '1')
    await redisClient?.xadd(key, '2-1', 'b', '2')

    assert.strictEqual(await redisClient?.xdel(key, '1-1', '9-9'), 1)
    assert.strictEqual(await redisClient?.xlen(key), 1)

    assert.strictEqual(await redisClient?.xdel(key, '2-1'), 1)
    // Stream still exists with length 0 (not removed).
    assert.strictEqual(await redisClient?.xlen(key), 0)
    assert.strictEqual(await redisClient?.exists(key), 1)

    // A new id must still beat the retained last id (2-1).
    await assert.rejects(
      () => redisClient!.xadd(key, '2-1', 'f', 'v'),
      errorWithMessage(
        'ERR The ID specified in XADD is equal or smaller than the target stream top item',
      ),
    )
  })

  test('XDEL on a missing key returns 0', async () => {
    const key = randomKey()
    assert.strictEqual(await redisClient?.xdel(key, '1-1'), 0)
    assert.strictEqual(await redisClient?.exists(key), 0)
  })

  test('XTRIM MAXLEN removes oldest entries and returns removed count', async () => {
    const key = randomKey()
    await redisClient!.xadd(key, '1-1', 'f', 'v')
    await redisClient!.xadd(key, '2-1', 'f', 'v')
    await redisClient!.xadd(key, '3-1', 'f', 'v')

    assert.strictEqual(await redisClient!.xtrim(key, 'MAXLEN', 2), 1)
    assert.deepStrictEqual(await redisClient!.xrange(key, '-', '+'), [
      ['2-1', ['f', 'v']],
      ['3-1', ['f', 'v']],
    ])
  })

  test('XTRIM MAXLEN with ~ (approximate) does not exact-trim tiny streams', async () => {
    // Real Redis uses radix-tree node boundaries for ~; on a tiny stream it
    // keeps entries above the threshold instead of trimming exactly to it.
    const key = randomKey()
    const node = await connectToSlotOwner(redisClient!, key)
    try {
      await node.xadd(key, '1-1', 'f', 'v')
      await node.xadd(key, '2-1', 'f', 'v')
      await node.xadd(key, '3-1', 'f', 'v')

      const removed = (await node.call(
        'XTRIM',
        key,
        'MAXLEN',
        '~',
        '2',
      )) as number
      assert.strictEqual(removed, 0)
      assert.strictEqual(await node.xlen(key), 3)
      assert.deepStrictEqual(await node.xrange(key, '-', '+'), [
        ['1-1', ['f', 'v']],
        ['2-1', ['f', 'v']],
        ['3-1', ['f', 'v']],
      ])
    } finally {
      node.disconnect()
    }
  })

  test('XTRIM MAXLEN with ~ accepts LIMIT count', async () => {
    const key = randomKey()
    const node = await connectToSlotOwner(redisClient!, key)
    try {
      await node.xadd(key, '1-1', 'f', 'v')
      await node.xadd(key, '2-1', 'f', 'v')
      await node.xadd(key, '3-1', 'f', 'v')

      const removed = (await node.call(
        'XTRIM',
        key,
        'MAXLEN',
        '~',
        '2',
        'LIMIT',
        '1',
      )) as number
      assert.ok(typeof removed === 'number' && removed >= 0)
    } finally {
      node.disconnect()
    }
  })

  test('XTRIM LIMIT validates approximate trim syntax', async () => {
    const key = randomKey()
    const node = await connectToSlotOwner(redisClient!, key)
    try {
      await node.xadd(key, '1-1', 'f', 'v')
      await node.xadd(key, '2-1', 'f', 'v')
      await node.xadd(key, '3-1', 'f', 'v')

      await assert.rejects(
        () => node.call('XTRIM', key, 'MAXLEN', '2', 'LIMIT', '1'),
        errorWithMessage(
          'ERR syntax error, LIMIT cannot be used without the special ~ option',
        ),
      )
      await assert.rejects(
        () => node.call('XTRIM', key, 'MAXLEN', '~', '2', 'LIMIT'),
        errorWithMessage('ERR syntax error'),
      )
      await assert.rejects(
        () => node.call('XTRIM', key, 'MAXLEN', '~', '2', 'LIMIT', 'abc'),
        errorWithMessage('ERR value is not an integer or out of range'),
      )
      await assert.rejects(
        () => node.call('XTRIM', key, 'MAXLEN', '~', '2', 'LIMIT', '-1'),
        errorWithMessage('ERR The LIMIT argument must be >= 0.'),
      )
      const removed = (await node.call(
        'XTRIM',
        key,
        'MAXLEN',
        '~',
        '2',
        'LIMIT',
        '1',
        'LIMIT',
        '1',
      )) as number
      assert.ok(typeof removed === 'number' && removed >= 0)
    } finally {
      node.disconnect()
    }
  })

  test('XTRIM MINID removes entries with id below threshold', async () => {
    const key = randomKey()
    const node = await connectToSlotOwner(redisClient!, key)
    try {
      await node.xadd(key, '1-0', 'f', 'v')
      await node.xadd(key, '2-0', 'f', 'v')
      await node.xadd(key, '3-0', 'f', 'v')

      assert.strictEqual(await node.call('XTRIM', key, 'MINID', '2-0'), 1)
      assert.deepStrictEqual(await node.xrange(key, '-', '+'), [
        ['2-0', ['f', 'v']],
        ['3-0', ['f', 'v']],
      ])
    } finally {
      node.disconnect()
    }
  })

  test('XTRIM on missing key returns 0 without creating it', async () => {
    const key = randomKey()
    assert.strictEqual(await redisClient!.xtrim(key, 'MAXLEN', 5), 0)
    assert.strictEqual(await redisClient!.exists(key), 0)
  })

  test('XTRIM MAXLEN no-op when stream is within limit', async () => {
    const key = randomKey()
    await redisClient!.xadd(key, '1-1', 'f', 'v')
    await redisClient!.xadd(key, '2-1', 'f', 'v')

    assert.strictEqual(await redisClient!.xtrim(key, 'MAXLEN', 5), 0)
    assert.strictEqual(await redisClient!.xlen(key), 2)
  })
})
