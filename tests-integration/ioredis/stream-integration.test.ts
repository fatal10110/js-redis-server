import { test, describe, before, after } from 'node:test'
import assert from 'node:assert'
import { Cluster } from 'ioredis'
import { TestRunner } from '../test-config'
import { connectToSlotOwner, errorWithMessage, randomKey } from '../utils'

const testRunner = new TestRunner()

describe(`Stream Commands Integration (${testRunner.getBackendName()})`, () => {
  let redisClient: Cluster | undefined

  before(async () => {
    redisClient = await testRunner.setupIoredisCluster('stream-integration')
  })

  after(async () => {
    await testRunner.cleanup()
  })

  test('XADD with explicit ids and XLEN', async () => {
    const key = randomKey()
    assert.strictEqual(await redisClient?.xadd(key, '1-1', 'f', 'v'), '1-1')
    assert.strictEqual(
      await redisClient?.xadd(key, '1-2', 'a', 'b', 'c', 'd'),
      '1-2',
    )
    assert.strictEqual(await redisClient?.xlen(key), 2)
  })

  test('XADD * generates monotonically increasing ids', async () => {
    const key = randomKey()
    const id1 = (await redisClient?.xadd(key, '*', 'f', 'v')) as string
    const id2 = (await redisClient?.xadd(key, '*', 'f', 'v')) as string
    assert.match(id1, /^\d+-\d+$/)
    assert.match(id2, /^\d+-\d+$/)

    const [ms1, seq1] = id1.split('-').map(BigInt)
    const [ms2, seq2] = id2.split('-').map(BigInt)
    assert.ok(ms2 > ms1 || (ms2 === ms1 && seq2 > seq1), `${id2} > ${id1}`)
  })

  test('XADD <ms>-* auto-increments the sequence', async () => {
    const key = randomKey()
    assert.strictEqual(await redisClient?.xadd(key, '5-*', 'f', 'v'), '5-0')
    assert.strictEqual(await redisClient?.xadd(key, '5-*', 'f', 'v'), '5-1')
  })

  test('XADD rejects ids equal to or smaller than the top item', async () => {
    const key = randomKey()
    await redisClient?.xadd(key, '5-5', 'f', 'v')

    await assert.rejects(
      () => redisClient!.xadd(key, '5-5', 'f', 'v'),
      errorWithMessage(
        'ERR The ID specified in XADD is equal or smaller than the target stream top item',
      ),
    )
    await assert.rejects(
      () => redisClient!.xadd(key, '3-0', 'f', 'v'),
      errorWithMessage(
        'ERR The ID specified in XADD is equal or smaller than the target stream top item',
      ),
    )
  })

  test('XADD rejects 0-0 and invalid ids', async () => {
    const key = randomKey()
    await assert.rejects(
      () => redisClient!.xadd(key, '0-0', 'f', 'v'),
      errorWithMessage('ERR The ID specified in XADD must be greater than 0-0'),
    )
    await assert.rejects(
      () => redisClient!.xadd(key, 'not-an-id', 'f', 'v'),
      errorWithMessage(
        'ERR Invalid stream ID specified as stream command argument',
      ),
    )
  })

  test('XRANGE returns entries within an inclusive range', async () => {
    const key = randomKey()
    await redisClient?.xadd(key, '1-1', 'a', '1')
    await redisClient?.xadd(key, '2-1', 'b', '2')
    await redisClient?.xadd(key, '3-1', 'c', '3')

    assert.deepStrictEqual(await redisClient?.xrange(key, '-', '+'), [
      ['1-1', ['a', '1']],
      ['2-1', ['b', '2']],
      ['3-1', ['c', '3']],
    ])
    assert.deepStrictEqual(await redisClient?.xrange(key, '2', '2'), [
      ['2-1', ['b', '2']],
    ])
  })

  test('XRANGE honors exclusive bounds and COUNT', async () => {
    const key = randomKey()
    await redisClient?.xadd(key, '1-1', 'a', '1')
    await redisClient?.xadd(key, '2-1', 'b', '2')
    await redisClient?.xadd(key, '3-1', 'c', '3')

    assert.deepStrictEqual(await redisClient?.xrange(key, '(1-1', '+'), [
      ['2-1', ['b', '2']],
      ['3-1', ['c', '3']],
    ])
    assert.deepStrictEqual(
      await redisClient?.xrange(key, '-', '+', 'COUNT', 2),
      [
        ['1-1', ['a', '1']],
        ['2-1', ['b', '2']],
      ],
    )
  })

  test('XREVRANGE returns entries in descending order', async () => {
    const key = randomKey()
    await redisClient?.xadd(key, '1-1', 'a', '1')
    await redisClient?.xadd(key, '2-1', 'b', '2')

    assert.deepStrictEqual(await redisClient?.xrevrange(key, '+', '-'), [
      ['2-1', ['b', '2']],
      ['1-1', ['a', '1']],
    ])
  })

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

  test('stream commands reject keys holding another type', async () => {
    const key = randomKey()
    await redisClient?.set(key, 'x')

    const wrongType = errorWithMessage(
      'WRONGTYPE Operation against a key holding the wrong kind of value',
    )
    await assert.rejects(
      () => redisClient!.xadd(key, '1-1', 'f', 'v'),
      wrongType,
    )
    await assert.rejects(() => redisClient!.xlen(key), wrongType)
    await assert.rejects(() => redisClient!.xrange(key, '-', '+'), wrongType)
  })

  test('XLEN and XRANGE on a missing key return empty results', async () => {
    const key = randomKey()
    assert.strictEqual(await redisClient?.xlen(key), 0)
    assert.deepStrictEqual(await redisClient?.xrange(key, '-', '+'), [])
  })

  // XTRIM

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

  test('XTRIM MAXLEN with ~ (approximate) accepts the flag and returns an integer', async () => {
    // Real Redis uses radix-tree node boundaries for ~; on a tiny stream it may not
    // trim at all. We only assert the command succeeds and returns a non-negative int.
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

  // XADD with NOMKSTREAM and MAXLEN options

  test('XADD NOMKSTREAM returns null when key does not exist', async () => {
    const key = randomKey()
    const node = await connectToSlotOwner(redisClient!, key)
    try {
      const result = await node.call('XADD', key, 'NOMKSTREAM', '1-1', 'f', 'v')
      assert.strictEqual(result, null)
      assert.strictEqual(await node.exists(key), 0)
    } finally {
      node.disconnect()
    }
  })

  test('XADD NOMKSTREAM appends to an existing stream normally', async () => {
    const key = randomKey()
    const node = await connectToSlotOwner(redisClient!, key)
    try {
      await node.xadd(key, '1-1', 'f', 'v')
      const result = await node.call('XADD', key, 'NOMKSTREAM', '2-1', 'g', 'w')
      assert.strictEqual(result, '2-1')
      assert.strictEqual(await node.xlen(key), 2)
    } finally {
      node.disconnect()
    }
  })

  test('XADD MAXLEN trims oldest entries after append', async () => {
    const key = randomKey()
    const node = await connectToSlotOwner(redisClient!, key)
    try {
      await node.xadd(key, '1-1', 'f', 'v')
      await node.xadd(key, '2-1', 'f', 'v')
      await node.xadd(key, '3-1', 'f', 'v')
      await node.call('XADD', key, 'MAXLEN', '2', '4-1', 'f', 'v')
      assert.deepStrictEqual(await node.xrange(key, '-', '+'), [
        ['3-1', ['f', 'v']],
        ['4-1', ['f', 'v']],
      ])
    } finally {
      node.disconnect()
    }
  })

  // XREAD

  test('XREAD returns entries after the given id', async () => {
    const key = randomKey()
    const node = await connectToSlotOwner(redisClient!, key)
    try {
      await node.xadd(key, '1-1', 'f', '1')
      await node.xadd(key, '2-1', 'f', '2')
      await node.xadd(key, '3-1', 'f', '3')

      const result = (await node.xread('STREAMS', key, '1-1')) as [
        string,
        [string, string[]][],
      ][]
      assert.ok(Array.isArray(result))
      assert.strictEqual(result.length, 1)
      assert.strictEqual(result[0][0], key)
      assert.deepStrictEqual(result[0][1], [
        ['2-1', ['f', '2']],
        ['3-1', ['f', '3']],
      ])
    } finally {
      node.disconnect()
    }
  })

  test('XREAD COUNT limits the number of returned entries', async () => {
    const key = randomKey()
    const node = await connectToSlotOwner(redisClient!, key)
    try {
      await node.xadd(key, '1-1', 'f', '1')
      await node.xadd(key, '2-1', 'f', '2')
      await node.xadd(key, '3-1', 'f', '3')

      const result = (await node.xread('COUNT', 1, 'STREAMS', key, '0-0')) as [
        string,
        [string, string[]][],
      ][]
      assert.ok(Array.isArray(result))
      assert.strictEqual(result[0][1].length, 1)
      assert.strictEqual(result[0][1][0][0], '1-1')
    } finally {
      node.disconnect()
    }
  })

  test('XREAD returns null when no new entries exist for the given id', async () => {
    const key = randomKey()
    const node = await connectToSlotOwner(redisClient!, key)
    try {
      await node.xadd(key, '1-1', 'f', '1')
      const result = await node.xread('STREAMS', key, '1-1')
      assert.strictEqual(result, null)
    } finally {
      node.disconnect()
    }
  })

  test('XREAD with $ id returns null (no entries after current last)', async () => {
    const key = randomKey()
    const node = await connectToSlotOwner(redisClient!, key)
    try {
      await node.xadd(key, '1-1', 'f', '1')
      const result = await node.xread('STREAMS', key, '$')
      assert.strictEqual(result, null)
    } finally {
      node.disconnect()
    }
  })

  test('XREAD on a missing key returns null', async () => {
    const key = randomKey()
    const node = await connectToSlotOwner(redisClient!, key)
    try {
      const result = await node.xread('STREAMS', key, '0-0')
      assert.strictEqual(result, null)
    } finally {
      node.disconnect()
    }
  })

  test('XREAD from multiple streams on same slot returns combined results', async () => {
    // Use hashtag to pin both keys to the same cluster slot.
    const tag = Math.random().toString(36).substring(2, 8)
    const key1 = `{${tag}}s1`
    const key2 = `{${tag}}s2`
    const node = await connectToSlotOwner(redisClient!, key1)
    try {
      await node.xadd(key1, '1-1', 'a', '1')
      await node.xadd(key2, '2-1', 'b', '2')

      const result = (await node.xread(
        'STREAMS',
        key1,
        key2,
        '0-0',
        '0-0',
      )) as [string, [string, string[]][]][]
      assert.ok(Array.isArray(result))
      assert.strictEqual(result.length, 2)
    } finally {
      node.disconnect()
    }
  })
})
