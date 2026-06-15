import { test, describe, before, after } from 'node:test'
import assert from 'node:assert'
import { Cluster } from 'ioredis'
import { TestRunner } from '../test-config'
import {
  commandFrame,
  connectToSlotOwner,
  errorWithMessage,
  findSlotOwner,
  randomKey,
} from '../utils'
import {
  RawRedisConnection,
  respMapGet,
  respText,
} from '../raw-tcp/raw-connection'

const testRunner = new TestRunner()
const MAX_UINT64 = '18446744073709551615'

function kvArrayGet(items: unknown[], key: string): unknown {
  const index = items.indexOf(key)
  assert.notStrictEqual(index, -1, `expected ${key} field`)
  return items[index + 1]
}

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

  test('XADD handles auto-generated ids when the sequence overflows', async () => {
    const taggedKey = `{${randomKey()}}`
    const fixedMsKey = `${taggedKey}:fixed-ms`
    const autoKey = `${taggedKey}:auto`
    const node = await connectToSlotOwner(redisClient!, fixedMsKey)
    const futureMs = BigInt(Date.now()) + 60_000n

    try {
      assert.strictEqual(
        await node.call('XADD', fixedMsKey, `7-${MAX_UINT64}`, 'f', 'v'),
        `7-${MAX_UINT64}`,
      )
      await assert.rejects(
        () => node.call('XADD', fixedMsKey, '7-*', 'f', 'v'),
        errorWithMessage('ERR Elements are too large to be stored'),
      )

      assert.strictEqual(
        await node.call('XADD', autoKey, `${futureMs}-${MAX_UINT64}`, 'f', 'v'),
        `${futureMs}-${MAX_UINT64}`,
      )
      assert.strictEqual(
        await node.call('XADD', autoKey, '*', 'f', 'v'),
        `${futureMs + 1n}-0`,
      )
    } finally {
      node.disconnect()
    }
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

  test('XRANGE and XREVRANGE reject non-integer COUNT values', async () => {
    const key = randomKey()
    const node = await connectToSlotOwner(redisClient!, key)
    try {
      await node.xadd(key, '1-1', 'a', '1')

      const invalidCount = errorWithMessage(
        'ERR value is not an integer or out of range',
      )
      await assert.rejects(
        () => node.call('XRANGE', key, '-', '+', 'COUNT', 'abc'),
        invalidCount,
      )
      await assert.rejects(
        () => node.call('XREVRANGE', key, '+', '-', 'COUNT', 'abc'),
        invalidCount,
      )
    } finally {
      node.disconnect()
    }
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

  test('XADD MAXLEN with ~ accepts LIMIT count before generated id', async () => {
    const key = randomKey()
    const node = await connectToSlotOwner(redisClient!, key)
    try {
      await node.xadd(key, '1-1', 'f', 'v')
      await node.xadd(key, '2-1', 'f', 'v')
      await node.xadd(key, '3-1', 'f', 'v')

      const id = (await node.call(
        'XADD',
        key,
        'MAXLEN',
        '~',
        '2',
        'LIMIT',
        '1',
        '*',
        'f',
        'v',
      )) as string
      assert.match(id, /^\d+-\d+$/)

      const duplicateLimitId = (await node.call(
        'XADD',
        key,
        'MAXLEN',
        '~',
        '2',
        'LIMIT',
        '1',
        'limit',
        '1',
        '*',
        'f',
        'v',
      )) as string
      assert.match(duplicateLimitId, /^\d+-\d+$/)
    } finally {
      node.disconnect()
    }
  })

  test('XADD LIMIT validates approximate trim syntax before id', async () => {
    const key = randomKey()
    const node = await connectToSlotOwner(redisClient!, key)
    try {
      await node.xadd(key, '1-1', 'f', 'v')

      await assert.rejects(
        () =>
          node.call('XADD', key, 'MAXLEN', '2', 'LIMIT', '1', '*', 'f', 'v'),
        errorWithMessage(
          'ERR syntax error, LIMIT cannot be used without the special ~ option',
        ),
      )
      await assert.rejects(
        () =>
          node.call('XADD', key, 'MAXLEN', '~', '2', 'LIMIT', '*', 'f', 'v'),
        errorWithMessage('ERR value is not an integer or out of range'),
      )
      await assert.rejects(
        () =>
          node.call(
            'XADD',
            key,
            'MAXLEN',
            '~',
            '2',
            'LIMIT',
            '-1',
            '*',
            'f',
            'v',
          ),
        errorWithMessage('ERR The LIMIT argument must be >= 0.'),
      )
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

  test('XREAD returns a RESP3 map after HELLO 3', async () => {
    assert.ok(redisClient)

    const key = `{stream-resp3:${randomKey()}}:stream`
    const [host, port] = await findSlotOwner(redisClient, key)
    const connection = await RawRedisConnection.connect(host, port)

    try {
      connection.write(commandFrame('HELLO', '3'))
      assert.ok((await connection.readFrame()) instanceof Map)

      connection.write(commandFrame('XADD', key, '*', 'field1', 'value1'))
      assert.match(respText(await connection.readFrame()), /^\d+-\d+$/)

      connection.write(commandFrame('XREAD', 'STREAMS', key, '0-0'))
      const reply = await connection.readFrame()
      assert.ok(reply instanceof Map)

      const entries = respMapGet(reply, key)
      assert.ok(Array.isArray(entries))
      assert.strictEqual(entries.length, 1)

      const entry = entries[0]
      assert.ok(Array.isArray(entry))
      assert.strictEqual(entry.length, 2)
      assert.match(respText(entry[0]), /^\d+-\d+$/)
      assert.deepStrictEqual(
        (entry[1] as unknown[]).map(value => respText(value)),
        ['field1', 'value1'],
      )
    } finally {
      connection.close()
      await redisClient.del(key)
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

  // Stream consumer groups

  test('XGROUP creates, mutates, and destroys consumer groups', async () => {
    const key = randomKey()
    const node = await connectToSlotOwner(redisClient!, key)
    try {
      await node.xadd(key, '1-0', 'f', '1')
      await node.xadd(key, '2-0', 'f', '2')

      assert.strictEqual(
        await node.call('XGROUP', 'CREATE', key, 'workers', '0'),
        'OK',
      )
      assert.strictEqual(
        await node.call('XGROUP', 'CREATECONSUMER', key, 'workers', 'alice'),
        1,
      )
      assert.strictEqual(
        await node.call('XGROUP', 'CREATECONSUMER', key, 'workers', 'alice'),
        0,
      )

      const read = (await node.call(
        'XREADGROUP',
        'GROUP',
        'workers',
        'alice',
        'COUNT',
        '2',
        'STREAMS',
        key,
        '>',
      )) as [string, [string, string[]][]][]
      assert.deepStrictEqual(read, [
        [
          key,
          [
            ['1-0', ['f', '1']],
            ['2-0', ['f', '2']],
          ],
        ],
      ])

      const pendingSummary = (await node.call(
        'XPENDING',
        key,
        'workers',
      )) as unknown[]
      assert.strictEqual(pendingSummary[0], 2)
      assert.strictEqual(pendingSummary[1], '1-0')
      assert.strictEqual(pendingSummary[2], '2-0')
      assert.ok(Array.isArray(pendingSummary[3]))

      assert.strictEqual(await node.call('XACK', key, 'workers', '1-0'), 1)
      const pendingDetails = (await node.call(
        'XPENDING',
        key,
        'workers',
        '-',
        '+',
        '10',
      )) as unknown[][]
      assert.strictEqual(pendingDetails.length, 1)
      assert.strictEqual(pendingDetails[0][0], '2-0')
      assert.strictEqual(pendingDetails[0][1], 'alice')
      assert.strictEqual(pendingDetails[0][3], 1)

      const consumers = (await node.call(
        'XINFO',
        'CONSUMERS',
        key,
        'workers',
      )) as unknown[][]
      const alice = consumers.find(item => kvArrayGet(item, 'name') === 'alice')
      assert.ok(alice)
      assert.strictEqual(kvArrayGet(alice, 'pending'), 1)

      assert.strictEqual(
        await node.call('XGROUP', 'DELCONSUMER', key, 'workers', 'alice'),
        1,
      )
      assert.strictEqual(
        await node.call('XGROUP', 'DESTROY', key, 'workers'),
        1,
      )
      assert.strictEqual(
        await node.call('XGROUP', 'DESTROY', key, 'workers'),
        0,
      )
    } finally {
      node.disconnect()
    }
  })

  test('XGROUP MKSTREAM and SETID control group delivery position', async () => {
    const key = randomKey()
    const node = await connectToSlotOwner(redisClient!, key)
    try {
      assert.strictEqual(
        await node.call('XGROUP', 'CREATE', key, 'workers', '$', 'MKSTREAM'),
        'OK',
      )
      assert.strictEqual(await node.xlen(key), 0)

      await node.xadd(key, '1-0', 'f', '1')
      await node.xadd(key, '2-0', 'f', '2')
      assert.deepStrictEqual(
        await node.call(
          'XREADGROUP',
          'GROUP',
          'workers',
          'alice',
          'STREAMS',
          key,
          '>',
        ),
        [
          [
            key,
            [
              ['1-0', ['f', '1']],
              ['2-0', ['f', '2']],
            ],
          ],
        ],
      )

      assert.strictEqual(
        await node.call('XGROUP', 'SETID', key, 'workers', '$'),
        'OK',
      )
      await node.xadd(key, '3-0', 'f', '3')
      assert.deepStrictEqual(
        await node.call(
          'XREADGROUP',
          'GROUP',
          'workers',
          'bob',
          'STREAMS',
          key,
          '>',
        ),
        [[key, [['3-0', ['f', '3']]]]],
      )
    } finally {
      node.disconnect()
    }
  })

  test('XCLAIM and XAUTOCLAIM transfer pending stream entries', async () => {
    const key = randomKey()
    const node = await connectToSlotOwner(redisClient!, key)
    try {
      await node.xadd(key, '1-0', 'f', '1')
      await node.xadd(key, '2-0', 'f', '2')
      await node.call('XGROUP', 'CREATE', key, 'workers', '0')
      await node.call(
        'XREADGROUP',
        'GROUP',
        'workers',
        'alice',
        'COUNT',
        '2',
        'STREAMS',
        key,
        '>',
      )

      assert.deepStrictEqual(
        await node.call('XCLAIM', key, 'workers', 'bob', '0', '1-0'),
        [['1-0', ['f', '1']]],
      )

      const claimed = (await node.call(
        'XAUTOCLAIM',
        key,
        'workers',
        'carol',
        '0',
        '0-0',
        'COUNT',
        '10',
      )) as [string, [string, string[]][], string[]]
      assert.strictEqual(claimed[0], '0-0')
      assert.deepStrictEqual(
        claimed[1].map(entry => entry[0]),
        ['1-0', '2-0'],
      )
      assert.deepStrictEqual(claimed[2], [])

      const pendingDetails = (await node.call(
        'XPENDING',
        key,
        'workers',
        '-',
        '+',
        '10',
      )) as unknown[][]
      assert.deepStrictEqual(
        pendingDetails.map(item => [item[0], item[1]]),
        [
          ['1-0', 'carol'],
          ['2-0', 'carol'],
        ],
      )
    } finally {
      node.disconnect()
    }
  })

  test('XREADGROUP history keeps deleted pending entries visible', async () => {
    const key = randomKey()
    const node = await connectToSlotOwner(redisClient!, key)
    try {
      await node.xadd(key, '1-1', 'f', '1')
      await node.xadd(key, '2-2', 'f', '2')
      await node.call('XGROUP', 'CREATE', key, 'workers', '0')
      await node.call(
        'XREADGROUP',
        'GROUP',
        'workers',
        'alice',
        'COUNT',
        '10',
        'STREAMS',
        key,
        '>',
      )
      assert.strictEqual(await node.xdel(key, '1-1'), 1)

      assert.deepStrictEqual(
        await node.call(
          'XREADGROUP',
          'GROUP',
          'workers',
          'alice',
          'STREAMS',
          key,
          '0',
        ),
        [
          [
            key,
            [
              ['1-1', null],
              ['2-2', ['f', '2']],
            ],
          ],
        ],
      )
    } finally {
      node.disconnect()
    }
  })

  test('XREADGROUP history returns an empty per-key list for consumers with no pending entries', async () => {
    const key = randomKey()
    const node = await connectToSlotOwner(redisClient!, key)
    try {
      await node.xadd(key, '1-1', 'f', '1')
      await node.call('XGROUP', 'CREATE', key, 'workers', '0')

      assert.deepStrictEqual(
        await node.call(
          'XREADGROUP',
          'GROUP',
          'workers',
          'alice',
          'STREAMS',
          key,
          '0',
        ),
        [[key, []]],
      )
    } finally {
      node.disconnect()
    }
  })

  test('XINFO reports stream, group, and consumer metadata', async () => {
    const key = randomKey()
    const node = await connectToSlotOwner(redisClient!, key)
    try {
      await node.xadd(key, '1-0', 'f', '1')
      await node.xadd(key, '2-0', 'f', '2')
      await node.call('XGROUP', 'CREATE', key, 'workers', '0')
      await node.call(
        'XREADGROUP',
        'GROUP',
        'workers',
        'alice',
        'COUNT',
        '1',
        'STREAMS',
        key,
        '>',
      )

      const streamInfo = (await node.call(
        'XINFO',
        'STREAM',
        key,
        'FULL',
        'COUNT',
        '1',
      )) as unknown[]
      assert.strictEqual(kvArrayGet(streamInfo, 'length'), 2)
      assert.strictEqual(kvArrayGet(streamInfo, 'last-generated-id'), '2-0')
      assert.deepStrictEqual(kvArrayGet(streamInfo, 'entries'), [
        ['1-0', ['f', '1']],
      ])
      assert.ok(Array.isArray(kvArrayGet(streamInfo, 'groups')))

      const groupsInfo = (await node.call(
        'XINFO',
        'GROUPS',
        key,
      )) as unknown[][]
      assert.strictEqual(groupsInfo.length, 1)
      assert.strictEqual(kvArrayGet(groupsInfo[0], 'name'), 'workers')
      assert.strictEqual(kvArrayGet(groupsInfo[0], 'consumers'), 1)
      assert.strictEqual(kvArrayGet(groupsInfo[0], 'pending'), 1)

      const consumersInfo = (await node.call(
        'XINFO',
        'CONSUMERS',
        key,
        'workers',
      )) as unknown[][]
      assert.strictEqual(consumersInfo.length, 1)
      assert.strictEqual(kvArrayGet(consumersInfo[0], 'name'), 'alice')
      assert.strictEqual(kvArrayGet(consumersInfo[0], 'pending'), 1)
    } finally {
      node.disconnect()
    }
  })

  test('XINFO STREAM FULL defaults to 10 stream entries and PEL rows', async () => {
    const key = randomKey()
    const node = await connectToSlotOwner(redisClient!, key)
    try {
      for (let i = 1; i <= 12; i++) {
        await node.xadd(key, `${i}-0`, 'f', `${i}`)
      }
      await node.call('XGROUP', 'CREATE', key, 'workers', '0')
      await node.call(
        'XREADGROUP',
        'GROUP',
        'workers',
        'alice',
        'COUNT',
        '12',
        'STREAMS',
        key,
        '>',
      )

      const streamInfo = (await node.call(
        'XINFO',
        'STREAM',
        key,
        'FULL',
      )) as unknown[]
      const entries = kvArrayGet(streamInfo, 'entries') as unknown[][]
      assert.strictEqual(entries.length, 10)
      assert.strictEqual(entries[0][0], '1-0')
      assert.strictEqual(entries[9][0], '10-0')

      const groups = kvArrayGet(streamInfo, 'groups') as unknown[][]
      const pending = kvArrayGet(groups[0], 'pending') as unknown[][]
      assert.strictEqual(pending.length, 10)
      assert.strictEqual(pending[0][0], '1-0')
      assert.strictEqual(pending[9][0], '10-0')
    } finally {
      node.disconnect()
    }
  })

  test('stream consumer group commands report Redis-compatible errors', async () => {
    const tag = randomKey()
    const key = `{${tag}}:stream`
    const node = await connectToSlotOwner(redisClient!, key)
    try {
      await assert.rejects(
        () => node.call('XGROUP', 'CREATE', key, 'workers', '0'),
        errorWithMessage(
          'ERR The XGROUP subcommand requires the key to exist. Note that for CREATE you may want to use the MKSTREAM option to create an empty stream automatically.',
        ),
      )

      await node.xadd(key, '1-0', 'f', '1')
      assert.strictEqual(
        await node.call('XGROUP', 'CREATE', key, 'workers', '0'),
        'OK',
      )
      await assert.rejects(
        () => node.call('XGROUP', 'CREATE', key, 'workers', '0'),
        errorWithMessage('BUSYGROUP Consumer Group name already exists'),
      )
      await assert.rejects(
        () =>
          node.call(
            'XREADGROUP',
            'GROUP',
            'missing',
            'alice',
            'STREAMS',
            key,
            '>',
          ),
        errorWithMessage(
          `NOGROUP No such key '${key}' or consumer group 'missing' in XREADGROUP with GROUP option`,
        ),
      )

      const stringKey = `{${tag}}:string`
      await node.set(stringKey, 'value')
      await assert.rejects(
        () => node.call('XINFO', 'GROUPS', stringKey),
        errorWithMessage(
          'WRONGTYPE Operation against a key holding the wrong kind of value',
        ),
      )
    } finally {
      node.disconnect()
    }
  })
})
