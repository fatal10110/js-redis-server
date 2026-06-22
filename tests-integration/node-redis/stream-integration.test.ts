import { test, describe, before, after } from 'node:test'
import assert from 'node:assert'
import { RedisClientType, RedisClusterType } from 'redis'
import { TestRunner } from '../test-config'
import {
  connectToNodeRedisSlotOwner,
  errorWithMessage,
  flushNodeRedisCluster,
  randomKey,
} from '../utils'

const testRunner = new TestRunner()
const MAX_UINT64 = '18446744073709551615'

// XINFO replies are RESP3 maps (objects) on node-redis; access raw via
// sendCommand so the same kebab-case keys work on mock and real backends.
type StreamInfo = Record<string, unknown>

describe(`Stream Commands Integration (node-redis, ${testRunner.getBackendName()})`, () => {
  let redisClient: RedisClusterType

  before(async () => {
    redisClient = (await testRunner.setupNodeRedisCluster()) as RedisClusterType
    await flushNodeRedisCluster(redisClient)
  })

  after(async () => {
    await testRunner.cleanup()
  })

  function xinfo(c: RedisClientType, args: string[]): Promise<StreamInfo> {
    return c.sendCommand(args) as Promise<StreamInfo>
  }

  test('XADD with explicit ids and XLEN', async () => {
    const key = randomKey()
    assert.strictEqual(await redisClient.xAdd(key, '1-1', { f: 'v' }), '1-1')
    assert.strictEqual(
      await redisClient.xAdd(key, '1-2', { a: 'b', c: 'd' }),
      '1-2',
    )
    assert.strictEqual(await redisClient.xLen(key), 2)
  })

  test('XADD * generates monotonically increasing ids', async () => {
    const key = randomKey()
    const id1 = await redisClient.xAdd(key, '*', { f: 'v' })
    const id2 = await redisClient.xAdd(key, '*', { f: 'v' })
    assert.match(id1, /^\d+-\d+$/)
    assert.match(id2, /^\d+-\d+$/)

    const [ms1, seq1] = id1.split('-').map(BigInt)
    const [ms2, seq2] = id2.split('-').map(BigInt)
    assert.ok(ms2 > ms1 || (ms2 === ms1 && seq2 > seq1), `${id2} > ${id1}`)
  })

  test('XADD <ms>-* auto-increments the sequence', async () => {
    const key = randomKey()
    assert.strictEqual(await redisClient.xAdd(key, '5-*', { f: 'v' }), '5-0')
    assert.strictEqual(await redisClient.xAdd(key, '5-*', { f: 'v' }), '5-1')
  })

  test('XADD handles auto-generated ids when the sequence overflows', async () => {
    const taggedKey = `{${randomKey()}}`
    const fixedMsKey = `${taggedKey}:fixed-ms`
    const autoKey = `${taggedKey}:auto`
    const node = await connectToNodeRedisSlotOwner(redisClient, fixedMsKey)
    const futureMs = BigInt(Date.now()) + 60_000n

    try {
      assert.strictEqual(
        await node.xAdd(fixedMsKey, `7-${MAX_UINT64}`, { f: 'v' }),
        `7-${MAX_UINT64}`,
      )
      await assert.rejects(
        () => node.xAdd(fixedMsKey, '7-*', { f: 'v' }),
        errorWithMessage(
          'ERR The ID specified in XADD is equal or smaller than the target stream top item',
        ),
      )

      assert.strictEqual(
        await node.xAdd(autoKey, `${futureMs}-${MAX_UINT64}`, { f: 'v' }),
        `${futureMs}-${MAX_UINT64}`,
      )
      assert.strictEqual(
        await node.xAdd(autoKey, '*', { f: 'v' }),
        `${futureMs + 1n}-0`,
      )
    } finally {
      node.destroy()
    }
  })

  test('XADD rejects ids equal to or smaller than the top item', async () => {
    const key = randomKey()
    await redisClient.xAdd(key, '5-5', { f: 'v' })

    await assert.rejects(
      () => redisClient.xAdd(key, '5-5', { f: 'v' }),
      errorWithMessage(
        'ERR The ID specified in XADD is equal or smaller than the target stream top item',
      ),
    )
    await assert.rejects(
      () => redisClient.xAdd(key, '3-0', { f: 'v' }),
      errorWithMessage(
        'ERR The ID specified in XADD is equal or smaller than the target stream top item',
      ),
    )
  })

  test('XADD rejects 0-0 and invalid ids', async () => {
    const key = randomKey()
    await assert.rejects(
      () => redisClient.xAdd(key, '0-0', { f: 'v' }),
      errorWithMessage('ERR The ID specified in XADD must be greater than 0-0'),
    )
    await assert.rejects(
      () => redisClient.xAdd(key, 'not-an-id', { f: 'v' }),
      errorWithMessage(
        'ERR Invalid stream ID specified as stream command argument',
      ),
    )
  })

  test('XSETID sets last-generated-id and advances generated XADD ids', async () => {
    const key = `{xsetid:${randomKey()}}`
    const node = await connectToNodeRedisSlotOwner(redisClient, key)
    const futureMs = BigInt(Date.now()) + 60_000n

    try {
      await node.xAdd(key, '1-0', { f: 'v' })
      assert.strictEqual(await node.xSetId(key, `${futureMs}-0`), 'OK')

      const streamInfo = await xinfo(node, ['XINFO', 'STREAM', key])
      assert.strictEqual(streamInfo['length'], 1)
      assert.strictEqual(streamInfo['last-generated-id'], `${futureMs}-0`)
      assert.strictEqual(streamInfo['entries-added'], 1)

      assert.strictEqual(await node.xAdd(key, '*', { f: 'v' }), `${futureMs}-1`)
    } finally {
      await node.del(key)
      node.destroy()
    }
  })

  test('XSETID ENTRIESADDED updates XINFO STREAM metadata', async () => {
    const key = `{xsetid-meta:${randomKey()}}`
    const node = await connectToNodeRedisSlotOwner(redisClient, key)

    try {
      await node.xAdd(key, '1-0', { f: 'v' })
      assert.strictEqual(
        await node.xSetId(key, '5-0', { ENTRIESADDED: 42 }),
        'OK',
      )

      const streamInfo = await xinfo(node, ['XINFO', 'STREAM', key])
      assert.strictEqual(streamInfo['last-generated-id'], '5-0')
      assert.strictEqual(streamInfo['entries-added'], 42)
      assert.strictEqual(await node.xAdd(key, '5-*', { f: 'next' }), '5-1')

      const updatedInfo = await xinfo(node, ['XINFO', 'STREAM', key])
      assert.strictEqual(updatedInfo['entries-added'], 43)
    } finally {
      await node.del(key)
      node.destroy()
    }
  })

  test('XSETID MAXDELETEDID updates XINFO STREAM metadata', async () => {
    const key = `{xsetid-maxdeleted:${randomKey()}}`
    const node = await connectToNodeRedisSlotOwner(redisClient, key)

    try {
      await node.xAdd(key, '1-0', { f: 'v' })
      assert.strictEqual(
        await node.xSetId(key, '5-0', {
          MAXDELETEDID: '2-0',
          ENTRIESADDED: 42,
        }),
        'OK',
      )

      const streamInfo = await xinfo(node, ['XINFO', 'STREAM', key])
      assert.strictEqual(streamInfo['last-generated-id'], '5-0')
      assert.strictEqual(streamInfo['max-deleted-entry-id'], '2-0')
      assert.strictEqual(streamInfo['entries-added'], 42)

      // Duplicate options: last one wins (raw command form).
      assert.strictEqual(
        await node.sendCommand([
          'XSETID',
          key,
          '6-0',
          'ENTRIESADDED',
          '7',
          'MAXDELETEDID',
          '3-0',
          'ENTRIESADDED',
          '9',
          'MAXDELETEDID',
          '4-0',
        ]),
        'OK',
      )

      const duplicateInfo = await xinfo(node, ['XINFO', 'STREAM', key])
      assert.strictEqual(duplicateInfo['last-generated-id'], '6-0')
      assert.strictEqual(duplicateInfo['max-deleted-entry-id'], '4-0')
      assert.strictEqual(duplicateInfo['entries-added'], 9)
    } finally {
      await node.del(key)
      node.destroy()
    }
  })

  test('XSETID rejects lower ids, invalid options, and wrong types', async () => {
    const tag = `{xsetid-errors:${randomKey()}}`
    const key = `${tag}:stream`
    const stringKey = `${tag}:string`
    const node = await connectToNodeRedisSlotOwner(redisClient, key)

    try {
      await node.xAdd(key, '5-0', { f: 'v' })
      await node.set(stringKey, 'not-a-stream')

      await assert.rejects(
        () => node.xSetId(`${tag}:missing`, '1-0'),
        errorWithMessage('ERR no such key'),
      )
      await assert.rejects(
        () => node.xSetId(key, '4-0'),
        errorWithMessage(
          'ERR The ID specified in XSETID is smaller than the target stream top item',
        ),
      )
      await assert.rejects(
        () => node.xSetId(key, 'not-an-id'),
        errorWithMessage(
          'ERR Invalid stream ID specified as stream command argument',
        ),
      )
      await assert.rejects(
        () => node.sendCommand(['XSETID', key, '6-0', 'ENTRIESADDED', 'nope']),
        errorWithMessage('ERR value is not an integer or out of range'),
      )
      await assert.rejects(
        () => node.sendCommand(['XSETID', key, '6-0', 'ENTRIESADDED']),
        errorWithMessage('ERR syntax error'),
      )
      await assert.rejects(
        () =>
          node.sendCommand(['XSETID', key, '6-0', 'MAXDELETEDID', 'bad-id']),
        errorWithMessage(
          'ERR Invalid stream ID specified as stream command argument',
        ),
      )
      await assert.rejects(
        () => node.sendCommand(['XSETID', key, '6-0', 'MAXDELETEDID']),
        errorWithMessage('ERR syntax error'),
      )
      await assert.rejects(
        () => node.sendCommand(['XSETID', key, '6-0', 'BOGUS', '1']),
        errorWithMessage('ERR syntax error'),
      )
      await assert.rejects(
        () => node.xSetId(stringKey, '6-0'),
        errorWithMessage(
          'WRONGTYPE Operation against a key holding the wrong kind of value',
        ),
      )
    } finally {
      await node.del([key, stringKey])
      node.destroy()
    }
  })

  test('XRANGE returns entries within an inclusive range', async () => {
    const key = randomKey()
    await redisClient.xAdd(key, '1-1', { a: '1' })
    await redisClient.xAdd(key, '2-1', { b: '2' })
    await redisClient.xAdd(key, '3-1', { c: '3' })

    assert.deepStrictEqual(await redisClient.xRange(key, '-', '+'), [
      { id: '1-1', message: { a: '1' } },
      { id: '2-1', message: { b: '2' } },
      { id: '3-1', message: { c: '3' } },
    ])
    assert.deepStrictEqual(await redisClient.xRange(key, '2', '2'), [
      { id: '2-1', message: { b: '2' } },
    ])
  })

  test('XRANGE honors exclusive bounds and COUNT', async () => {
    const key = randomKey()
    await redisClient.xAdd(key, '1-1', { a: '1' })
    await redisClient.xAdd(key, '2-1', { b: '2' })
    await redisClient.xAdd(key, '3-1', { c: '3' })

    assert.deepStrictEqual(await redisClient.xRange(key, '(1-1', '+'), [
      { id: '2-1', message: { b: '2' } },
      { id: '3-1', message: { c: '3' } },
    ])
    assert.deepStrictEqual(
      await redisClient.xRange(key, '-', '+', { COUNT: 2 }),
      [
        { id: '1-1', message: { a: '1' } },
        { id: '2-1', message: { b: '2' } },
      ],
    )
  })

  test('XRANGE and XREVRANGE reject non-integer COUNT values', async () => {
    const key = randomKey()
    const node = await connectToNodeRedisSlotOwner(redisClient, key)
    try {
      await node.xAdd(key, '1-1', { a: '1' })

      const invalidCount = errorWithMessage(
        'ERR value is not an integer or out of range',
      )
      await assert.rejects(
        () => node.sendCommand(['XRANGE', key, '-', '+', 'COUNT', 'abc']),
        invalidCount,
      )
      await assert.rejects(
        () => node.sendCommand(['XREVRANGE', key, '+', '-', 'COUNT', 'abc']),
        invalidCount,
      )
    } finally {
      node.destroy()
    }
  })

  test('XREVRANGE returns entries in descending order', async () => {
    const key = randomKey()
    await redisClient.xAdd(key, '1-1', { a: '1' })
    await redisClient.xAdd(key, '2-1', { b: '2' })

    assert.deepStrictEqual(await redisClient.xRevRange(key, '+', '-'), [
      { id: '2-1', message: { b: '2' } },
      { id: '1-1', message: { a: '1' } },
    ])
  })

  test('XDEL removes entries, keeps the empty stream, and retains last id', async () => {
    const key = randomKey()
    await redisClient.xAdd(key, '1-1', { a: '1' })
    await redisClient.xAdd(key, '2-1', { b: '2' })

    assert.strictEqual(await redisClient.xDel(key, ['1-1', '9-9']), 1)
    assert.strictEqual(await redisClient.xLen(key), 1)

    assert.strictEqual(await redisClient.xDel(key, '2-1'), 1)
    assert.strictEqual(await redisClient.xLen(key), 0)
    assert.strictEqual(await redisClient.exists(key), 1)

    await assert.rejects(
      () => redisClient.xAdd(key, '2-1', { f: 'v' }),
      errorWithMessage(
        'ERR The ID specified in XADD is equal or smaller than the target stream top item',
      ),
    )
  })

  test('XDEL on a missing key returns 0', async () => {
    const key = randomKey()
    assert.strictEqual(await redisClient.xDel(key, '1-1'), 0)
    assert.strictEqual(await redisClient.exists(key), 0)
  })

  test('stream commands reject keys holding another type', async () => {
    const key = randomKey()
    await redisClient.set(key, 'x')

    const wrongType = errorWithMessage(
      'WRONGTYPE Operation against a key holding the wrong kind of value',
    )
    await assert.rejects(
      () => redisClient.xAdd(key, '1-1', { f: 'v' }),
      wrongType,
    )
    await assert.rejects(() => redisClient.xLen(key), wrongType)
    await assert.rejects(() => redisClient.xRange(key, '-', '+'), wrongType)
  })

  test('XLEN and XRANGE on a missing key return empty results', async () => {
    const key = randomKey()
    assert.strictEqual(await redisClient.xLen(key), 0)
    assert.deepStrictEqual(await redisClient.xRange(key, '-', '+'), [])
  })

  // XTRIM

  test('XTRIM MAXLEN removes oldest entries and returns removed count', async () => {
    const key = randomKey()
    await redisClient.xAdd(key, '1-1', { f: 'v' })
    await redisClient.xAdd(key, '2-1', { f: 'v' })
    await redisClient.xAdd(key, '3-1', { f: 'v' })

    assert.strictEqual(await redisClient.xTrim(key, 'MAXLEN', 2), 1)
    assert.deepStrictEqual(await redisClient.xRange(key, '-', '+'), [
      { id: '2-1', message: { f: 'v' } },
      { id: '3-1', message: { f: 'v' } },
    ])
  })

  test('XTRIM MAXLEN with ~ (approximate) does not exact-trim tiny streams', async () => {
    const key = randomKey()
    const node = await connectToNodeRedisSlotOwner(redisClient, key)
    try {
      await node.xAdd(key, '1-1', { f: 'v' })
      await node.xAdd(key, '2-1', { f: 'v' })
      await node.xAdd(key, '3-1', { f: 'v' })

      const removed = await node.xTrim(key, 'MAXLEN', 2, {
        strategyModifier: '~',
      })
      assert.strictEqual(removed, 0)
      assert.strictEqual(await node.xLen(key), 3)
      assert.deepStrictEqual(await node.xRange(key, '-', '+'), [
        { id: '1-1', message: { f: 'v' } },
        { id: '2-1', message: { f: 'v' } },
        { id: '3-1', message: { f: 'v' } },
      ])
    } finally {
      node.destroy()
    }
  })

  test('XTRIM MAXLEN with ~ accepts LIMIT count', async () => {
    const key = randomKey()
    const node = await connectToNodeRedisSlotOwner(redisClient, key)
    try {
      await node.xAdd(key, '1-1', { f: 'v' })
      await node.xAdd(key, '2-1', { f: 'v' })
      await node.xAdd(key, '3-1', { f: 'v' })

      const removed = await node.xTrim(key, 'MAXLEN', 2, {
        strategyModifier: '~',
        LIMIT: 1,
      })
      assert.ok(typeof removed === 'number' && removed >= 0)
    } finally {
      node.destroy()
    }
  })

  test('XTRIM LIMIT validates approximate trim syntax', async () => {
    const key = randomKey()
    const node = await connectToNodeRedisSlotOwner(redisClient, key)
    try {
      await node.xAdd(key, '1-1', { f: 'v' })
      await node.xAdd(key, '2-1', { f: 'v' })
      await node.xAdd(key, '3-1', { f: 'v' })

      await assert.rejects(
        () => node.sendCommand(['XTRIM', key, 'MAXLEN', '2', 'LIMIT', '1']),
        errorWithMessage(
          'ERR syntax error, LIMIT cannot be used without the special ~ option',
        ),
      )
      await assert.rejects(
        () => node.sendCommand(['XTRIM', key, 'MAXLEN', '~', '2', 'LIMIT']),
        errorWithMessage('ERR syntax error'),
      )
      await assert.rejects(
        () =>
          node.sendCommand(['XTRIM', key, 'MAXLEN', '~', '2', 'LIMIT', 'abc']),
        errorWithMessage('ERR value is not an integer or out of range'),
      )
      await assert.rejects(
        () =>
          node.sendCommand(['XTRIM', key, 'MAXLEN', '~', '2', 'LIMIT', '-1']),
        errorWithMessage('ERR The LIMIT argument must be >= 0.'),
      )
      const removed = (await node.sendCommand([
        'XTRIM',
        key,
        'MAXLEN',
        '~',
        '2',
        'LIMIT',
        '1',
        'LIMIT',
        '1',
      ])) as number
      assert.ok(typeof removed === 'number' && removed >= 0)
    } finally {
      node.destroy()
    }
  })

  test('XTRIM MINID removes entries with id below threshold', async () => {
    const key = randomKey()
    const node = await connectToNodeRedisSlotOwner(redisClient, key)
    try {
      await node.xAdd(key, '1-0', { f: 'v' })
      await node.xAdd(key, '2-0', { f: 'v' })
      await node.xAdd(key, '3-0', { f: 'v' })

      assert.strictEqual(await node.xTrim(key, 'MINID', 2), 1)
      assert.deepStrictEqual(await node.xRange(key, '-', '+'), [
        { id: '2-0', message: { f: 'v' } },
        { id: '3-0', message: { f: 'v' } },
      ])
    } finally {
      node.destroy()
    }
  })

  test('XTRIM on missing key returns 0 without creating it', async () => {
    const key = randomKey()
    assert.strictEqual(await redisClient.xTrim(key, 'MAXLEN', 5), 0)
    assert.strictEqual(await redisClient.exists(key), 0)
  })

  test('XTRIM MAXLEN no-op when stream is within limit', async () => {
    const key = randomKey()
    await redisClient.xAdd(key, '1-1', { f: 'v' })
    await redisClient.xAdd(key, '2-1', { f: 'v' })

    assert.strictEqual(await redisClient.xTrim(key, 'MAXLEN', 5), 0)
    assert.strictEqual(await redisClient.xLen(key), 2)
  })

  // XADD with NOMKSTREAM and MAXLEN options

  test('XADD NOMKSTREAM returns null when key does not exist', async () => {
    const key = randomKey()
    const node = await connectToNodeRedisSlotOwner(redisClient, key)
    try {
      // node-redis xAdd has no NOMKSTREAM option — use the raw command.
      const result = await node.sendCommand([
        'XADD',
        key,
        'NOMKSTREAM',
        '1-1',
        'f',
        'v',
      ])
      assert.strictEqual(result, null)
      assert.strictEqual(await node.exists(key), 0)
    } finally {
      node.destroy()
    }
  })

  test('XADD NOMKSTREAM appends to an existing stream normally', async () => {
    const key = randomKey()
    const node = await connectToNodeRedisSlotOwner(redisClient, key)
    try {
      await node.xAdd(key, '1-1', { f: 'v' })
      const result = await node.sendCommand([
        'XADD',
        key,
        'NOMKSTREAM',
        '2-1',
        'g',
        'w',
      ])
      assert.strictEqual(result, '2-1')
      assert.strictEqual(await node.xLen(key), 2)
    } finally {
      node.destroy()
    }
  })

  test('XADD MAXLEN trims oldest entries after append', async () => {
    const key = randomKey()
    const node = await connectToNodeRedisSlotOwner(redisClient, key)
    try {
      await node.xAdd(key, '1-1', { f: 'v' })
      await node.xAdd(key, '2-1', { f: 'v' })
      await node.xAdd(key, '3-1', { f: 'v' })
      await node.xAdd(
        key,
        '4-1',
        { f: 'v' },
        {
          TRIM: { strategy: 'MAXLEN', threshold: 2 },
        },
      )
      assert.deepStrictEqual(await node.xRange(key, '-', '+'), [
        { id: '3-1', message: { f: 'v' } },
        { id: '4-1', message: { f: 'v' } },
      ])
    } finally {
      node.destroy()
    }
  })

  test('XADD MAXLEN with ~ accepts LIMIT count before generated id', async () => {
    const key = randomKey()
    const node = await connectToNodeRedisSlotOwner(redisClient, key)
    try {
      await node.xAdd(key, '1-1', { f: 'v' })
      await node.xAdd(key, '2-1', { f: 'v' })
      await node.xAdd(key, '3-1', { f: 'v' })

      const id = await node.xAdd(
        key,
        '*',
        { f: 'v' },
        {
          TRIM: {
            strategy: 'MAXLEN',
            strategyModifier: '~',
            threshold: 2,
            limit: 1,
          },
        },
      )
      assert.match(id, /^\d+-\d+$/)
    } finally {
      node.destroy()
    }
  })

  test('XADD LIMIT validates approximate trim syntax before id', async () => {
    const key = randomKey()
    const node = await connectToNodeRedisSlotOwner(redisClient, key)
    try {
      await node.xAdd(key, '1-1', { f: 'v' })

      await assert.rejects(
        () =>
          node.sendCommand([
            'XADD',
            key,
            'MAXLEN',
            '2',
            'LIMIT',
            '1',
            '*',
            'f',
            'v',
          ]),
        errorWithMessage(
          'ERR syntax error, LIMIT cannot be used without the special ~ option',
        ),
      )
      await assert.rejects(
        () =>
          node.sendCommand([
            'XADD',
            key,
            'MAXLEN',
            '~',
            '2',
            'LIMIT',
            '*',
            'f',
            'v',
          ]),
        errorWithMessage('ERR value is not an integer or out of range'),
      )
      await assert.rejects(
        () =>
          node.sendCommand([
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
          ]),
        errorWithMessage('ERR The LIMIT argument must be >= 0.'),
      )
    } finally {
      node.destroy()
    }
  })

  // XREAD

  test('XREAD returns entries after the given id', async () => {
    const key = randomKey()
    const node = await connectToNodeRedisSlotOwner(redisClient, key)
    try {
      await node.xAdd(key, '1-1', { f: '1' })
      await node.xAdd(key, '2-1', { f: '2' })
      await node.xAdd(key, '3-1', { f: '3' })

      const result = await node.xRead({ key, id: '1-1' })
      assert.ok(result)
      assert.strictEqual(result.length, 1)
      assert.strictEqual(result[0].name, key)
      assert.deepStrictEqual(result[0].messages, [
        { id: '2-1', message: { f: '2' } },
        { id: '3-1', message: { f: '3' } },
      ])
    } finally {
      node.destroy()
    }
  })

  test('XREAD parses entries over a default RESP3 connection', async () => {
    const key = `{stream-resp3:${randomKey()}}:stream`
    const node = await connectToNodeRedisSlotOwner(redisClient, key)
    try {
      const id = await node.xAdd(key, '*', { field1: 'value1' })
      assert.match(id, /^\d+-\d+$/)

      const result = await node.xRead({ key, id: '0-0' })
      assert.ok(result)
      assert.strictEqual(result.length, 1)
      assert.strictEqual(result[0].name, key)
      assert.strictEqual(result[0].messages.length, 1)
      assert.strictEqual(result[0].messages[0].id, id)
      assert.deepStrictEqual(result[0].messages[0].message, {
        field1: 'value1',
      })
    } finally {
      await node.del(key)
      node.destroy()
    }
  })

  test('XREAD COUNT limits the number of returned entries', async () => {
    const key = randomKey()
    const node = await connectToNodeRedisSlotOwner(redisClient, key)
    try {
      await node.xAdd(key, '1-1', { f: '1' })
      await node.xAdd(key, '2-1', { f: '2' })
      await node.xAdd(key, '3-1', { f: '3' })

      const result = await node.xRead({ key, id: '0-0' }, { COUNT: 1 })
      assert.ok(result)
      assert.strictEqual(result[0].messages.length, 1)
      assert.strictEqual(result[0].messages[0].id, '1-1')
    } finally {
      node.destroy()
    }
  })

  test('XREAD returns null when no new entries exist for the given id', async () => {
    const key = randomKey()
    const node = await connectToNodeRedisSlotOwner(redisClient, key)
    try {
      await node.xAdd(key, '1-1', { f: '1' })
      assert.strictEqual(await node.xRead({ key, id: '1-1' }), null)
    } finally {
      node.destroy()
    }
  })

  test('XREAD with $ id returns null (no entries after current last)', async () => {
    const key = randomKey()
    const node = await connectToNodeRedisSlotOwner(redisClient, key)
    try {
      await node.xAdd(key, '1-1', { f: '1' })
      assert.strictEqual(await node.xRead({ key, id: '$' }), null)
    } finally {
      node.destroy()
    }
  })

  test('XREAD on a missing key returns null', async () => {
    const key = randomKey()
    const node = await connectToNodeRedisSlotOwner(redisClient, key)
    try {
      assert.strictEqual(await node.xRead({ key, id: '0-0' }), null)
    } finally {
      node.destroy()
    }
  })

  test('XREAD from multiple streams on same slot returns combined results', async () => {
    const tag = Math.random().toString(36).substring(2, 8)
    const key1 = `{${tag}}s1`
    const key2 = `{${tag}}s2`
    const node = await connectToNodeRedisSlotOwner(redisClient, key1)
    try {
      await node.xAdd(key1, '1-1', { a: '1' })
      await node.xAdd(key2, '2-1', { b: '2' })

      const result = await node.xRead([
        { key: key1, id: '0-0' },
        { key: key2, id: '0-0' },
      ])
      assert.ok(result)
      assert.strictEqual(result.length, 2)
    } finally {
      node.destroy()
    }
  })

  // Stream consumer groups

  test('XGROUP creates, mutates, and destroys consumer groups', async () => {
    const key = randomKey()
    const node = await connectToNodeRedisSlotOwner(redisClient, key)
    try {
      await node.xAdd(key, '1-0', { f: '1' })
      await node.xAdd(key, '2-0', { f: '2' })

      assert.strictEqual(await node.xGroupCreate(key, 'workers', '0'), 'OK')
      assert.strictEqual(
        await node.xGroupCreateConsumer(key, 'workers', 'alice'),
        1,
      )
      assert.strictEqual(
        await node.xGroupCreateConsumer(key, 'workers', 'alice'),
        0,
      )

      const read = await node.xReadGroup(
        'workers',
        'alice',
        { key, id: '>' },
        { COUNT: 2 },
      )
      assert.deepStrictEqual(read, [
        {
          name: key,
          messages: [
            { id: '1-0', message: { f: '1' } },
            { id: '2-0', message: { f: '2' } },
          ],
        },
      ])

      const pendingSummary = await node.xPending(key, 'workers')
      assert.strictEqual(pendingSummary.pending, 2)
      assert.strictEqual(pendingSummary.firstId, '1-0')
      assert.strictEqual(pendingSummary.lastId, '2-0')
      assert.ok(Array.isArray(pendingSummary.consumers))

      assert.strictEqual(await node.xAck(key, 'workers', '1-0'), 1)
      const pendingDetails = await node.xPendingRange(
        key,
        'workers',
        '-',
        '+',
        10,
      )
      assert.strictEqual(pendingDetails.length, 1)
      assert.strictEqual(pendingDetails[0].id, '2-0')
      assert.strictEqual(pendingDetails[0].consumer, 'alice')
      assert.strictEqual(pendingDetails[0].deliveriesCounter, 1)

      const consumers = await node.xInfoConsumers(key, 'workers')
      const alice = consumers.find(item => item.name === 'alice')
      assert.ok(alice)
      assert.strictEqual(alice.pending, 1)

      assert.strictEqual(
        await node.xGroupDelConsumer(key, 'workers', 'alice'),
        1,
      )
      assert.strictEqual(await node.xGroupDestroy(key, 'workers'), 1)
      assert.strictEqual(await node.xGroupDestroy(key, 'workers'), 0)
    } finally {
      node.destroy()
    }
  })

  test('XGROUP MKSTREAM and SETID control group delivery position', async () => {
    const key = randomKey()
    const node = await connectToNodeRedisSlotOwner(redisClient, key)
    try {
      assert.strictEqual(
        await node.xGroupCreate(key, 'workers', '$', { MKSTREAM: true }),
        'OK',
      )
      assert.strictEqual(await node.xLen(key), 0)

      await node.xAdd(key, '1-0', { f: '1' })
      await node.xAdd(key, '2-0', { f: '2' })
      assert.deepStrictEqual(
        await node.xReadGroup('workers', 'alice', { key, id: '>' }),
        [
          {
            name: key,
            messages: [
              { id: '1-0', message: { f: '1' } },
              { id: '2-0', message: { f: '2' } },
            ],
          },
        ],
      )

      assert.strictEqual(await node.xGroupSetId(key, 'workers', '$'), 'OK')
      await node.xAdd(key, '3-0', { f: '3' })
      assert.deepStrictEqual(
        await node.xReadGroup('workers', 'bob', { key, id: '>' }),
        [{ name: key, messages: [{ id: '3-0', message: { f: '3' } }] }],
      )
    } finally {
      node.destroy()
    }
  })

  test('XCLAIM and XAUTOCLAIM transfer pending stream entries', async () => {
    const key = randomKey()
    const node = await connectToNodeRedisSlotOwner(redisClient, key)
    try {
      await node.xAdd(key, '1-0', { f: '1' })
      await node.xAdd(key, '2-0', { f: '2' })
      await node.xGroupCreate(key, 'workers', '0')
      await node.xReadGroup('workers', 'alice', { key, id: '>' }, { COUNT: 2 })

      assert.deepStrictEqual(
        await node.xClaim(key, 'workers', 'bob', 0, '1-0'),
        [{ id: '1-0', message: { f: '1' } }],
      )

      const claimed = await node.xAutoClaim(key, 'workers', 'carol', 0, '0-0', {
        COUNT: 10,
      })
      assert.strictEqual(claimed.nextId, '0-0')
      assert.deepStrictEqual(
        claimed.messages.map(entry => entry.id),
        ['1-0', '2-0'],
      )
      assert.deepStrictEqual(claimed.deletedMessages, [])

      const pendingDetails = await node.xPendingRange(
        key,
        'workers',
        '-',
        '+',
        10,
      )
      assert.deepStrictEqual(
        pendingDetails.map(item => [item.id, item.consumer]),
        [
          ['1-0', 'carol'],
          ['2-0', 'carol'],
        ],
      )
    } finally {
      node.destroy()
    }
  })

  test('XREADGROUP history keeps deleted pending entries visible', async () => {
    const key = randomKey()
    const node = await connectToNodeRedisSlotOwner(redisClient, key)
    try {
      await node.xAdd(key, '1-1', { f: '1' })
      await node.xAdd(key, '2-2', { f: '2' })
      await node.xGroupCreate(key, 'workers', '0')
      await node.xReadGroup('workers', 'alice', { key, id: '>' }, { COUNT: 10 })
      assert.strictEqual(await node.xDel(key, '1-1'), 1)

      // A deleted PEL entry comes back with a nil message; node-redis' typed
      // xReadGroup transform throws on that, so read the raw reply instead.
      const history = (await node.sendCommand([
        'XREADGROUP',
        'GROUP',
        'workers',
        'alice',
        'STREAMS',
        key,
        '0',
      ])) as Record<string, unknown>
      assert.deepStrictEqual(history[key], [
        ['1-1', null],
        ['2-2', ['f', '2']],
      ])
    } finally {
      node.destroy()
    }
  })

  test('XREADGROUP history returns an empty per-key list for consumers with no pending entries', async () => {
    const key = randomKey()
    const node = await connectToNodeRedisSlotOwner(redisClient, key)
    try {
      await node.xAdd(key, '1-1', { f: '1' })
      await node.xGroupCreate(key, 'workers', '0')

      assert.deepStrictEqual(
        await node.xReadGroup('workers', 'alice', { key, id: '0' }),
        [{ name: key, messages: [] }],
      )
    } finally {
      node.destroy()
    }
  })

  test('XINFO reports stream, group, and consumer metadata', async () => {
    const key = randomKey()
    const node = await connectToNodeRedisSlotOwner(redisClient, key)
    try {
      await node.xAdd(key, '1-0', { f: '1' })
      await node.xAdd(key, '2-0', { f: '2' })
      await node.xGroupCreate(key, 'workers', '0')
      await node.xReadGroup('workers', 'alice', { key, id: '>' }, { COUNT: 1 })

      const streamInfo = await xinfo(node, [
        'XINFO',
        'STREAM',
        key,
        'FULL',
        'COUNT',
        '1',
      ])
      assert.strictEqual(streamInfo['length'], 2)
      assert.strictEqual(streamInfo['last-generated-id'], '2-0')
      assert.deepStrictEqual(streamInfo['entries'], [['1-0', ['f', '1']]])
      assert.ok(Array.isArray(streamInfo['groups']))

      const groupsInfo = await node.xInfoGroups(key)
      assert.strictEqual(groupsInfo.length, 1)
      assert.strictEqual(groupsInfo[0].name, 'workers')
      assert.strictEqual(groupsInfo[0].consumers, 1)
      assert.strictEqual(groupsInfo[0].pending, 1)

      const consumersInfo = await node.xInfoConsumers(key, 'workers')
      assert.strictEqual(consumersInfo.length, 1)
      assert.strictEqual(consumersInfo[0].name, 'alice')
      assert.strictEqual(consumersInfo[0].pending, 1)
    } finally {
      node.destroy()
    }
  })

  test('XINFO STREAM FULL defaults to 10 stream entries and PEL rows', async () => {
    const key = randomKey()
    const node = await connectToNodeRedisSlotOwner(redisClient, key)
    try {
      for (let i = 1; i <= 12; i++) {
        await node.xAdd(key, `${i}-0`, { f: `${i}` })
      }
      await node.xGroupCreate(key, 'workers', '0')
      await node.xReadGroup('workers', 'alice', { key, id: '>' }, { COUNT: 12 })

      const streamInfo = await xinfo(node, ['XINFO', 'STREAM', key, 'FULL'])
      const entries = streamInfo['entries'] as unknown[][]
      assert.strictEqual(entries.length, 10)
      assert.strictEqual(entries[0][0], '1-0')
      assert.strictEqual(entries[9][0], '10-0')

      const groups = streamInfo['groups'] as StreamInfo[]
      const pending = groups[0]['pending'] as unknown[][]
      assert.strictEqual(pending.length, 10)
      assert.strictEqual(pending[0][0], '1-0')
      assert.strictEqual(pending[9][0], '10-0')
    } finally {
      node.destroy()
    }
  })

  test('stream consumer group commands report Redis-compatible errors', async () => {
    const tag = randomKey()
    const key = `{${tag}}:stream`
    const node = await connectToNodeRedisSlotOwner(redisClient, key)
    try {
      await assert.rejects(
        () => node.xGroupCreate(key, 'workers', '0'),
        errorWithMessage(
          'ERR The XGROUP subcommand requires the key to exist. Note that for CREATE you may want to use the MKSTREAM option to create an empty stream automatically.',
        ),
      )

      await node.xAdd(key, '1-0', { f: '1' })
      assert.strictEqual(await node.xGroupCreate(key, 'workers', '0'), 'OK')
      await assert.rejects(
        () => node.xGroupCreate(key, 'workers', '0'),
        errorWithMessage('BUSYGROUP Consumer Group name already exists'),
      )
      await assert.rejects(
        () => node.xReadGroup('missing', 'alice', { key, id: '>' }),
        errorWithMessage(
          `NOGROUP No such key '${key}' or consumer group 'missing' in XREADGROUP with GROUP option`,
        ),
      )

      const stringKey = `{${tag}}:string`
      await node.set(stringKey, 'value')
      await assert.rejects(
        () => node.xInfoGroups(stringKey),
        errorWithMessage(
          'WRONGTYPE Operation against a key holding the wrong kind of value',
        ),
      )
    } finally {
      node.destroy()
    }
  })
})
