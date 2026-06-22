import { test, describe, before, after } from 'node:test'
import assert from 'node:assert'
import { RedisClusterType } from 'redis'
import { TestRunner } from '../../test-config'
import {
  connectToNodeRedisSlotOwner,
  errorWithMessage,
  flushNodeRedisCluster,
  randomKey,
} from '../../utils'

const testRunner = new TestRunner()
const MAX_UINT64 = '18446744073709551615'

// XINFO replies are RESP3 maps (objects) on node-redis; access raw via
// sendCommand so the same kebab-case keys work on mock and real backends.

describe(`Stream Commands Integration (node-redis, ${testRunner.getBackendName()})`, () => {
  let redisClient: RedisClusterType

  before(async () => {
    redisClient = (await testRunner.setupNodeRedisCluster()) as RedisClusterType
    await flushNodeRedisCluster(redisClient)
  })

  after(async () => {
    await testRunner.cleanup()
  })

  // XTRIM

  // XADD with NOMKSTREAM and MAXLEN options

  // XREAD

  // Stream consumer groups

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
})
