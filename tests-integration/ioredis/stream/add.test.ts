import { test, describe, before, after } from 'node:test'
import assert from 'node:assert'
import { Cluster } from 'ioredis'
import { TestRunner } from '../../test-config'
import { connectToSlotOwner, errorWithMessage, randomKey } from '../../utils'

const testRunner = new TestRunner()
const MAX_UINT64 = '18446744073709551615'

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
        await node.xadd(fixedMsKey, `7-${MAX_UINT64}`, 'f', 'v'),
        `7-${MAX_UINT64}`,
      )
      await assert.rejects(
        () => node.xadd(fixedMsKey, '7-*', 'f', 'v'),
        errorWithMessage(
          'ERR The ID specified in XADD is equal or smaller than the target stream top item',
        ),
      )

      assert.strictEqual(
        await node.xadd(autoKey, `${futureMs}-${MAX_UINT64}`, 'f', 'v'),
        `${futureMs}-${MAX_UINT64}`,
      )
      assert.strictEqual(
        await node.xadd(autoKey, '*', 'f', 'v'),
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

  test('XADD NOMKSTREAM returns null when key does not exist', async () => {
    const key = randomKey()
    const node = await connectToSlotOwner(redisClient!, key)
    try {
      const result = await node.xadd(key, 'NOMKSTREAM', '1-1', 'f', 'v')
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
      const result = await node.xadd(key, 'NOMKSTREAM', '2-1', 'g', 'w')
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
      await node.xadd(key, 'MAXLEN', '2', '4-1', 'f', 'v')
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

      const id = (await node.xadd(
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

      const duplicateLimitId = (await node.xadd(
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
        () => node.xadd(key, 'MAXLEN', '2', 'LIMIT', '1', '*', 'f', 'v'),
        errorWithMessage(
          'ERR syntax error, LIMIT cannot be used without the special ~ option',
        ),
      )
      await assert.rejects(
        () => node.xadd(key, 'MAXLEN', '~', '2', 'LIMIT', '*', 'f', 'v'),
        errorWithMessage('ERR value is not an integer or out of range'),
      )
      await assert.rejects(
        () => node.xadd(key, 'MAXLEN', '~', '2', 'LIMIT', '-1', '*', 'f', 'v'),
        errorWithMessage('ERR The LIMIT argument must be >= 0.'),
      )
    } finally {
      node.disconnect()
    }
  })
})
