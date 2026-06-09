import { test, describe, before, after } from 'node:test'
import assert from 'node:assert'
import { Cluster } from 'ioredis'
import { TestRunner } from '../test-config'
import { errorWithMessage, randomKey } from '../utils'

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
})
