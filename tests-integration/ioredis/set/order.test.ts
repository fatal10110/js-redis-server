import { test, describe, before, after } from 'node:test'
import assert from 'node:assert'
import { Cluster } from 'ioredis'
import { TestRunner } from '../../test-config'
import { randomKey } from '../../utils'

const testRunner = new TestRunner()

// Real Redis stores a set of integers as an intset, sorted by value, and a
// small set of anything else as a listpack, in insertion order; every reader
// walks a set in that storage order (#504). Only orders a real 7.2+ server
// fixes are asserted here: a large set, and before 7.2 any non-intset set, is
// a hashtable, whose order is undefined. Verified against redis-server
// 7.2.16, 7.4, 8.0.6 and Valkey 8.0 / 9.0.
describe(`Set storage order (${testRunner.getBackendName()})`, () => {
  let redisClient: Cluster

  before(async () => {
    redisClient = await testRunner.setupIoredisCluster('set-order')
  })

  after(async () => {
    await testRunner.cleanup()
  })

  function keys(): (name: string) => string {
    const tag = `{set-order:${randomKey()}}`
    return name => `${tag}:${name}`
  }

  async function assertStorageOrder(key: string, expected: string[]) {
    assert.deepStrictEqual(await redisClient.smembers(key), expected)
    assert.deepStrictEqual(await redisClient.sscan(key, 0), ['0', expected])
    assert.deepStrictEqual(
      await redisClient.sort(key, 'BY', 'nosort'),
      expected,
    )
  }

  test('an integer-only set is read in ascending order', async () => {
    const k = keys()
    await redisClient.sadd(k('s'), '10', '-5', '2', '7')
    await assertStorageOrder(k('s'), ['-5', '2', '7', '10'])
  })

  test('an intset that gains a non-integer keeps its integers sorted first', async () => {
    const k = keys()
    await redisClient.sadd(k('s'), '3', '1', 'a')
    await assertStorageOrder(k('s'), ['1', '3', 'a'])
    await redisClient.sadd(k('s'), '2')
    await assertStorageOrder(k('s'), ['1', '3', 'a', '2'])
  })

  test('a set that held a non-integer never becomes an intset again', async () => {
    const k = keys()
    await redisClient.sadd(k('s'), 'a', '3', '1')
    await redisClient.srem(k('s'), 'a')
    await assertStorageOrder(k('s'), ['3', '1'])
    await redisClient.sadd(k('s'), '2')
    await assertStorageOrder(k('s'), ['3', '1', '2'])
  })

  test('COPY keeps the encoding', async () => {
    const k = keys()
    await redisClient.sadd(k('ints'), '3', '1')
    await redisClient.sadd(k('lp'), 'a', '3', '1')
    await redisClient.srem(k('lp'), 'a')
    await redisClient.copy(k('ints'), k('ints-copy'))
    await redisClient.copy(k('lp'), k('lp-copy'))
    await redisClient.sadd(k('ints-copy'), '2')
    await redisClient.sadd(k('lp-copy'), '2')
    await assertStorageOrder(k('ints-copy'), ['1', '2', '3'])
    await assertStorageOrder(k('lp-copy'), ['3', '1', '2'])
  })

  test('SMOVE creates its destination like SADD does', async () => {
    const k = keys()
    await redisClient.sadd(k('src'), 'q', '3')
    await redisClient.smove(k('src'), k('ints'), '3')
    await redisClient.sadd(k('ints'), '1')
    await assertStorageOrder(k('ints'), ['1', '3'])

    await redisClient.smove(k('src'), k('lp'), 'q')
    await redisClient.sadd(k('lp'), '1')
    await assertStorageOrder(k('lp'), ['q', '1'])
  })

  test('SINTER walks the smallest set', async () => {
    const k = keys()
    await redisClient.sadd(k('small'), 'c', 'b', 'a')
    await redisClient.sadd(k('big'), 'a', 'b', 'c', 'd')
    assert.deepStrictEqual(await redisClient.sinter(k('big'), k('small')), [
      'c',
      'b',
      'a',
    ])
    // Equal sizes keep argument order.
    await redisClient.sadd(k('other'), 'a', 'c', 'b')
    assert.deepStrictEqual(await redisClient.sinter(k('other'), k('small')), [
      'a',
      'c',
      'b',
    ])
  })

  test('SINTERSTORE stores an integer-only result as an intset', async () => {
    const k = keys()
    await redisClient.sadd(k('x'), 'q', '3', '1')
    await redisClient.sadd(k('y'), '3', '1', 'w', 'z')
    assert.deepStrictEqual(await redisClient.sinter(k('y'), k('x')), ['3', '1'])
    assert.strictEqual(await redisClient.sinterstore(k('d'), k('y'), k('x')), 2)
    await assertStorageOrder(k('d'), ['1', '3'])
  })

  test('SUNION and SDIFF of intsets are sorted', async () => {
    const k = keys()
    await redisClient.sadd(k('a'), '5', '1')
    await redisClient.sadd(k('b'), '3')
    assert.deepStrictEqual(await redisClient.sunion(k('b'), k('a')), [
      '1',
      '3',
      '5',
    ])
    assert.deepStrictEqual(await redisClient.sdiff(k('a'), k('b')), ['1', '5'])
  })

  test('SUNIONSTORE and SDIFFSTORE rebuild the result from an empty intset', async () => {
    const k = keys()
    await redisClient.sadd(k('src'), 'x', '3', '1')
    await redisClient.srem(k('src'), 'x')
    await redisClient.sadd(k('src'), 'y')
    await assertStorageOrder(k('src'), ['3', '1', 'y'])

    assert.strictEqual(await redisClient.sunionstore(k('u'), k('src')), 3)
    await assertStorageOrder(k('u'), ['1', '3', 'y'])
    assert.strictEqual(
      await redisClient.sdiffstore(k('d'), k('src'), k('missing')),
      3,
    )
    await assertStorageOrder(k('d'), ['1', '3', 'y'])
  })

  test('SDIFFSTORE result depends on the difference algorithm Redis picks', async () => {
    const k = keys()
    const letters = ['x', 'y', 'z', 'w', 'v', 'u', 't']
    await redisClient.sadd(k('f'), 'x', '5', '3', '1', ...letters.slice(1))

    // One big subtracted set: the first set is walked and only survivors are
    // added, so the result is all integers and stays an intset.
    await redisClient.sadd(k('all'), ...letters)
    assert.strictEqual(
      await redisClient.sdiffstore(k('d1'), k('f'), k('all')),
      3,
    )
    await assertStorageOrder(k('d1'), ['1', '3', '5'])

    // Many small ones: the first set is copied whole, which converts the
    // result on its first non-integer, and the rest are removed afterwards.
    const singles = letters.map(letter => k(`single-${letter}`))
    for (const [i, letter] of letters.entries()) {
      await redisClient.sadd(singles[i], letter)
    }
    assert.strictEqual(
      await redisClient.sdiffstore(k('d2'), k('f'), ...singles),
      3,
    )
    await assertStorageOrder(k('d2'), ['5', '3', '1'])
  })

  test('SRANDMEMBER and SPOP return the whole set in storage order', async () => {
    const k = keys()
    await redisClient.sadd(k('ints'), '5', '1', '3')
    await redisClient.sadd(k('lp'), 'x', '3', '1')
    await redisClient.srem(k('lp'), 'x')
    await redisClient.sadd(k('lp'), 'y')

    assert.deepStrictEqual(await redisClient.srandmember(k('ints'), 10), [
      '1',
      '3',
      '5',
    ])
    assert.deepStrictEqual(await redisClient.srandmember(k('lp'), 10), [
      '3',
      '1',
      'y',
    ])
    assert.deepStrictEqual(await redisClient.spop(k('ints'), 10), [
      '1',
      '3',
      '5',
    ])
  })

  test('SRANDMEMBER and SPOP sample a small set in storage order', async () => {
    const k = keys()
    const order = ['e', 'd', 'c', 'b', 'a']
    await redisClient.sadd(k('lp'), ...order)

    const isInStorageOrder = (sample: string[]) =>
      sample.every(
        (member, i) =>
          i === 0 || order.indexOf(sample[i - 1]) < order.indexOf(member),
      )

    for (let i = 0; i < 10; i++) {
      const sample = await redisClient.srandmember(k('lp'), 3)
      assert.strictEqual(new Set(sample).size, 3)
      assert.ok(isInStorageOrder(sample), sample.join(' '))
    }

    const popped = await redisClient.spop(k('lp'), 2)
    assert.strictEqual(popped.length, 2)
    assert.ok(isInStorageOrder(popped), popped.join(' '))
    assert.deepStrictEqual(
      await redisClient.smembers(k('lp')),
      order.filter(member => !popped.includes(member)),
    )
  })
})
