import { test, describe, before, after } from 'node:test'
import assert from 'node:assert'
import { Cluster } from 'ioredis'
import { TestRunner } from '../test-config'
import { errorWithMessage, randomKey } from '../utils'

const testRunner = new TestRunner()

describe(`Sorted Set Score-Range (ZRANGEBYSCORE/ZREVRANGEBYSCORE/ZREMRANGEBYRANK) (${testRunner.getBackendName()})`, () => {
  let redisClient: Cluster | undefined

  before(async () => {
    redisClient = await testRunner.setupIoredisCluster('zset-score-range')
  })

  after(async () => {
    await testRunner.cleanup()
  })

  async function seedScored(pairs: Array<[number, string]>): Promise<string> {
    const key = `{zsr:${randomKey()}}`
    const args: (string | number)[] = []
    for (const [score, member] of pairs) {
      args.push(score, member)
    }
    await redisClient?.zadd(key, ...(args as [number, string]))
    return key
  }

  const sample: Array<[number, string]> = [
    [1, 'a'],
    [2, 'b'],
    [3, 'c'],
    [4, 'd'],
    [5, 'e'],
  ]

  // ---------- ZRANGEBYSCORE WITHSCORES / LIMIT ----------

  test('ZRANGEBYSCORE WITHSCORES returns member/score pairs', async () => {
    const key = await seedScored(sample)
    try {
      assert.deepStrictEqual(
        await redisClient?.zrangebyscore(key, 2, 4, 'WITHSCORES'),
        ['b', '2', 'c', '3', 'd', '4'],
      )
    } finally {
      await redisClient?.del(key)
    }
  })

  test('ZRANGEBYSCORE LIMIT offset count paginates', async () => {
    const key = await seedScored(sample)
    try {
      assert.deepStrictEqual(
        await redisClient?.zrangebyscore(key, '-inf', '+inf', 'LIMIT', 1, 2),
        ['b', 'c'],
      )
    } finally {
      await redisClient?.del(key)
    }
  })

  test('ZRANGEBYSCORE LIMIT negative count returns all remaining', async () => {
    const key = await seedScored(sample)
    try {
      assert.deepStrictEqual(
        await redisClient?.zrangebyscore(
          key,
          '-inf',
          '+inf',
          'LIMIT',
          2,
          -1,
          'WITHSCORES',
        ),
        ['c', '3', 'd', '4', 'e', '5'],
      )
    } finally {
      await redisClient?.del(key)
    }
  })

  test('ZRANGEBYSCORE WITHSCORES and LIMIT in either order', async () => {
    const key = await seedScored(sample)
    try {
      const expected = ['a', '1', 'b', '2']
      assert.deepStrictEqual(
        await redisClient?.call(
          'zrangebyscore',
          key,
          '-inf',
          '+inf',
          'LIMIT',
          '0',
          '2',
          'WITHSCORES',
        ),
        expected,
      )
      assert.deepStrictEqual(
        await redisClient?.call(
          'zrangebyscore',
          key,
          '-inf',
          '+inf',
          'WITHSCORES',
          'LIMIT',
          '0',
          '2',
        ),
        expected,
      )
    } finally {
      await redisClient?.del(key)
    }
  })

  test('ZRANGEBYSCORE exclusive bound with LIMIT', async () => {
    const key = await seedScored(sample)
    try {
      assert.deepStrictEqual(await redisClient?.zrangebyscore(key, '(1', 3), [
        'b',
        'c',
      ])
    } finally {
      await redisClient?.del(key)
    }
  })

  test('ZRANGEBYSCORE LIMIT negative offset returns empty', async () => {
    const key = await seedScored(sample)
    try {
      assert.deepStrictEqual(
        await redisClient?.zrangebyscore(key, '-inf', '+inf', 'LIMIT', -1, 2),
        [],
      )
    } finally {
      await redisClient?.del(key)
    }
  })

  test('ZRANGEBYSCORE LIMIT with non-integer rejects', async () => {
    const key = await seedScored(sample)
    try {
      await assert.rejects(
        () =>
          redisClient?.call('zrangebyscore', key, '1', '2', 'LIMIT', 'a', 'b'),
        errorWithMessage('ERR value is not an integer or out of range'),
      )
    } finally {
      await redisClient?.del(key)
    }
  })

  test('ZRANGEBYSCORE LIMIT without offset/count rejects with syntax error', async () => {
    const key = await seedScored(sample)
    try {
      await assert.rejects(
        () => redisClient?.call('zrangebyscore', key, '1', '2', 'LIMIT'),
        errorWithMessage('ERR syntax error'),
      )
    } finally {
      await redisClient?.del(key)
    }
  })

  test('ZRANGEBYSCORE rejects wrong arity', async () => {
    const key = await seedScored([[1, 'a']])
    try {
      await assert.rejects(
        () => redisClient?.call('zrangebyscore', key, '1'),
        errorWithMessage(
          "ERR wrong number of arguments for 'zrangebyscore' command",
        ),
      )
    } finally {
      await redisClient?.del(key)
    }
  })

  test('ZRANGEBYSCORE rejects non-float bound', async () => {
    const key = await seedScored([[1, 'a']])
    try {
      await assert.rejects(
        () => redisClient?.call('zrangebyscore', key, 'x', '2'),
        errorWithMessage('ERR min or max is not a float'),
      )
    } finally {
      await redisClient?.del(key)
    }
  })

  test('ZRANGEBYSCORE on wrong type rejects WRONGTYPE', async () => {
    const key = `{zsr:${randomKey()}}`
    await redisClient?.set(key, 'v')
    try {
      await assert.rejects(
        () => redisClient?.call('zrangebyscore', key, '1', '2'),
        errorWithMessage(
          'WRONGTYPE Operation against a key holding the wrong kind of value',
        ),
      )
    } finally {
      await redisClient?.del(key)
    }
  })

  // ---------- ZREVRANGEBYSCORE ----------

  test('ZREVRANGEBYSCORE returns descending range with max/min order', async () => {
    const key = await seedScored(sample)
    try {
      assert.deepStrictEqual(
        await redisClient?.zrevrangebyscore(key, 4, 2, 'WITHSCORES'),
        ['d', '4', 'c', '3', 'b', '2'],
      )
    } finally {
      await redisClient?.del(key)
    }
  })

  test('ZREVRANGEBYSCORE supports LIMIT', async () => {
    const key = await seedScored(sample)
    try {
      assert.deepStrictEqual(
        await redisClient?.zrevrangebyscore(key, '+inf', '-inf', 'LIMIT', 1, 2),
        ['d', 'c'],
      )
    } finally {
      await redisClient?.del(key)
    }
  })

  test('ZREVRANGEBYSCORE exclusive bounds', async () => {
    const key = await seedScored(sample)
    try {
      assert.deepStrictEqual(
        await redisClient?.zrevrangebyscore(key, '(4', '(2'),
        ['c'],
      )
    } finally {
      await redisClient?.del(key)
    }
  })

  test('ZREVRANGEBYSCORE on missing key returns empty', async () => {
    const key = `{zsr:${randomKey()}}`
    assert.deepStrictEqual(
      await redisClient?.zrevrangebyscore(key, '+inf', '-inf'),
      [],
    )
  })

  test('ZREVRANGEBYSCORE rejects non-float bound', async () => {
    const key = await seedScored([[1, 'a']])
    try {
      await assert.rejects(
        () => redisClient?.call('zrevrangebyscore', key, 'x', '2'),
        errorWithMessage('ERR min or max is not a float'),
      )
    } finally {
      await redisClient?.del(key)
    }
  })

  test('ZREVRANGEBYSCORE on wrong type rejects WRONGTYPE', async () => {
    const key = `{zsr:${randomKey()}}`
    await redisClient?.set(key, 'v')
    try {
      await assert.rejects(
        () => redisClient?.call('zrevrangebyscore', key, '2', '1'),
        errorWithMessage(
          'WRONGTYPE Operation against a key holding the wrong kind of value',
        ),
      )
    } finally {
      await redisClient?.del(key)
    }
  })

  // ---------- ZREMRANGEBYRANK ----------

  test('ZREMRANGEBYRANK removes members in rank range', async () => {
    const key = await seedScored(sample)
    try {
      assert.strictEqual(await redisClient?.zremrangebyrank(key, 1, 2), 2)
      assert.deepStrictEqual(await redisClient?.zrange(key, 0, -1), [
        'a',
        'd',
        'e',
      ])
    } finally {
      await redisClient?.del(key)
    }
  })

  test('ZREMRANGEBYRANK supports negative ranks', async () => {
    const key = await seedScored([
      [1, 'a'],
      [2, 'b'],
      [3, 'c'],
      [4, 'd'],
    ])
    try {
      assert.strictEqual(await redisClient?.zremrangebyrank(key, -2, -1), 2)
      assert.deepStrictEqual(await redisClient?.zrange(key, 0, -1), ['a', 'b'])
    } finally {
      await redisClient?.del(key)
    }
  })

  test('ZREMRANGEBYRANK on missing key returns 0', async () => {
    const key = `{zsr:${randomKey()}}`
    assert.strictEqual(await redisClient?.zremrangebyrank(key, 0, 1), 0)
  })

  test('ZREMRANGEBYRANK removing all members deletes the key', async () => {
    const key = await seedScored([[1, 'a']])
    try {
      assert.strictEqual(await redisClient?.zremrangebyrank(key, 0, -1), 1)
      assert.strictEqual(await redisClient?.exists(key), 0)
    } finally {
      await redisClient?.del(key)
    }
  })

  test('ZREMRANGEBYRANK rejects non-integer rank', async () => {
    const key = await seedScored([[1, 'a']])
    try {
      await assert.rejects(
        () => redisClient?.call('zremrangebyrank', key, 'x', '1'),
        errorWithMessage('ERR value is not an integer or out of range'),
      )
    } finally {
      await redisClient?.del(key)
    }
  })

  test('ZREMRANGEBYRANK rejects wrong arity', async () => {
    const key = await seedScored([[1, 'a']])
    try {
      await assert.rejects(
        () => redisClient?.call('zremrangebyrank', key, '0'),
        errorWithMessage(
          "ERR wrong number of arguments for 'zremrangebyrank' command",
        ),
      )
    } finally {
      await redisClient?.del(key)
    }
  })

  test('ZREMRANGEBYRANK on wrong type rejects WRONGTYPE', async () => {
    const key = `{zsr:${randomKey()}}`
    await redisClient?.set(key, 'v')
    try {
      await assert.rejects(
        () => redisClient?.call('zremrangebyrank', key, '0', '1'),
        errorWithMessage(
          'WRONGTYPE Operation against a key holding the wrong kind of value',
        ),
      )
    } finally {
      await redisClient?.del(key)
    }
  })
})
