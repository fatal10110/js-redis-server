import { test, describe, before, after } from 'node:test'
import assert from 'node:assert'
import { RedisClientType, RedisClusterType } from 'redis'
import { TestRunner } from '../../test-config'
import {
  connectToNodeRedisSlotOwner,
  errorWithMessage,
  flushNodeRedisCluster,
  randomKey,
} from '../../utils'

const testRunner = new TestRunner()

describe(`Sorted Set Set-Operations (node-redis, ${testRunner.getBackendName()})`, () => {
  let redisClient: RedisClusterType

  before(async () => {
    redisClient = (await testRunner.setupNodeRedisCluster()) as RedisClusterType
    await flushNodeRedisCluster(redisClient)
  })

  after(async () => {
    await testRunner.cleanup()
  })

  // All keys in a single op must hash to the same slot, so every test shares a
  // hash tag and talks to that slot's owner through a prefix-free directClient.
  async function withOps(
    fn: (client: RedisClientType, k: (name: string) => string) => Promise<void>,
  ): Promise<void> {
    const tag = `{zsetops:${randomKey()}}`
    const k = (name: string) => `${tag}:${name}`
    const directClient = await connectToNodeRedisSlotOwner(
      redisClient,
      k('seed'),
    )
    try {
      await fn(directClient, k)
    } finally {
      directClient.destroy()
    }
  }

  // ---------------------------------------------------------------- ZUNIONSTORE

  test('ZUNIONSTORE sums scores by default and stores the result', async () => {
    await withOps(async (c, k) => {
      await c.zAdd(k('z1'), [
        { score: 1, value: 'a' },
        { score: 2, value: 'b' },
        { score: 3, value: 'c' },
      ])
      await c.zAdd(k('z2'), [
        { score: 10, value: 'b' },
        { score: 20, value: 'c' },
        { score: 30, value: 'd' },
      ])

      const n = await c.zUnionStore(k('dest'), [k('z1'), k('z2')])
      assert.strictEqual(n, 4)
      assert.deepStrictEqual(await c.zRangeWithScores(k('dest'), 0, -1), [
        { value: 'a', score: 1 },
        { value: 'b', score: 12 },
        { value: 'c', score: 23 },
        { value: 'd', score: 30 },
      ])
    })
  })

  test('ZUNIONSTORE applies WEIGHTS before aggregating', async () => {
    await withOps(async (c, k) => {
      await c.zAdd(k('z1'), [
        { score: 1, value: 'a' },
        { score: 2, value: 'b' },
        { score: 3, value: 'c' },
      ])
      await c.zAdd(k('z2'), [
        { score: 10, value: 'b' },
        { score: 20, value: 'c' },
        { score: 30, value: 'd' },
      ])

      await c.zUnionStore(k('dest'), [
        { key: k('z1'), weight: 2 },
        { key: k('z2'), weight: 3 },
      ])
      assert.deepStrictEqual(await c.zRangeWithScores(k('dest'), 0, -1), [
        { value: 'a', score: 2 },
        { value: 'b', score: 34 },
        { value: 'c', score: 66 },
        { value: 'd', score: 90 },
      ])
    })
  })

  test('ZUNIONSTORE honors AGGREGATE MIN and MAX', async () => {
    await withOps(async (c, k) => {
      await c.zAdd(k('z1'), [
        { score: 1, value: 'a' },
        { score: 2, value: 'b' },
        { score: 3, value: 'c' },
      ])
      await c.zAdd(k('z2'), [
        { score: 10, value: 'b' },
        { score: 20, value: 'c' },
        { score: 30, value: 'd' },
      ])

      await c.zUnionStore(k('dest'), [k('z1'), k('z2')], { AGGREGATE: 'MIN' })
      assert.deepStrictEqual(await c.zRangeWithScores(k('dest'), 0, -1), [
        { value: 'a', score: 1 },
        { value: 'b', score: 2 },
        { value: 'c', score: 3 },
        { value: 'd', score: 30 },
      ])

      await c.zUnionStore(k('dest'), [k('z1'), k('z2')], { AGGREGATE: 'MAX' })
      assert.deepStrictEqual(await c.zRangeWithScores(k('dest'), 0, -1), [
        { value: 'a', score: 1 },
        { value: 'b', score: 10 },
        { value: 'c', score: 20 },
        { value: 'd', score: 30 },
      ])
    })
  })

  test('ZUNIONSTORE treats a plain set source as scores of 1', async () => {
    await withOps(async (c, k) => {
      await c.zAdd(k('z1'), [
        { score: 1, value: 'a' },
        { score: 2, value: 'b' },
        { score: 3, value: 'c' },
      ])
      await c.sAdd(k('s1'), ['a', 'b', 'x'])

      await c.zUnionStore(k('dest'), [k('z1'), k('s1')])
      assert.deepStrictEqual(await c.zRangeWithScores(k('dest'), 0, -1), [
        { value: 'x', score: 1 },
        { value: 'a', score: 2 },
        { value: 'b', score: 3 },
        { value: 'c', score: 3 },
      ])
    })
  })

  test('ZUNIONSTORE resets a SUM that becomes NaN (inf + -inf) to 0', async () => {
    await withOps(async (c, k) => {
      await c.zAdd(k('za'), { score: Infinity, value: 'm' })
      await c.zAdd(k('zb'), { score: -Infinity, value: 'm' })

      await c.zUnionStore(k('dest'), [k('za'), k('zb')])
      assert.strictEqual(await c.zScore(k('dest'), 'm'), 0)
    })
  })

  // ---------------------------------------------------------------- ZINTERSTORE

  test('ZINTERSTORE keeps only common members, summing scores', async () => {
    await withOps(async (c, k) => {
      await c.zAdd(k('z1'), [
        { score: 1, value: 'a' },
        { score: 2, value: 'b' },
        { score: 3, value: 'c' },
      ])
      await c.zAdd(k('z2'), [
        { score: 10, value: 'b' },
        { score: 20, value: 'c' },
        { score: 30, value: 'd' },
      ])

      const n = await c.zInterStore(k('dest'), [k('z1'), k('z2')])
      assert.strictEqual(n, 2)
      assert.deepStrictEqual(await c.zRangeWithScores(k('dest'), 0, -1), [
        { value: 'b', score: 12 },
        { value: 'c', score: 23 },
      ])
    })
  })

  test('ZINTERSTORE with empty result deletes the destination key', async () => {
    await withOps(async (c, k) => {
      await c.zAdd(k('z1'), { score: 1, value: 'a' })
      await c.zAdd(k('z2'), { score: 1, value: 'b' })
      await c.set(k('dest'), 'preexisting')

      const n = await c.zInterStore(k('dest'), [k('z1'), k('z2')])
      assert.strictEqual(n, 0)
      assert.strictEqual(await c.exists(k('dest')), 0)
    })
  })

  // ----------------------------------------------------------- ZUNION / ZINTER

  test('ZUNION returns the union, with and without WITHSCORES', async () => {
    await withOps(async (c, k) => {
      await c.zAdd(k('z1'), [
        { score: 1, value: 'a' },
        { score: 2, value: 'b' },
        { score: 3, value: 'c' },
      ])
      await c.zAdd(k('z2'), [
        { score: 10, value: 'b' },
        { score: 20, value: 'c' },
        { score: 30, value: 'd' },
      ])

      assert.deepStrictEqual(await c.zUnion([k('z1'), k('z2')]), [
        'a',
        'b',
        'c',
        'd',
      ])
      assert.deepStrictEqual(await c.zUnionWithScores([k('z1'), k('z2')]), [
        { value: 'a', score: 1 },
        { value: 'b', score: 12 },
        { value: 'c', score: 23 },
        { value: 'd', score: 30 },
      ])
    })
  })

  test('ZINTER returns the intersection WITHSCORES', async () => {
    await withOps(async (c, k) => {
      await c.zAdd(k('z1'), [
        { score: 1, value: 'a' },
        { score: 2, value: 'b' },
        { score: 3, value: 'c' },
      ])
      await c.zAdd(k('z2'), [
        { score: 10, value: 'b' },
        { score: 20, value: 'c' },
        { score: 30, value: 'd' },
      ])

      assert.deepStrictEqual(await c.zInterWithScores([k('z1'), k('z2')]), [
        { value: 'b', score: 12 },
        { value: 'c', score: 23 },
      ])
    })
  })

  // ----------------------------------------------------------- ZDIFF / STORE

  test('ZDIFF returns members of the first set not in the rest', async () => {
    await withOps(async (c, k) => {
      await c.zAdd(k('z1'), [
        { score: 1, value: 'a' },
        { score: 2, value: 'b' },
        { score: 3, value: 'c' },
      ])
      await c.zAdd(k('z2'), [
        { score: 10, value: 'b' },
        { score: 20, value: 'c' },
      ])

      assert.deepStrictEqual(await c.zDiff([k('z1'), k('z2')]), ['a'])
      assert.deepStrictEqual(await c.zDiffWithScores([k('z1'), k('z2')]), [
        { value: 'a', score: 1 },
      ])
    })
  })

  test('ZDIFFSTORE stores the difference and returns its cardinality', async () => {
    await withOps(async (c, k) => {
      await c.zAdd(k('z1'), [
        { score: 1, value: 'a' },
        { score: 2, value: 'b' },
        { score: 3, value: 'c' },
      ])
      await c.zAdd(k('z2'), [
        { score: 10, value: 'b' },
        { score: 20, value: 'c' },
      ])

      const n = await c.zDiffStore(k('dest'), [k('z1'), k('z2')])
      assert.strictEqual(n, 1)
      assert.deepStrictEqual(await c.zRangeWithScores(k('dest'), 0, -1), [
        { value: 'a', score: 1 },
      ])
    })
  })

  // ----------------------------------------------------------------- ZINTERCARD

  test('ZINTERCARD counts the intersection and honors LIMIT', async () => {
    await withOps(async (c, k) => {
      await c.zAdd(k('z1'), [
        { score: 1, value: 'a' },
        { score: 2, value: 'b' },
        { score: 3, value: 'c' },
      ])
      await c.zAdd(k('z2'), [
        { score: 10, value: 'b' },
        { score: 20, value: 'c' },
        { score: 30, value: 'd' },
      ])

      assert.strictEqual(await c.zInterCard([k('z1'), k('z2')]), 2)
      assert.strictEqual(
        await c.zInterCard([k('z1'), k('z2')], { LIMIT: 1 }),
        1,
      )
      // LIMIT 0 means no limit
      assert.strictEqual(
        await c.zInterCard([k('z1'), k('z2')], { LIMIT: 0 }),
        2,
      )
    })
  })

  // -------------------------------------------------------------- error paths

  test('ZUNIONSTORE rejects numkeys <= 0 with the input-key error', async () => {
    await withOps(async (c, k) => {
      await c.zAdd(k('z1'), { score: 1, value: 'a' })
      await assert.rejects(
        () => c.sendCommand(['ZUNIONSTORE', k('dest'), '0', k('z1')]),
        errorWithMessage(
          "ERR at least 1 input key is needed for 'zunionstore' command",
        ),
      )
      await assert.rejects(
        () => c.sendCommand(['ZUNIONSTORE', k('dest'), '-1', k('z1')]),
        errorWithMessage(
          "ERR at least 1 input key is needed for 'zunionstore' command",
        ),
      )
    })
  })

  test('ZUNION rejects numkeys <= 0 with the input-key error', async () => {
    await withOps(async (c, k) => {
      await c.zAdd(k('z1'), { score: 1, value: 'a' })
      await assert.rejects(
        () => c.sendCommand(['ZUNION', '0', k('z1')]),
        errorWithMessage(
          "ERR at least 1 input key is needed for 'zunion' command",
        ),
      )
    })
  })

  test('ZINTERCARD rejects numkeys <= 0 with the input-key error', async () => {
    await withOps(async (c, k) => {
      await c.zAdd(k('z1'), { score: 1, value: 'a' })
      await assert.rejects(
        () => c.sendCommand(['ZINTERCARD', '0', k('z1')]),
        errorWithMessage(
          "ERR at least 1 input key is needed for 'zintercard' command",
        ),
      )
    })
  })

  test('ZUNIONSTORE rejects a non-integer numkeys', async () => {
    await withOps(async (c, k) => {
      await c.zAdd(k('z1'), { score: 1, value: 'a' })
      await assert.rejects(
        () => c.sendCommand(['ZUNIONSTORE', k('dest'), 'abc', k('z1')]),
        errorWithMessage('ERR value is not an integer or out of range'),
      )
    })
  })

  test('ZUNIONSTORE rejects numkeys greater than available keys', async () => {
    await withOps(async (c, k) => {
      await c.zAdd(k('z1'), { score: 1, value: 'a' })
      await c.zAdd(k('z2'), { score: 1, value: 'b' })
      await assert.rejects(
        () => c.sendCommand(['ZUNIONSTORE', k('dest'), '3', k('z1'), k('z2')]),
        errorWithMessage('ERR syntax error'),
      )
    })
  })

  test('ZUNIONSTORE rejects a WEIGHTS count mismatch', async () => {
    await withOps(async (c, k) => {
      await c.zAdd(k('z1'), { score: 1, value: 'a' })
      await c.zAdd(k('z2'), { score: 1, value: 'b' })
      await assert.rejects(
        () =>
          c.sendCommand([
            'ZUNIONSTORE',
            k('dest'),
            '2',
            k('z1'),
            k('z2'),
            'WEIGHTS',
            '1',
          ]),
        errorWithMessage('ERR syntax error'),
      )
    })
  })

  test('ZUNIONSTORE rejects a non-float weight', async () => {
    await withOps(async (c, k) => {
      await c.zAdd(k('z1'), { score: 1, value: 'a' })
      await c.zAdd(k('z2'), { score: 1, value: 'b' })
      await assert.rejects(
        () =>
          c.sendCommand([
            'ZUNIONSTORE',
            k('dest'),
            '2',
            k('z1'),
            k('z2'),
            'WEIGHTS',
            'x',
            'y',
          ]),
        errorWithMessage('ERR weight value is not a float'),
      )
    })
  })

  test('ZUNIONSTORE rejects an invalid AGGREGATE value', async () => {
    await withOps(async (c, k) => {
      await c.zAdd(k('z1'), { score: 1, value: 'a' })
      await c.zAdd(k('z2'), { score: 1, value: 'b' })
      await assert.rejects(
        () =>
          c.sendCommand([
            'ZUNIONSTORE',
            k('dest'),
            '2',
            k('z1'),
            k('z2'),
            'AGGREGATE',
            'foo',
          ]),
        errorWithMessage('ERR syntax error'),
      )
    })
  })

  test('ZUNIONSTORE rejects WITHSCORES (store variants have no scores option)', async () => {
    await withOps(async (c, k) => {
      await c.zAdd(k('z1'), { score: 1, value: 'a' })
      await c.zAdd(k('z2'), { score: 1, value: 'b' })
      await assert.rejects(
        () =>
          c.sendCommand([
            'ZUNIONSTORE',
            k('dest'),
            '2',
            k('z1'),
            k('z2'),
            'WITHSCORES',
          ]),
        errorWithMessage('ERR syntax error'),
      )
    })
  })

  test('ZDIFF rejects WEIGHTS (diff has no weights/aggregate)', async () => {
    await withOps(async (c, k) => {
      await c.zAdd(k('z1'), { score: 1, value: 'a' })
      await c.zAdd(k('z2'), { score: 1, value: 'b' })
      await assert.rejects(
        () =>
          c.sendCommand(['ZDIFF', '2', k('z1'), k('z2'), 'WEIGHTS', '1', '2']),
        errorWithMessage('ERR syntax error'),
      )
    })
  })

  test('ZINTERCARD rejects a negative LIMIT', async () => {
    await withOps(async (c, k) => {
      await c.zAdd(k('z1'), { score: 1, value: 'a' })
      await c.zAdd(k('z2'), { score: 1, value: 'b' })
      await assert.rejects(
        () => c.zInterCard([k('z1'), k('z2')], { LIMIT: -1 }),
        errorWithMessage("ERR LIMIT can't be negative"),
      )
    })
  })

  test('ZUNION propagates WRONGTYPE for a non-zset/non-set source', async () => {
    await withOps(async (c, k) => {
      await c.zAdd(k('z1'), { score: 1, value: 'a' })
      await c.set(k('str'), 'hello')
      await assert.rejects(
        () => c.zUnion([k('z1'), k('str')]),
        errorWithMessage(
          'WRONGTYPE Operation against a key holding the wrong kind of value',
        ),
      )
    })
  })

  test('ZUNIONSTORE rejects wrong arity', async () => {
    await withOps(async (c, k) => {
      await assert.rejects(
        () => c.sendCommand(['ZUNIONSTORE', k('dest')]),
        errorWithMessage(
          "ERR wrong number of arguments for 'zunionstore' command",
        ),
      )
    })
  })

  test('ZUNION rejects wrong arity', async () => {
    await withOps(async c => {
      await assert.rejects(
        () => c.sendCommand(['ZUNION']),
        errorWithMessage("ERR wrong number of arguments for 'zunion' command"),
      )
    })
  })
})
