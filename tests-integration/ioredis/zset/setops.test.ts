import { test, describe, before, after } from 'node:test'
import assert from 'node:assert'
import { Cluster, Redis } from 'ioredis'
import { TestRunner } from '../../test-config'
import { connectToSlotOwner, errorWithMessage, randomKey } from '../../utils'

const testRunner = new TestRunner()

describe(`Sorted Set Set-Operations (${testRunner.getBackendName()})`, () => {
  let redisClient: Cluster | undefined

  before(async () => {
    redisClient = await testRunner.setupIoredisCluster(
      'zset-setops-integration',
    )
  })

  after(async () => {
    await testRunner.cleanup()
  })

  // All keys in a single op must hash to the same slot, so every test shares a
  // hash tag and talks to that slot's owner through a prefix-free directClient.
  async function withOps(
    fn: (client: Redis, k: (name: string) => string) => Promise<void>,
  ): Promise<void> {
    assert.ok(redisClient)
    const tag = `{zsetops:${randomKey()}}`
    const k = (name: string) => `${tag}:${name}`
    const directClient = await connectToSlotOwner(redisClient, k('seed'))
    try {
      await fn(directClient, k)
    } finally {
      directClient.disconnect()
    }
  }

  // ---------------------------------------------------------------- ZUNIONSTORE

  test('ZUNIONSTORE sums scores by default and stores the result', async () => {
    await withOps(async (c, k) => {
      await c.zadd(k('z1'), 1, 'a', 2, 'b', 3, 'c')
      await c.zadd(k('z2'), 10, 'b', 20, 'c', 30, 'd')

      const n = await c.call('ZUNIONSTORE', k('dest'), '2', k('z1'), k('z2'))
      assert.strictEqual(n, 4)
      assert.deepStrictEqual(
        await c.call('ZRANGE', k('dest'), '0', '-1', 'WITHSCORES'),
        ['a', '1', 'b', '12', 'c', '23', 'd', '30'],
      )
    })
  })

  test('ZUNIONSTORE applies WEIGHTS before aggregating', async () => {
    await withOps(async (c, k) => {
      await c.zadd(k('z1'), 1, 'a', 2, 'b', 3, 'c')
      await c.zadd(k('z2'), 10, 'b', 20, 'c', 30, 'd')

      await c.call(
        'ZUNIONSTORE',
        k('dest'),
        '2',
        k('z1'),
        k('z2'),
        'WEIGHTS',
        '2',
        '3',
      )
      assert.deepStrictEqual(
        await c.call('ZRANGE', k('dest'), '0', '-1', 'WITHSCORES'),
        ['a', '2', 'b', '34', 'c', '66', 'd', '90'],
      )
    })
  })

  test('ZUNIONSTORE honors AGGREGATE MIN and MAX', async () => {
    await withOps(async (c, k) => {
      await c.zadd(k('z1'), 1, 'a', 2, 'b', 3, 'c')
      await c.zadd(k('z2'), 10, 'b', 20, 'c', 30, 'd')

      await c.call(
        'ZUNIONSTORE',
        k('dest'),
        '2',
        k('z1'),
        k('z2'),
        'AGGREGATE',
        'MIN',
      )
      assert.deepStrictEqual(
        await c.call('ZRANGE', k('dest'), '0', '-1', 'WITHSCORES'),
        ['a', '1', 'b', '2', 'c', '3', 'd', '30'],
      )

      await c.call(
        'ZUNIONSTORE',
        k('dest'),
        '2',
        k('z1'),
        k('z2'),
        'AGGREGATE',
        'MAX',
      )
      assert.deepStrictEqual(
        await c.call('ZRANGE', k('dest'), '0', '-1', 'WITHSCORES'),
        ['a', '1', 'b', '10', 'c', '20', 'd', '30'],
      )
    })
  })

  test('ZUNIONSTORE treats a plain set source as scores of 1', async () => {
    await withOps(async (c, k) => {
      await c.zadd(k('z1'), 1, 'a', 2, 'b', 3, 'c')
      await c.sadd(k('s1'), 'a', 'b', 'x')

      await c.call('ZUNIONSTORE', k('dest'), '2', k('z1'), k('s1'))
      assert.deepStrictEqual(
        await c.call('ZRANGE', k('dest'), '0', '-1', 'WITHSCORES'),
        ['x', '1', 'a', '2', 'b', '3', 'c', '3'],
      )
    })
  })

  test('ZUNIONSTORE resets a SUM that becomes NaN (inf + -inf) to 0', async () => {
    await withOps(async (c, k) => {
      await c.zadd(k('za'), '+inf', 'm')
      await c.zadd(k('zb'), '-inf', 'm')

      await c.call('ZUNIONSTORE', k('dest'), '2', k('za'), k('zb'))
      assert.strictEqual(await c.call('ZSCORE', k('dest'), 'm'), '0')
    })
  })

  // ---------------------------------------------------------------- ZINTERSTORE

  test('ZINTERSTORE keeps only common members, summing scores', async () => {
    await withOps(async (c, k) => {
      await c.zadd(k('z1'), 1, 'a', 2, 'b', 3, 'c')
      await c.zadd(k('z2'), 10, 'b', 20, 'c', 30, 'd')

      const n = await c.call('ZINTERSTORE', k('dest'), '2', k('z1'), k('z2'))
      assert.strictEqual(n, 2)
      assert.deepStrictEqual(
        await c.call('ZRANGE', k('dest'), '0', '-1', 'WITHSCORES'),
        ['b', '12', 'c', '23'],
      )
    })
  })

  test('ZINTERSTORE with empty result deletes the destination key', async () => {
    await withOps(async (c, k) => {
      await c.zadd(k('z1'), 1, 'a')
      await c.zadd(k('z2'), 1, 'b')
      await c.set(k('dest'), 'preexisting')

      const n = await c.call('ZINTERSTORE', k('dest'), '2', k('z1'), k('z2'))
      assert.strictEqual(n, 0)
      assert.strictEqual(await c.exists(k('dest')), 0)
    })
  })

  // ----------------------------------------------------------- ZUNION / ZINTER

  test('ZUNION returns the union, with and without WITHSCORES', async () => {
    await withOps(async (c, k) => {
      await c.zadd(k('z1'), 1, 'a', 2, 'b', 3, 'c')
      await c.zadd(k('z2'), 10, 'b', 20, 'c', 30, 'd')

      assert.deepStrictEqual(await c.call('ZUNION', '2', k('z1'), k('z2')), [
        'a',
        'b',
        'c',
        'd',
      ])
      assert.deepStrictEqual(
        await c.call('ZUNION', '2', k('z1'), k('z2'), 'WITHSCORES'),
        ['a', '1', 'b', '12', 'c', '23', 'd', '30'],
      )
    })
  })

  test('ZINTER returns the intersection WITHSCORES', async () => {
    await withOps(async (c, k) => {
      await c.zadd(k('z1'), 1, 'a', 2, 'b', 3, 'c')
      await c.zadd(k('z2'), 10, 'b', 20, 'c', 30, 'd')

      assert.deepStrictEqual(
        await c.call('ZINTER', '2', k('z1'), k('z2'), 'WITHSCORES'),
        ['b', '12', 'c', '23'],
      )
    })
  })

  // ----------------------------------------------------------- ZDIFF / STORE

  test('ZDIFF returns members of the first set not in the rest', async () => {
    await withOps(async (c, k) => {
      await c.zadd(k('z1'), 1, 'a', 2, 'b', 3, 'c')
      await c.zadd(k('z2'), 10, 'b', 20, 'c')

      assert.deepStrictEqual(await c.call('ZDIFF', '2', k('z1'), k('z2')), [
        'a',
      ])
      assert.deepStrictEqual(
        await c.call('ZDIFF', '2', k('z1'), k('z2'), 'WITHSCORES'),
        ['a', '1'],
      )
    })
  })

  test('ZDIFFSTORE stores the difference and returns its cardinality', async () => {
    await withOps(async (c, k) => {
      await c.zadd(k('z1'), 1, 'a', 2, 'b', 3, 'c')
      await c.zadd(k('z2'), 10, 'b', 20, 'c')

      const n = await c.call('ZDIFFSTORE', k('dest'), '2', k('z1'), k('z2'))
      assert.strictEqual(n, 1)
      assert.deepStrictEqual(
        await c.call('ZRANGE', k('dest'), '0', '-1', 'WITHSCORES'),
        ['a', '1'],
      )
    })
  })

  // ----------------------------------------------------------------- ZINTERCARD

  test('ZINTERCARD counts the intersection and honors LIMIT', async () => {
    await withOps(async (c, k) => {
      await c.zadd(k('z1'), 1, 'a', 2, 'b', 3, 'c')
      await c.zadd(k('z2'), 10, 'b', 20, 'c', 30, 'd')

      assert.strictEqual(await c.call('ZINTERCARD', '2', k('z1'), k('z2')), 2)
      assert.strictEqual(
        await c.call('ZINTERCARD', '2', k('z1'), k('z2'), 'LIMIT', '1'),
        1,
      )
      // LIMIT 0 means no limit
      assert.strictEqual(
        await c.call('ZINTERCARD', '2', k('z1'), k('z2'), 'LIMIT', '0'),
        2,
      )
    })
  })

  // -------------------------------------------------------------- error paths

  test('ZUNIONSTORE rejects numkeys <= 0 with the input-key error', async () => {
    await withOps(async (c, k) => {
      await c.zadd(k('z1'), 1, 'a')
      await assert.rejects(
        () => c.call('ZUNIONSTORE', k('dest'), '0', k('z1')),
        errorWithMessage(
          "ERR at least 1 input key is needed for 'zunionstore' command",
        ),
      )
      await assert.rejects(
        () => c.call('ZUNIONSTORE', k('dest'), '-1', k('z1')),
        errorWithMessage(
          "ERR at least 1 input key is needed for 'zunionstore' command",
        ),
      )
    })
  })

  test('ZUNION rejects numkeys <= 0 with the input-key error', async () => {
    await withOps(async (c, k) => {
      await c.zadd(k('z1'), 1, 'a')
      await assert.rejects(
        () => c.call('ZUNION', '0', k('z1')),
        errorWithMessage(
          "ERR at least 1 input key is needed for 'zunion' command",
        ),
      )
    })
  })

  test('ZINTERCARD rejects numkeys <= 0 with the input-key error', async () => {
    await withOps(async (c, k) => {
      await c.zadd(k('z1'), 1, 'a')
      await assert.rejects(
        () => c.call('ZINTERCARD', '0', k('z1')),
        errorWithMessage(
          "ERR at least 1 input key is needed for 'zintercard' command",
        ),
      )
    })
  })

  test('ZUNIONSTORE rejects a non-integer numkeys', async () => {
    await withOps(async (c, k) => {
      await c.zadd(k('z1'), 1, 'a')
      await assert.rejects(
        () => c.call('ZUNIONSTORE', k('dest'), 'abc', k('z1')),
        errorWithMessage('ERR value is not an integer or out of range'),
      )
    })
  })

  test('ZUNIONSTORE rejects numkeys greater than available keys', async () => {
    await withOps(async (c, k) => {
      await c.zadd(k('z1'), 1, 'a')
      await c.zadd(k('z2'), 1, 'b')
      await assert.rejects(
        () => c.call('ZUNIONSTORE', k('dest'), '3', k('z1'), k('z2')),
        errorWithMessage('ERR syntax error'),
      )
    })
  })

  test('ZUNIONSTORE rejects a WEIGHTS count mismatch', async () => {
    await withOps(async (c, k) => {
      await c.zadd(k('z1'), 1, 'a')
      await c.zadd(k('z2'), 1, 'b')
      await assert.rejects(
        () =>
          c.call(
            'ZUNIONSTORE',
            k('dest'),
            '2',
            k('z1'),
            k('z2'),
            'WEIGHTS',
            '1',
          ),
        errorWithMessage('ERR syntax error'),
      )
    })
  })

  test('ZUNIONSTORE rejects a non-float weight', async () => {
    await withOps(async (c, k) => {
      await c.zadd(k('z1'), 1, 'a')
      await c.zadd(k('z2'), 1, 'b')
      await assert.rejects(
        () =>
          c.call(
            'ZUNIONSTORE',
            k('dest'),
            '2',
            k('z1'),
            k('z2'),
            'WEIGHTS',
            'x',
            'y',
          ),
        errorWithMessage('ERR weight value is not a float'),
      )
    })
  })

  test('ZUNIONSTORE rejects an invalid AGGREGATE value', async () => {
    await withOps(async (c, k) => {
      await c.zadd(k('z1'), 1, 'a')
      await c.zadd(k('z2'), 1, 'b')
      await assert.rejects(
        () =>
          c.call(
            'ZUNIONSTORE',
            k('dest'),
            '2',
            k('z1'),
            k('z2'),
            'AGGREGATE',
            'foo',
          ),
        errorWithMessage('ERR syntax error'),
      )
    })
  })

  test('ZUNIONSTORE rejects WITHSCORES (store variants have no scores option)', async () => {
    await withOps(async (c, k) => {
      await c.zadd(k('z1'), 1, 'a')
      await c.zadd(k('z2'), 1, 'b')
      await assert.rejects(
        () =>
          c.call('ZUNIONSTORE', k('dest'), '2', k('z1'), k('z2'), 'WITHSCORES'),
        errorWithMessage('ERR syntax error'),
      )
    })
  })

  test('ZDIFF rejects WEIGHTS (diff has no weights/aggregate)', async () => {
    await withOps(async (c, k) => {
      await c.zadd(k('z1'), 1, 'a')
      await c.zadd(k('z2'), 1, 'b')
      await assert.rejects(
        () => c.call('ZDIFF', '2', k('z1'), k('z2'), 'WEIGHTS', '1', '2'),
        errorWithMessage('ERR syntax error'),
      )
    })
  })

  test('ZINTERCARD rejects a negative LIMIT', async () => {
    await withOps(async (c, k) => {
      await c.zadd(k('z1'), 1, 'a')
      await c.zadd(k('z2'), 1, 'b')
      await assert.rejects(
        () => c.call('ZINTERCARD', '2', k('z1'), k('z2'), 'LIMIT', '-1'),
        errorWithMessage("ERR LIMIT can't be negative"),
      )
    })
  })

  test('ZUNION propagates WRONGTYPE for a non-zset/non-set source', async () => {
    await withOps(async (c, k) => {
      await c.zadd(k('z1'), 1, 'a')
      await c.set(k('str'), 'hello')
      await assert.rejects(
        () => c.call('ZUNION', '2', k('z1'), k('str')),
        errorWithMessage(
          'WRONGTYPE Operation against a key holding the wrong kind of value',
        ),
      )
    })
  })

  test('ZUNIONSTORE rejects wrong arity', async () => {
    await withOps(async (c, k) => {
      await assert.rejects(
        () => c.call('ZUNIONSTORE', k('dest')),
        errorWithMessage(
          "ERR wrong number of arguments for 'zunionstore' command",
        ),
      )
    })
  })

  test('ZUNION rejects wrong arity', async () => {
    await withOps(async c => {
      await assert.rejects(
        () => c.call('ZUNION'),
        errorWithMessage("ERR wrong number of arguments for 'zunion' command"),
      )
    })
  })
})
