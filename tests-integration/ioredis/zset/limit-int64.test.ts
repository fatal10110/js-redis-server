import { test, describe, before, after } from 'node:test'
import assert from 'node:assert'
import { Cluster } from 'ioredis'
import { TestRunner } from '../../test-config'
import { errorWithMessage, randomKey } from '../../utils'

/**
 * ZRANGEBYSCORE / ZRANGEBYLEX / ZRANGE ... LIMIT take the full int64 range
 * (`getLongFromObjectOrReply`, #576): a negative offset is an empty range, a
 * negative count the rest of it. Same on redis-server 6.2.14 .. 8.0.0 and
 * valkey-server 8.0.0 / 9.0.0.
 */
const testRunner = new TestRunner()
const NOT_INTEGER = 'ERR value is not an integer or out of range'
const INT64_MAX = '9223372036854775807'
const INT64_MIN = '-9223372036854775808'

describe(`Sorted set int64 LIMIT (${testRunner.getBackendName()})`, () => {
  let redisClient: Cluster | undefined

  before(async () => {
    redisClient = await testRunner.setupIoredisCluster('zset-limit-int64')
  })

  after(async () => {
    await testRunner.cleanup()
  })

  async function withZset(
    fn: (client: Cluster, key: string, tag: string) => Promise<void>,
  ): Promise<void> {
    assert.ok(redisClient)
    const tag = `{zlimit:${randomKey()}}`
    const key = `${tag}:z`
    await redisClient.zadd(key, 1, 'a', 2, 'b')
    try {
      await fn(redisClient, key, tag)
    } finally {
      await redisClient.del(key, `${tag}:dst`)
    }
  }

  test('ZRANGEBYSCORE and ZREVRANGEBYSCORE LIMIT take int64 bounds', async () => {
    await withZset(async (c, key) => {
      assert.deepStrictEqual(
        await c.zrangebyscore(key, '-inf', '+inf', 'LIMIT', '0', INT64_MAX),
        ['a', 'b'],
      )
      assert.deepStrictEqual(
        await c.zrangebyscore(key, '-inf', '+inf', 'LIMIT', INT64_MIN, '1'),
        [],
      )
      assert.deepStrictEqual(
        await c.zrangebyscore(key, '-inf', '+inf', 'LIMIT', INT64_MAX, '5'),
        [],
      )
      assert.deepStrictEqual(
        await c.zrangebyscore(key, '-inf', '+inf', 'LIMIT', '1', INT64_MIN),
        ['b'],
      )
      assert.deepStrictEqual(
        await c.zrangebyscore(
          key,
          '-inf',
          '+inf',
          'LIMIT',
          '0',
          '9007199254740993',
        ),
        ['a', 'b'],
      )
      assert.deepStrictEqual(
        await c.zrangebyscore(
          key,
          '-inf',
          '+inf',
          'WITHSCORES',
          'LIMIT',
          '0',
          INT64_MAX,
        ),
        ['a', '1', 'b', '2'],
      )
      assert.deepStrictEqual(
        await c.zrevrangebyscore(key, '+inf', '-inf', 'LIMIT', '0', INT64_MAX),
        ['b', 'a'],
      )
    })
  })

  test('ZRANGEBYLEX and ZREVRANGEBYLEX LIMIT take int64 bounds', async () => {
    await withZset(async (c, key) => {
      assert.deepStrictEqual(
        await c.zrangebylex(key, '-', '+', 'LIMIT', '0', INT64_MAX),
        ['a', 'b'],
      )
      assert.deepStrictEqual(
        await c.zrangebylex(key, '-', '+', 'LIMIT', INT64_MIN, '1'),
        [],
      )
      assert.deepStrictEqual(
        await c.zrevrangebylex(key, '+', '-', 'LIMIT', '0', INT64_MAX),
        ['b', 'a'],
      )
    })
  })

  test('ZRANGE BYSCORE|BYLEX and ZRANGESTORE LIMIT take int64 bounds', async () => {
    await withZset(async (c, key, tag) => {
      assert.deepStrictEqual(
        await c.zrange(key, '-inf', '+inf', 'BYSCORE', 'LIMIT', 0, INT64_MAX),
        ['a', 'b'],
      )
      assert.deepStrictEqual(
        await c.zrange(key, '-', '+', 'BYLEX', 'LIMIT', 0, INT64_MAX),
        ['a', 'b'],
      )
      assert.deepStrictEqual(
        await c.zrange(
          key,
          '+inf',
          '-inf',
          'BYSCORE',
          'REV',
          'LIMIT',
          0,
          INT64_MAX,
        ),
        ['b', 'a'],
      )
      assert.strictEqual(
        await c.zrangestore(
          `${tag}:dst`,
          key,
          '-inf',
          '+inf',
          'BYSCORE',
          'LIMIT',
          0,
          INT64_MAX,
        ),
        2,
      )
      assert.strictEqual(
        await c.zrangestore(
          `${tag}:dst`,
          key,
          '-',
          '+',
          'BYLEX',
          'LIMIT',
          INT64_MIN,
          INT64_MAX,
        ),
        0,
      )
    })
  })

  test('LIMIT refuses values outside int64 and non-canonical integers', async () => {
    await withZset(async (c, key) => {
      for (const [offset, count] of [
        ['0', '9223372036854775808'],
        ['-9223372036854775809', '1'],
        ['00', '01'],
        ['1e2', '1'],
      ]) {
        await assert.rejects(
          c.zrangebyscore(key, '-inf', '+inf', 'LIMIT', offset!, count!),
          errorWithMessage(NOT_INTEGER),
        )
        await assert.rejects(
          c.zrangebylex(key, '-', '+', 'LIMIT', offset!, count!),
          errorWithMessage(NOT_INTEGER),
        )
      }
    })
  })
})
