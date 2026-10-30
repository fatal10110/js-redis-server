import { test, describe, before, after } from 'node:test'
import assert from 'node:assert'
import { RedisClusterType } from 'redis'
import { TestRunner } from '../../test-config'
import { errorWithMessage, randomKey } from '../../utils'

/**
 * ZRANGEBYSCORE / ZRANGEBYLEX / ZRANGE ... LIMIT take the full int64 range
 * (`getLongFromObjectOrReply`, #576): a negative offset is an empty range, a
 * negative count the rest of it. Same on redis-server 6.2.14 .. 8.0.0 and
 * valkey-server 8.0.0 / 9.0.0. node-redis' typed LIMIT takes JS numbers,
 * which cannot hold int64 bounds, so those go through sendCommand.
 */
const testRunner = new TestRunner()
const NOT_INTEGER = 'ERR value is not an integer or out of range'
const INT64_MAX = '9223372036854775807'
const INT64_MIN = '-9223372036854775808'

describe(`Sorted set int64 LIMIT (node-redis, ${testRunner.getBackendName()})`, () => {
  let redisClient: RedisClusterType

  before(async () => {
    redisClient = (await testRunner.setupNodeRedisCluster()) as RedisClusterType
  })

  after(async () => {
    await testRunner.cleanup()
  })

  type Send = (args: string[]) => Promise<unknown>

  async function withZset(
    fn: (send: Send, key: string, tag: string) => Promise<void>,
  ): Promise<void> {
    const tag = `{zlimit:${randomKey()}}`
    const key = `${tag}:z`
    const send: Send = args => redisClient.sendCommand(key, false, args)
    await redisClient.zAdd(key, [
      { score: 1, value: 'a' },
      { score: 2, value: 'b' },
    ])
    try {
      await fn(send, key, tag)
    } finally {
      await redisClient.del(key)
      await redisClient.del(`${tag}:dst`)
    }
  }

  test('ZRANGEBYSCORE and ZREVRANGEBYSCORE LIMIT take int64 bounds', async () => {
    await withZset(async (send, key) => {
      const byScore = (offset: string, count: string) =>
        send(['ZRANGEBYSCORE', key, '-inf', '+inf', 'LIMIT', offset, count])
      assert.deepStrictEqual(await byScore('0', INT64_MAX), ['a', 'b'])
      assert.deepStrictEqual(await byScore(INT64_MIN, '1'), [])
      assert.deepStrictEqual(await byScore(INT64_MAX, '5'), [])
      assert.deepStrictEqual(await byScore('1', INT64_MIN), ['b'])
      assert.deepStrictEqual(await byScore('0', '9007199254740993'), ['a', 'b'])
      assert.deepStrictEqual(
        await send([
          'ZREVRANGEBYSCORE',
          key,
          '+inf',
          '-inf',
          'LIMIT',
          '0',
          INT64_MAX,
        ]),
        ['b', 'a'],
      )
      // A count beyond the members through the typed method still works.
      assert.deepStrictEqual(
        await redisClient.zRangeByScore(key, '-inf', '+inf', {
          LIMIT: { offset: 0, count: Number.MAX_SAFE_INTEGER },
        }),
        ['a', 'b'],
      )
    })
  })

  test('ZRANGEBYLEX and ZREVRANGEBYLEX LIMIT take int64 bounds', async () => {
    await withZset(async (send, key) => {
      assert.deepStrictEqual(
        await send(['ZRANGEBYLEX', key, '-', '+', 'LIMIT', '0', INT64_MAX]),
        ['a', 'b'],
      )
      assert.deepStrictEqual(
        await send(['ZRANGEBYLEX', key, '-', '+', 'LIMIT', INT64_MIN, '1']),
        [],
      )
      assert.deepStrictEqual(
        await send(['ZREVRANGEBYLEX', key, '+', '-', 'LIMIT', '0', INT64_MAX]),
        ['b', 'a'],
      )
    })
  })

  test('ZRANGE BYSCORE|BYLEX and ZRANGESTORE LIMIT take int64 bounds', async () => {
    await withZset(async (send, key, tag) => {
      assert.deepStrictEqual(
        await send([
          'ZRANGE',
          key,
          '-inf',
          '+inf',
          'BYSCORE',
          'LIMIT',
          '0',
          INT64_MAX,
        ]),
        ['a', 'b'],
      )
      assert.deepStrictEqual(
        await send(['ZRANGE', key, '-', '+', 'BYLEX', 'LIMIT', '0', INT64_MAX]),
        ['a', 'b'],
      )
      assert.deepStrictEqual(
        await send([
          'ZRANGE',
          key,
          '+inf',
          '-inf',
          'BYSCORE',
          'REV',
          'LIMIT',
          '0',
          INT64_MAX,
        ]),
        ['b', 'a'],
      )
      assert.strictEqual(
        await send([
          'ZRANGESTORE',
          `${tag}:dst`,
          key,
          '-inf',
          '+inf',
          'BYSCORE',
          'LIMIT',
          '0',
          INT64_MAX,
        ]),
        2,
      )
      assert.strictEqual(
        await send([
          'ZRANGESTORE',
          `${tag}:dst`,
          key,
          '-',
          '+',
          'BYLEX',
          'LIMIT',
          INT64_MIN,
          INT64_MAX,
        ]),
        0,
      )
    })
  })

  test('LIMIT refuses values outside int64 and non-canonical integers', async () => {
    await withZset(async (send, key) => {
      for (const [offset, count] of [
        ['0', '9223372036854775808'],
        ['-9223372036854775809', '1'],
        ['00', '01'],
        ['1e2', '1'],
      ]) {
        await assert.rejects(
          send([
            'ZRANGEBYSCORE',
            key,
            '-inf',
            '+inf',
            'LIMIT',
            offset!,
            count!,
          ]),
          errorWithMessage(NOT_INTEGER),
        )
        await assert.rejects(
          send(['ZRANGEBYLEX', key, '-', '+', 'LIMIT', offset!, count!]),
          errorWithMessage(NOT_INTEGER),
        )
      }
    })
  })
})
