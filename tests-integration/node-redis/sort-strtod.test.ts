import { test, describe, before, after } from 'node:test'
import assert from 'node:assert'
import { RedisClientType, RedisClusterType } from 'redis'
import { TestRunner } from '../test-config'
import {
  activeProfile,
  connectToNodeRedisSlotOwner,
  errorWithMessage,
  randomKey,
} from '../utils'

/**
 * SORT's strtod-like score conversion and int64 LIMIT (#513), through the
 * typed client. Expectations follow the profile (redis-8.0 by default):
 * fast_float from Redis 8.0 / Valkey 8.1 refuses hex floats; Valkey 8.1+
 * refuses an empty element; only Redis 8.0 takes `1e400` (as infinity).
 * Checked against redis-server 6.2.14 .. 8.0.0 and valkey-server 8.0.0 /
 * 9.0.0. The range-error row is last: older servers keep a stale ERANGE.
 */
const testRunner = new TestRunner()
const fastFloat =
  activeProfile === 'redis-8.0' || activeProfile === 'valkey-9.0'
const NOT_DOUBLE = "ERR One or more scores can't be converted into double"
const NOT_INTEGER = 'ERR value is not an integer or out of range'
const INT64_MAX = '9223372036854775807'
const INT64_MIN = '-9223372036854775808'

describe(`SORT strtod scores and int64 LIMIT (node-redis, ${testRunner.getBackendName()})`, () => {
  let redisClient: RedisClusterType

  before(async () => {
    redisClient = (await testRunner.setupNodeRedisCluster()) as RedisClusterType
  })

  after(async () => {
    await testRunner.cleanup()
  })

  async function withOps(
    fn: (client: RedisClientType, k: (name: string) => string) => Promise<void>,
  ): Promise<void> {
    assert.ok(redisClient)
    const tag = `{sort-strtod:${randomKey()}}`
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

  test('SORT orders every infinity spelling as +/-inf', async () => {
    await withOps(async (c, k) => {
      await c.rPush(k('l'), ['inf', '2', '-Infinity', '+inf', '-5', 'infinity'])
      assert.deepStrictEqual(await c.sort(k('l')), [
        '-Infinity',
        '-5',
        '2',
        '+inf',
        'inf',
        'infinity',
      ])
    })
  })

  test('SORT treats an empty element as 0, except on Valkey 8.1+', async () => {
    await withOps(async (c, k) => {
      await c.rPush(k('l'), ['', '2', '-5'])
      if (activeProfile === 'valkey-9.0') {
        await assert.rejects(c.sort(k('l')), errorWithMessage(NOT_DOUBLE))
      } else {
        assert.deepStrictEqual(await c.sort(k('l')), ['-5', '', '2'])
      }
    })
  })

  test('SORT refuses trailing whitespace and NaN', async () => {
    await withOps(async (c, k) => {
      for (const [i, element] of ['1 ', 'nan'].entries()) {
        await c.rPush(k(`l${i}`), [element, '2'])
        await assert.rejects(c.sort(k(`l${i}`)), errorWithMessage(NOT_DOUBLE))
      }
    })
  })

  test('SORT reads hex floats only before Redis 8.0 / Valkey 8.1', async () => {
    await withOps(async (c, k) => {
      await c.rPush(k('l'), ['0x10', '2', '-5'])
      await c.set(k('w_a'), '0x1p3')
      await c.rPush(k('b'), ['a', 'x'])
      if (fastFloat) {
        await assert.rejects(c.sort(k('l')), errorWithMessage(NOT_DOUBLE))
        await assert.rejects(
          c.sort(k('b'), { BY: k('w_*') }),
          errorWithMessage(NOT_DOUBLE),
        )
      } else {
        assert.deepStrictEqual(await c.sort(k('l')), ['-5', '2', '0x10'])
        assert.deepStrictEqual(await c.sort(k('b'), { BY: k('w_*') }), [
          'x',
          'a',
        ])
      }
    })
  })

  // node-redis' typed LIMIT takes JS numbers, which cannot hold int64 bounds.
  test('SORT LIMIT takes the full int64 range', async () => {
    await withOps(async (c, k) => {
      await c.rPush(k('l'), ['3', '1', '2'])
      assert.deepStrictEqual(
        await c.sendCommand(['SORT', k('l'), 'LIMIT', '0', INT64_MAX]),
        ['1', '2', '3'],
      )
      assert.deepStrictEqual(
        await c.sendCommand(['SORT', k('l'), 'LIMIT', INT64_MIN, '1']),
        ['1'],
      )
      assert.deepStrictEqual(
        await c.sendCommand(['SORT', k('l'), 'LIMIT', '1', INT64_MAX]),
        ['2', '3'],
      )
      assert.deepStrictEqual(
        await c.sendCommand(['SORT', k('l'), 'LIMIT', INT64_MAX, '1']),
        [],
      )
      await assert.rejects(
        c.sendCommand(['SORT', k('l'), 'LIMIT', '0', '9223372036854775808']),
        errorWithMessage(NOT_INTEGER),
      )
    })
  })

  // Last: Redis <= 7.4 and Valkey 8.0 keep errno at ERANGE afterwards.
  test('SORT takes 1e400 as infinity only on Redis 8.0', async () => {
    await withOps(async (c, k) => {
      await c.rPush(k('l'), ['1e400', '2', '-5'])
      if (activeProfile === 'redis-8.0') {
        assert.deepStrictEqual(await c.sort(k('l')), ['-5', '2', '1e400'])
      } else {
        await assert.rejects(c.sort(k('l')), errorWithMessage(NOT_DOUBLE))
      }
    })
  })
})
