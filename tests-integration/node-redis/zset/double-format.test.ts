import { test, describe, before, after } from 'node:test'
import assert from 'node:assert'
import { RedisClusterType } from 'redis'
import { TestRunner } from '../../test-config'
import { randomKey } from '../../utils'

// Twin of tests-integration/ioredis/zset/double-format.test.ts (#451).
//
// node-redis talks RESP3 and parses every `,` double into a JS number, so the
// server's spelling is mostly invisible — except that its parser accumulates
// digits in floating point, so the number it hands back depends on the text.
// Real Redis 8.0 sends `,1.23e-5` for 0.0000123, which node-redis reads as
// 0.000012299999999999999; the old mock sent `,0.0000123` and node-redis read
// 0.0000123. The text itself is asserted through a script, whose
// `redis.call` result comes back to the client as a bulk string.
const testRunner = new TestRunner()

// Real Redis spells 0.0000123 `1.23e-5`; this is what node-redis parses it to.
const SMALL_AS_PARSED = 0.000012299999999999999

describe(`Double reply formatting (node-redis, ${testRunner.getBackendName()})`, () => {
  let redis: RedisClusterType

  before(async () => {
    redis = (await testRunner.setupNodeRedisCluster()) as RedisClusterType
  })

  after(async () => {
    await testRunner.cleanup()
  })

  test('ZSCORE spells a double like d2string', async () => {
    const key = `{dbl:${randomKey()}}:z`
    await redis.zAdd(key, [
      { score: 0.0000123, value: 'small' },
      { score: 2 ** 62, value: 'twoTo62' },
      { score: 1e20, value: 'big' },
      { score: 0.1, value: 'tenth' },
      { score: 4.8911660955712037e-5, value: 'grisu' },
    ])

    assert.strictEqual(await redis.zScore(key, 'small'), SMALL_AS_PARSED)
    assert.strictEqual(await redis.zScore(key, 'big'), 1e20)
    assert.strictEqual(await redis.zScore(key, 'tenth'), 0.1)
    assert.deepStrictEqual(await redis.zmScore(key, ['small', 'big']), [
      SMALL_AS_PARSED,
      1e20,
    ])
  })

  test('ZINCRBY replies with the same spelling', async () => {
    const key = `{dbl:${randomKey()}}:z`
    assert.strictEqual(await redis.zIncrBy(key, 1e20, 'm'), 1e20)
    assert.strictEqual(
      await redis.zIncrBy(key, 0.0000123, 'o'),
      SMALL_AS_PARSED,
    )
  })

  test('WITHSCORES replies use the same spelling', async () => {
    const key = `{dbl:${randomKey()}}:z`
    await redis.zAdd(key, [
      { score: 0.0000123, value: 'a' },
      { score: 1e20, value: 'b' },
    ])

    assert.deepStrictEqual(await redis.zRangeWithScores(key, 0, -1), [
      { value: 'a', score: SMALL_AS_PARSED },
      { value: 'b', score: 1e20 },
    ])
  })

  test('a score read through redis.call gets the same spelling', async () => {
    const key = `{dbl:${randomKey()}}:z`
    await redis.zAdd(key, [
      { score: 0.0000123, value: 'small' },
      { score: 2 ** 62, value: 'twoTo62' },
      { score: 1e20, value: 'big' },
      { score: 0.1, value: 'tenth' },
      { score: 4.8911660955712037e-5, value: 'grisu' },
    ])

    const zscore = (member: string) =>
      redis.eval("return redis.call('ZSCORE', KEYS[1], ARGV[1])", {
        keys: [key],
        arguments: [member],
      })
    assert.strictEqual(await zscore('small'), '1.23e-5')
    assert.strictEqual(await zscore('twoTo62'), '4611686018427387904')
    assert.strictEqual(await zscore('big'), '1e+20')
    assert.strictEqual(await zscore('tenth'), '0.1')
    // Grisu2 picks a longer digit string than the shortest round-trip here.
    assert.strictEqual(await zscore('grisu'), '4.8911660955712037e-5')
  })
})
