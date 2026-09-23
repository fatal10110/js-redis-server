import { test, describe, before, after } from 'node:test'
import assert from 'node:assert'
import { RedisClusterType } from 'redis'
import { TestRunner } from '../../test-config'
import { randomKey } from '../../utils'

// Twin of tests-integration/ioredis/zset/double-format.test.ts (#451).
//
// node-redis talks RESP3 and parses every `,` double into a JS number, so the
// server's spelling is invisible through its typed methods. Its parser also
// builds the number with floating-point arithmetic whose last bit differs
// between Node versions (`,1.23e-5` reads as 0.000012299999999999999 on Node
// 22 and 0.0000123 on Node 24), so typed scores are compared to within a
// couple of ulps. The spelling itself is asserted through a script, whose
// `redis.call` result comes back to the client as a bulk string.
const testRunner = new TestRunner()

function assertScore(actual: number | null | undefined, expected: number) {
  assert.ok(
    typeof actual === 'number' &&
      Math.abs(actual - expected) <= Math.abs(expected) * 2 ** -51,
    `expected ~${expected}, got ${actual}`,
  )
}

describe(`Double reply formatting (node-redis, ${testRunner.getBackendName()})`, () => {
  let redis: RedisClusterType

  before(async () => {
    redis = (await testRunner.setupNodeRedisCluster()) as RedisClusterType
  })

  after(async () => {
    await testRunner.cleanup()
  })

  test('ZSCORE / ZMSCORE parse back to the stored score', async () => {
    const key = `{dbl:${randomKey()}}:z`
    await redis.zAdd(key, [
      { score: 0.0000123, value: 'small' },
      { score: 1e20, value: 'big' },
      { score: 0.1, value: 'tenth' },
    ])

    assertScore(await redis.zScore(key, 'small'), 0.0000123)
    assert.strictEqual(await redis.zScore(key, 'big'), 1e20)
    assert.strictEqual(await redis.zScore(key, 'tenth'), 0.1)
    const [small, big] = await redis.zmScore(key, ['small', 'big'])
    assertScore(small, 0.0000123)
    assert.strictEqual(big, 1e20)
  })

  test('ZINCRBY parses back to the new score', async () => {
    const key = `{dbl:${randomKey()}}:z`
    assert.strictEqual(await redis.zIncrBy(key, 1e20, 'm'), 1e20)
    assertScore(await redis.zIncrBy(key, 0.0000123, 'o'), 0.0000123)
  })

  test('WITHSCORES parses back to the stored scores', async () => {
    const key = `{dbl:${randomKey()}}:z`
    await redis.zAdd(key, [
      { score: 0.0000123, value: 'a' },
      { score: 1e20, value: 'b' },
    ])

    const reply = await redis.zRangeWithScores(key, 0, -1)
    assert.deepStrictEqual(
      reply.map(entry => entry.value),
      ['a', 'b'],
    )
    assertScore(reply[0].score, 0.0000123)
    assert.strictEqual(reply[1].score, 1e20)
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
