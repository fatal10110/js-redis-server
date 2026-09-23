import { test, describe, before, after } from 'node:test'
import assert from 'node:assert'
import { Cluster } from 'ioredis'
import { TestRunner } from '../../test-config'
import { randomKey } from '../../utils'

// The text of a score reply, the way the default profile (Redis 8.0) spells it:
// `d2string()` — integers within ±2^62 print every digit, everything else goes
// through `fpconv_dtoa` (Grisu2) with its own plain/exponent layout (#451).
// The 6.2 / 7.0 `%.17g` spelling is pinned in
// tests-integration/compatibility/double-format.test.ts and, value by value,
// in tests/core/double-format.test.ts.
const testRunner = new TestRunner()

describe(`Double reply formatting (${testRunner.getBackendName()})`, () => {
  let redis: Cluster

  before(async () => {
    redis = await testRunner.setupIoredisCluster('zset-double-format')
  })

  after(async () => {
    await testRunner.cleanup()
  })

  test('ZSCORE spells a double like d2string', async () => {
    const key = `{dbl:${randomKey()}}:z`
    await redis.zadd(
      key,
      '0.0000123',
      'small',
      '4611686018427387904',
      'twoTo62',
      '1e20',
      'big',
      '0.1',
      'tenth',
      '4.8911660955712037e-5',
      'grisu',
    )

    assert.strictEqual(await redis.zscore(key, 'small'), '1.23e-5')
    assert.strictEqual(
      await redis.zscore(key, 'twoTo62'),
      '4611686018427387904',
    )
    assert.strictEqual(await redis.zscore(key, 'big'), '1e+20')
    assert.strictEqual(await redis.zscore(key, 'tenth'), '0.1')
    // Grisu2 picks a longer digit string than the shortest round-trip here.
    assert.strictEqual(
      await redis.zscore(key, 'grisu'),
      '4.8911660955712037e-5',
    )
    assert.deepStrictEqual(await redis.zmscore(key, 'small', 'big'), [
      '1.23e-5',
      '1e+20',
    ])
  })

  test('ZINCRBY replies with the same spelling', async () => {
    const key = `{dbl:${randomKey()}}:z`
    assert.strictEqual(await redis.zincrby(key, '1e20', 'm'), '1e+20')
    assert.strictEqual(await redis.zincrby(key, '-1e20', 'n'), '-1e+20')
    assert.strictEqual(await redis.zincrby(key, '0.0000123', 'o'), '1.23e-5')
  })

  test('WITHSCORES replies use the same spelling', async () => {
    const key = `{dbl:${randomKey()}}:z`
    await redis.zadd(key, '0.0000123', 'a', '1e20', 'b')

    assert.deepStrictEqual(await redis.zrange(key, 0, -1, 'WITHSCORES'), [
      'a',
      '1.23e-5',
      'b',
      '1e+20',
    ])
    assert.deepStrictEqual(
      await redis.zrangebyscore(key, '-inf', '+inf', 'WITHSCORES'),
      ['a', '1.23e-5', 'b', '1e+20'],
    )
    assert.deepStrictEqual(await redis.zpopmax(key), ['b', '1e+20'])
  })

  test('a score read through redis.call gets the same spelling', async () => {
    const key = `{dbl:${randomKey()}}:z`
    await redis.zadd(key, '1e20', 'big', '0.0000123', 'small')

    assert.strictEqual(
      await redis.eval(
        "return redis.call('ZSCORE', KEYS[1], ARGV[1])",
        1,
        key,
        'big',
      ),
      '1e+20',
    )
    assert.strictEqual(
      await redis.eval(
        "return redis.call('ZSCORE', KEYS[1], ARGV[1])",
        1,
        key,
        'small',
      ),
      '1.23e-5',
    )
  })
})
