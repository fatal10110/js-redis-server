import { test, describe, before, after } from 'node:test'
import assert from 'node:assert'
import { RedisClusterType } from 'redis'
import { TestRunner } from '../../test-config'
import { flushNodeRedisCluster, randomKey } from '../../utils'

const testRunner = new TestRunner()

// Issue #41: equal-score tie-breaking in the general sorted-set comparator must
// use raw byte order (memcmp), not String.prototype.localeCompare. With locale
// collation a non-ASCII member like 'é' (0xc3 0xa9) sorts *before* 'z' (0x7a);
// real Redis sorts by raw bytes, so 'z' comes first and 'é' last.
describe(`Sorted Set Byte-Order Tie-Breaking Integration (node-redis, ${testRunner.getBackendName()})`, () => {
  let redisClient: RedisClusterType

  before(async () => {
    redisClient = (await testRunner.setupNodeRedisCluster()) as RedisClusterType
    await flushNodeRedisCluster(redisClient)
  })

  after(async () => {
    await testRunner.cleanup()
  })

  async function seedEqualScore(members: string[]): Promise<string> {
    const key = `{bo:${randomKey()}}`
    await redisClient.zAdd(
      key,
      members.map(value => ({ score: 0, value })),
    )
    return key
  }

  test('ZRANGE breaks equal-score ties by raw byte order, not locale', async () => {
    const key = await seedEqualScore(['z', 'é', 'A', 'a'])
    try {
      // raw byte order: 'A'(0x41) 'a'(0x61) 'z'(0x7a) 'é'(0xc3...)
      assert.deepStrictEqual(await redisClient.zRange(key, 0, -1), [
        'A',
        'a',
        'z',
        'é',
      ])
    } finally {
      await redisClient.del(key)
    }
  })

  test('ZRANGEBYSCORE breaks equal-score ties by raw byte order', async () => {
    const key = await seedEqualScore(['z', 'é', 'A', 'a'])
    try {
      assert.deepStrictEqual(
        await redisClient.zRangeByScore(key, '-inf', '+inf'),
        ['A', 'a', 'z', 'é'],
      )
    } finally {
      await redisClient.del(key)
    }
  })

  test('ZRANK reflects raw byte order for equal scores', async () => {
    const key = await seedEqualScore(['z', 'é', 'A', 'a'])
    try {
      assert.strictEqual(await redisClient.zRank(key, 'A'), 0)
      assert.strictEqual(await redisClient.zRank(key, 'a'), 1)
      assert.strictEqual(await redisClient.zRank(key, 'z'), 2)
      assert.strictEqual(await redisClient.zRank(key, 'é'), 3)
      // ZREVRANK is the mirror image
      assert.strictEqual(await redisClient.zRevRank(key, 'é'), 0)
      assert.strictEqual(await redisClient.zRevRank(key, 'A'), 3)
    } finally {
      await redisClient.del(key)
    }
  })
})
