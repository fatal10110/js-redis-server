import { after, before, describe, it } from 'node:test'
import { ClusterNetwork } from '../../../src/core/cluster/network'
import { RedisClusterType, createCluster } from 'redis'
import { randomKey } from '../../utils'
import assert from 'node:assert'

describe('Redis commands', () => {
  const redisCluster = new ClusterNetwork(console)
  let redisClient: RedisClusterType | undefined

  before(async () => {
    await redisCluster.init({ masters: 3, slaves: 0 })
    redisClient = await createCluster({
      rootNodes: redisCluster.getAll().map(n => {
        return {
          url: `redis://127.0.0.1:${n.port}`,
        }
      }),
    })
    await redisClient?.connect()
  })

  after(async () => {
    await redisClient?.close()
    await redisCluster.shutdown()
  })

  describe('set', () => {
    it('sets data on key and gets value', async () => {
      const key = randomKey()
      await redisClient?.set(key, 1)

      const val = await redisClient?.get(key)
      assert.strictEqual(val, '1')
    })
  })

  describe('mget', () => {
    it('returns multiple key values', async () => {
      const key1 = randomKey()
      const key2 = `another:{${key1}}`
      await redisClient?.set(key1, 1)
      await redisClient?.set(key2, 2)

      const val = await redisClient?.mGet([key1, key2])
      assert.strictEqual(val?.length, 2)
      assert.strictEqual(val[0], '1')
      assert.strictEqual(val[1], '2')
    })

    it('cross slot error', async () => {
      const key1 = randomKey()
      const key2 = randomKey()
      await redisClient?.set(key1, 1)
      await redisClient?.set(key2, 2)

      const [res] = await Promise.allSettled([redisClient?.mGet([key1, key2])])

      assert.strictEqual(res.status, 'rejected')
      assert.strictEqual(
        res.reason.message,
        "CROSSSLOT Keys in request don't hash to the same slot",
      )
    })
  })
})
