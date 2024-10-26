import { after, before, describe, it } from 'node:test'
import { ClusterNetwork } from '../../src/core/cluster/network'
import { Redis, Cluster } from 'ioredis'
import { randomKey } from '../utils'
import assert from 'node:assert'

describe('Redis commands', () => {
  const redisCluster = new ClusterNetwork(console)
  let redisClient: Cluster | undefined

  before(async () => {
    await redisCluster.init({ masters: 3, slaves: 2 })
    redisClient = new Redis.Cluster(
      [
        {
          host: '127.0.0.1',
          port: Array.from(redisCluster.getAll())[0].getAddress().port,
        },
      ],
      {
        lazyConnect: true,
      },
    )
    await redisClient?.connect()
  })

  after(async () => {
    await redisClient?.quit()
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

      const val = await redisClient?.mget(key1, key2)
      assert.strictEqual(val?.length, 2)
      assert.strictEqual(val[0], '1')
      assert.strictEqual(val[1], '2')
    })

    it('cross slot error', async () => {
      const key1 = randomKey()
      const key2 = randomKey()
      await redisClient?.set(key1, 1)
      await redisClient?.set(key2, 2)

      const [res] = await Promise.allSettled([redisClient?.mget(key1, key2)])

      assert.strictEqual(res.status, 'rejected')
      assert.strictEqual(
        res.reason.message,
        "CROSSSLOT Keys in request don't hash to the same slot",
      )
    })
  })
})
