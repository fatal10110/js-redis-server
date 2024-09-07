import { after, before, describe, it } from 'node:test'
import { ClusterNetwork } from '../../src/core/cluster/network'
import Redis from 'ioredis'
import { randomKey } from '../utils'
import assert from 'node:assert'

describe('Redis commands', () => {
  const redisCluster = new ClusterNetwork(console)
  const redisClient = new Redis.Cluster([{ host: '127.0.0.1', port: 8010 }], {
    lazyConnect: true,
  })

  before(async () => {
    await redisCluster.init({ masters: 3, slaves: 2 })
    await redisClient.connect()
  })

  after(async () => {
    await redisClient.quit()
    await redisCluster.shutdown()
  })

  describe('set', () => {
    it('sets data on key and gets value', async () => {
      const key = randomKey()
      await redisClient.set(key, 1)

      const val = await redisClient.get(key)
      assert.strictEqual(val, '1')
    })
  })
})
