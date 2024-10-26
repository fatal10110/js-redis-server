import Redis, { Cluster } from 'ioredis'
import { after, before, describe, it } from 'node:test'
import { ClusterNetwork } from '../../src/core/cluster/network'
import assert from 'node:assert'

describe('multi', () => {
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

  it('Queue commands before execution without piplining', async () => {
    const anotherRedisClient = new Redis.Cluster(
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

    await anotherRedisClient.connect()

    try {
      const multi = redisClient!.multi()
      multi.set('myKey', 'myValue')
      const anotherRes = await anotherRedisClient.get('myKey')
      multi.get('myKey')

      const res = await multi.exec()

      assert.notEqual(res, null)

      const [[, first], [, second]] = res

      assert.strictEqual(first, 'OK')
      assert.strictEqual(second, 'myValue')
      assert.strictEqual(anotherRes, null)
    } finally {
      await anotherRedisClient.quit()
    }
  })
})
