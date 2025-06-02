import { after, before, describe, it } from 'node:test'
import { ClusterNetwork } from '../../../src/core/cluster/network'
import assert from 'node:assert'
import { RedisClusterType, createCluster } from 'redis'

describe.skip('multi', () => {
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

  it('Queue commands before execution without piplining', async () => {
    const anotherRedisClient = createCluster({
      rootNodes: redisCluster.getAll().map(n => {
        return {
          url: `redis://127.0.0.1:${n.port}`,
        }
      }),
    })

    await anotherRedisClient.connect()

    try {
      const multi = redisClient!.multi()
      multi.set('myKey', 'myValue')
      const anotherRes = await anotherRedisClient.get('myKey')
      multi.get('myKey')

      const res = await multi.exec()

      assert.notEqual(res, null)

      const [first, second] = res

      assert.strictEqual(first, 'OK')
      assert.strictEqual(second, 'myValue')
      assert.strictEqual(anotherRes, null)
    } finally {
      await anotherRedisClient.close()
    }
  })

  it('handle errors in multi', async () => {
    const multi = redisClient!.multi()
    multi.set('myKey', 'myValue')
    multi.evalSha('abc')

    let error

    try {
      await multi.exec()
    } catch (err) {
      error = err
    }

    assert.ok(error)
    assert.ok(error.message.includes('NOSCRIPT'))
  })
})
