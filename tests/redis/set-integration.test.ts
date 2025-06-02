import { test, describe, before, after } from 'node:test'
import assert from 'node:assert'
import { ClusterNetwork } from '../../src/core/cluster/network'
import { Redis, Cluster } from 'ioredis'

describe('Redis SET command integration', () => {
  const redisCluster = new ClusterNetwork(console)
  let redisClient: Cluster | undefined

  before(async () => {
    await redisCluster.init({ masters: 3, slaves: 0 })
    redisClient = new Redis.Cluster(
      [
        {
          host: '127.0.0.1',
          port: Array.from(redisCluster.getAll())[0].port,
        },
      ],
      {
        slotsRefreshTimeout: 10000000,
        lazyConnect: true,
      },
    )
    await redisClient?.connect()
  })

  after(async () => {
    await redisClient?.quit()
    await redisCluster.shutdown()
  })

  test('basic SET and GET', async () => {
    await redisClient?.set('testkey', 'testvalue')
    const value = await redisClient?.get('testkey')
    assert.strictEqual(value, 'testvalue')
  })

  test('SET with EX option', async () => {
    await redisClient?.set('exkey', 'exvalue', 'EX', 10)
    const value = await redisClient?.get('exkey')
    assert.strictEqual(value, 'exvalue')

    const ttl = await redisClient?.ttl('exkey')
    assert.ok(ttl !== undefined && ttl > 0 && ttl <= 10)
  })

  test('SET with PX option', async () => {
    await redisClient?.set('pxkey', 'pxvalue', 'PX', 5000)
    const value = await redisClient?.get('pxkey')
    assert.strictEqual(value, 'pxvalue')

    const ttl = await redisClient?.pttl('pxkey')
    assert.ok(ttl !== undefined && ttl > 0 && ttl <= 5000)
  })

  test('SET with NX option - key does not exist', async () => {
    const result = await redisClient?.set('nxkey1', 'nxvalue', 'NX')
    assert.strictEqual(result, 'OK')

    const value = await redisClient?.get('nxkey1')
    assert.strictEqual(value, 'nxvalue')
  })

  test('SET with NX option - key exists', async () => {
    await redisClient?.set('nxkey2', 'existing')
    const result = await redisClient?.set('nxkey2', 'newvalue', 'NX')
    assert.strictEqual(result, null)

    const value = await redisClient?.get('nxkey2')
    assert.strictEqual(value, 'existing')
  })

  test('SET with XX option - key exists', async () => {
    await redisClient?.set('xxkey1', 'existing')
    const result = await redisClient?.set('xxkey1', 'newvalue', 'XX')
    assert.strictEqual(result, 'OK')

    const value = await redisClient?.get('xxkey1')
    assert.strictEqual(value, 'newvalue')
  })

  test('SET with XX option - key does not exist', async () => {
    const result = await redisClient?.set('xxkey2', 'newvalue', 'XX')
    assert.strictEqual(result, null)

    const value = await redisClient?.get('xxkey2')
    assert.strictEqual(value, null)
  })

  test('SET with GET option', async () => {
    await redisClient?.set('getkey', 'oldvalue')

    // ioredis doesn't directly support GET option, but we can test with eval
    const result = await redisClient?.eval(
      `return redis.call('set', KEYS[1], ARGV[1], 'GET')`,
      1,
      'getkey',
      'newvalue',
    )

    assert.strictEqual(result, 'oldvalue')

    const value = await redisClient?.get('getkey')
    assert.strictEqual(value, 'newvalue')
  })

  test('SET with multiple options', async () => {
    await redisClient?.set('multikey', 'existing')

    // Test XX with EX
    const result = await redisClient?.eval(
      `return redis.call('set', KEYS[1], ARGV[1], 'XX', 'EX', ARGV[2])`,
      1,
      'multikey',
      'newvalue',
      '5',
    )

    assert.strictEqual(result, 'OK')

    const value = await redisClient?.get('multikey')
    assert.strictEqual(value, 'newvalue')

    const ttl = await redisClient?.ttl('multikey')
    assert.ok(ttl !== undefined && ttl > 0 && ttl <= 5)
  })
})
