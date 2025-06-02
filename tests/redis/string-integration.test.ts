import { test, describe, before, after } from 'node:test'
import assert from 'node:assert'
import { ClusterNetwork } from '../../src/core/cluster/network'
import { Redis, Cluster } from 'ioredis'

describe('String Commands Integration', () => {
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

  test('INCR and DECR commands', async () => {
    // INCR on non-existent key
    const incr1 = await redisClient?.incr('counter')
    assert.strictEqual(incr1, 1)

    // INCR on existing key
    const incr2 = await redisClient?.incr('counter')
    assert.strictEqual(incr2, 2)

    // DECR
    const decr1 = await redisClient?.decr('counter')
    assert.strictEqual(decr1, 1)
  })

  test('INCRBY and DECRBY commands', async () => {
    // INCRBY
    const incr1 = await redisClient?.incrby('bycounter', 5)
    assert.strictEqual(incr1, 5)

    const incr2 = await redisClient?.incrby('bycounter', 3)
    assert.strictEqual(incr2, 8)

    // DECRBY
    const decr1 = await redisClient?.decrby('bycounter', 2)
    assert.strictEqual(decr1, 6)
  })

  test('INCRBYFLOAT command', async () => {
    // INCRBYFLOAT
    const incr1 = await redisClient?.incrbyfloat('floatcounter', 1.5)
    assert.strictEqual(incr1, '1.5')

    const incr2 = await redisClient?.incrbyfloat('floatcounter', 2.3)
    assert.strictEqual(incr2, '3.8')
  })

  test('APPEND command', async () => {
    // APPEND to non-existent key
    const append1 = await redisClient?.append('appendkey', 'hello')
    assert.strictEqual(append1, 5)

    // APPEND to existing key
    const append2 = await redisClient?.append('appendkey', ' world')
    assert.strictEqual(append2, 11)

    const value = await redisClient?.get('appendkey')
    assert.strictEqual(value, 'hello world')
  })

  test('STRLEN command', async () => {
    // STRLEN on non-existent key
    const len1 = await redisClient?.strlen('nonexistent')
    assert.strictEqual(len1, 0)

    await redisClient?.set('strlenkey', 'hello')
    const len2 = await redisClient?.strlen('strlenkey')
    assert.strictEqual(len2, 5)
  })

  test('MGET command', async () => {
    await redisClient?.set('{same}mget1', 'value1')
    await redisClient?.set('{same}mget2', 'value2')

    const values = await redisClient?.mget(
      '{same}mget1',
      '{same}mget2',
      '{same}nonexistent',
    )
    assert.deepStrictEqual(values, ['value1', 'value2', null])
  })

  test('MSET command', async () => {
    await redisClient?.mset(
      '{same}mset1',
      'value1',
      '{same}mset2',
      'value2',
      '{same}mset3',
      'value3',
    )

    const get1 = await redisClient?.get('{same}mset1')
    const get2 = await redisClient?.get('{same}mset2')
    const get3 = await redisClient?.get('{same}mset3')

    assert.strictEqual(get1, 'value1')
    assert.strictEqual(get2, 'value2')
    assert.strictEqual(get3, 'value3')
  })

  test('MSETNX command', async () => {
    // All keys new
    const result1 = await redisClient?.msetnx(
      '{same}msetnx1',
      'value1',
      '{same}msetnx2',
      'value2',
    )
    assert.strictEqual(result1, 1)

    // Some keys exist
    const result2 = await redisClient?.msetnx(
      '{same}msetnx1',
      'newvalue',
      '{same}msetnx3',
      'value3',
    )
    assert.strictEqual(result2, 0)

    // Verify original values unchanged
    const check = await redisClient?.get('{same}msetnx1')
    assert.strictEqual(check, 'value1')
  })

  test('GETSET command', async () => {
    await redisClient?.set('getsetkey', 'oldvalue')

    const oldValue = await redisClient?.getset('getsetkey', 'newvalue')
    assert.strictEqual(oldValue, 'oldvalue')

    const newValue = await redisClient?.get('getsetkey')
    assert.strictEqual(newValue, 'newvalue')

    // GETSET on non-existent key
    const nullValue = await redisClient?.getset('newgetsetkey', 'firstvalue')
    assert.strictEqual(nullValue, null)
  })

  test('String commands workflow', async () => {
    // Create a session counter with user data
    await redisClient?.set('{user1001}name', 'Alice')
    await redisClient?.set('{user1001}sessions', '0')

    // Increment session count
    const sessions1 = await redisClient?.incr('{user1001}sessions')
    assert.strictEqual(sessions1, 1)

    // Add login timestamp
    await redisClient?.append('{user1001}name', ' (Online)')
    const nameWithStatus = await redisClient?.get('{user1001}name')
    assert.strictEqual(nameWithStatus, 'Alice (Online)')

    // Get multiple user fields
    const userData = await redisClient?.mget(
      '{user1001}name',
      '{user1001}sessions',
    )
    assert.deepStrictEqual(userData, ['Alice (Online)', '1'])

    // Update multiple fields atomically
    await redisClient?.mset(
      '{user1001}lastlogin',
      Date.now().toString(),
      '{user1001}score',
      '0',
    )

    // Increment score by points
    await redisClient?.incrby('{user1001}score', 150)
    const score = await redisClient?.get('{user1001}score')
    assert.strictEqual(score, '150')

    // Check total data length
    const nameLen = await redisClient?.strlen('{user1001}name')
    assert.strictEqual(nameLen, 14) // 'Alice (Online)'.length
  })
})
