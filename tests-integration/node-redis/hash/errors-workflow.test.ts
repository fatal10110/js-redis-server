import { test, describe, before, after } from 'node:test'
import assert from 'node:assert'
import { RedisClusterType } from 'redis'
import { TestRunner } from '../../test-config'
import { errorWithMessage, flushNodeRedisCluster, randomKey } from '../../utils'

const testRunner = new TestRunner()

describe(`Hash Commands Integration (node-redis, ${testRunner.getBackendName()})`, () => {
  let redisClient: RedisClusterType

  before(async () => {
    redisClient = (await testRunner.setupNodeRedisCluster()) as RedisClusterType
    await flushNodeRedisCluster(redisClient)
  })

  after(async () => {
    await testRunner.cleanup()
  })

  test('Hash command errors match Redis', async () => {
    const tag = `{hash-errors:${randomKey()}}`
    const hashKey = `${tag}:hash`
    const stringKey = `${tag}:string`

    try {
      await redisClient.set(stringKey, 'value')
      await redisClient.hSet(hashKey, {
        integer: 'abc',
        float: 'abc',
        'float-trailing-garbage': '1abc',
        'float-dangling-exponent': '1.0e',
        'float-trailing-space': '1.5 ',
        'leading-zero': '007',
        'negative-zero': '-0',
      })

      await assert.rejects(
        () => redisClient.hGet(stringKey, 'field'),
        errorWithMessage(
          'WRONGTYPE Operation against a key holding the wrong kind of value',
        ),
      )
      await assert.rejects(
        () =>
          redisClient.sendCommand(hashKey, false, ['HSET', hashKey, 'field']),
        errorWithMessage("ERR wrong number of arguments for 'hset' command"),
      )
      await assert.rejects(
        () =>
          redisClient.sendCommand(hashKey, false, [
            'HINCRBY',
            hashKey,
            'integer',
            'abc',
          ]),
        errorWithMessage('ERR value is not an integer or out of range'),
      )
      await assert.rejects(
        () =>
          redisClient.sendCommand(hashKey, false, [
            'HINCRBY',
            hashKey,
            'integer',
            '01',
          ]),
        errorWithMessage('ERR value is not an integer or out of range'),
      )
      await assert.rejects(
        () => redisClient.hIncrBy(hashKey, 'integer', 1),
        errorWithMessage('ERR hash value is not an integer'),
      )
      await assert.rejects(
        () => redisClient.hIncrBy(hashKey, 'leading-zero', 1),
        errorWithMessage('ERR hash value is not an integer'),
      )
      await assert.rejects(
        () => redisClient.hIncrBy(hashKey, 'negative-zero', 1),
        errorWithMessage('ERR hash value is not an integer'),
      )
      await assert.rejects(
        () =>
          redisClient.sendCommand(hashKey, false, [
            'HINCRBYFLOAT',
            hashKey,
            'float',
            'abc',
          ]),
        errorWithMessage('ERR value is not a valid float'),
      )
      await assert.rejects(
        () => redisClient.hIncrByFloat(hashKey, 'float', 1.5),
        errorWithMessage('ERR hash value is not a float'),
      )
      for (const field of [
        'float-trailing-garbage',
        'float-dangling-exponent',
        'float-trailing-space',
      ]) {
        await assert.rejects(
          () => redisClient.hIncrByFloat(hashKey, field, 1),
          errorWithMessage('ERR hash value is not a float'),
        )
      }
      for (const increment of ['1abc', '1.0e', '1.5 ']) {
        await assert.rejects(
          () =>
            redisClient.sendCommand(hashKey, false, [
              'HINCRBYFLOAT',
              hashKey,
              'missing',
              increment,
            ]),
          errorWithMessage('ERR value is not a valid float'),
        )
      }
    } finally {
      await redisClient.del([hashKey, stringKey])
    }
  })

  test('Hash commands workflow - User Profile', async () => {
    const userId = 'user:1001'

    await redisClient.hSet(userId, {
      name: 'Alice Johnson',
      email: 'alice@example.com',
      score: '0',
      level: '1',
      coins: '100.50',
    })

    const exists = await redisClient.hExists(userId, 'name')
    assert.strictEqual(exists, 1)

    const userData = await redisClient.hmGet(userId, ['name', 'email', 'score'])
    assert.deepStrictEqual(userData, [
      'Alice Johnson',
      'alice@example.com',
      '0',
    ])

    await redisClient.hIncrBy(userId, 'score', 150)
    await redisClient.hIncrBy(userId, 'level', 1)
    await redisClient.hIncrByFloat(userId, 'coins', 25.75)

    const profile = await redisClient.hGetAll(userId)
    assert.strictEqual(profile.name, 'Alice Johnson')
    assert.strictEqual(profile.score, '150')
    assert.strictEqual(profile.level, '2')
    assert.strictEqual(profile.coins, '126.25')

    const profileSize = await redisClient.hLen(userId)
    assert.strictEqual(profileSize, 5)

    const fields = await redisClient.hKeys(userId)
    assert.ok(fields.includes('name'))
    assert.ok(fields.includes('email'))
    assert.ok(fields.includes('score'))

    await redisClient.hSet(userId, 'old_email', profile.email || '')
    await redisClient.hDel(userId, 'email')

    const emailExists = await redisClient.hExists(userId, 'email')
    const oldEmailExists = await redisClient.hExists(userId, 'old_email')
    assert.strictEqual(emailExists, 0)
    assert.strictEqual(oldEmailExists, 1)
  })

  test('Hash commands workflow - Shopping Cart', async () => {
    const cartId = 'cart:session123'

    await redisClient.hSet(cartId, 'item:001', '2') // quantity 2
    await redisClient.hSet(cartId, 'item:002', '1') // quantity 1
    await redisClient.hSet(cartId, 'item:003', '3') // quantity 3

    await redisClient.hIncrBy(cartId, 'item:001', 1) // now 3

    const cart = await redisClient.hGetAll(cartId)
    assert.strictEqual(cart['item:001'], '3')
    assert.strictEqual(cart['item:002'], '1')
    assert.strictEqual(cart['item:003'], '3')

    await redisClient.hDel(cartId, 'item:002')

    const cartSize = await redisClient.hLen(cartId)
    assert.strictEqual(cartSize, 2)

    const items = await redisClient.hKeys(cartId)
    assert.deepStrictEqual(items.sort(), ['item:001', 'item:003'])

    const quantities = await redisClient.hVals(cartId)
    assert.deepStrictEqual(quantities.sort(), ['3', '3'])
  })
})
