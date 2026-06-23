import { test, describe, before, after } from 'node:test'
import assert from 'node:assert'
import { RedisClientType, RedisClusterType } from 'redis'
import { TestRunner } from '../../test-config'
import {
  connectToNodeRedisSlotOwner,
  errorWithMessage,
  flushNodeRedisCluster,
  randomKey,
} from '../../utils'

const testRunner = new TestRunner()

function delay(ms: number): Promise<void> {
  return new Promise(resolve => setTimeout(resolve, ms))
}

describe(`Hash Commands Integration (node-redis, ${testRunner.getBackendName()})`, () => {
  let redisClient: RedisClusterType

  before(async () => {
    redisClient = (await testRunner.setupNodeRedisCluster()) as RedisClusterType
    await flushNodeRedisCluster(redisClient)
  })

  after(async () => {
    await testRunner.cleanup()
  })

  test('HPERSIST, HTTL and HPTTL report and clear hash field TTLs', async () => {
    const key = `{hash-field-ttl-read:${randomKey()}}:hash`
    let directClient: RedisClientType | undefined

    try {
      directClient = await connectToNodeRedisSlotOwner(redisClient, key)
      await directClient.hSet(key, {
        persistent: 'value1',
        volatile: 'value2',
        soon: 'value3',
      })

      assert.deepStrictEqual(
        await directClient.hpExpire(key, 'volatile', 5000),
        [1],
      )
      assert.deepStrictEqual(await directClient.hpExpire(key, 'soon', 20), [1])

      const seconds = await directClient.hTTL(key, [
        'persistent',
        'volatile',
        'soon',
        'missing',
      ])
      assert.ok(Array.isArray(seconds))
      assert.strictEqual(seconds[0], -1)
      assert.strictEqual(typeof seconds[1], 'number')
      assert.ok(seconds[1] >= 0 && seconds[1] <= 5)
      assert.strictEqual(typeof seconds[2], 'number')
      assert.ok(seconds[2] >= 0 && seconds[2] <= 1)
      assert.strictEqual(seconds[3], -2)

      const milliseconds = await directClient.hpTTL(key, [
        'persistent',
        'volatile',
        'soon',
        'missing',
      ])
      assert.ok(Array.isArray(milliseconds))
      assert.strictEqual(milliseconds[0], -1)
      assert.strictEqual(typeof milliseconds[1], 'number')
      assert.ok(milliseconds[1] > 0 && milliseconds[1] <= 5000)
      assert.strictEqual(typeof milliseconds[2], 'number')
      assert.ok(milliseconds[2] > 0 && milliseconds[2] <= 20)
      assert.strictEqual(milliseconds[3], -2)

      assert.deepStrictEqual(
        await directClient.hPersist(key, [
          'persistent',
          'volatile',
          'missing',
          'volatile',
        ]),
        [-1, 1, -2, -1],
      )
      assert.deepStrictEqual(
        await directClient.hpTTL(key, ['persistent', 'volatile']),
        [-1, -1],
      )

      await delay(60)

      assert.strictEqual(await directClient.hGet(key, 'persistent'), 'value1')
      assert.strictEqual(await directClient.hGet(key, 'volatile'), 'value2')
      assert.strictEqual(await directClient.hGet(key, 'soon'), null)
      assert.deepStrictEqual(
        await directClient.hTTL(key, ['persistent', 'volatile', 'soon']),
        [-1, -1, -2],
      )
    } finally {
      await directClient?.del(key)
      directClient?.destroy()
    }
  })

  test('HPERSIST, HTTL and HPTTL handle missing keys', async () => {
    const key = `{hash-field-ttl-missing:${randomKey()}}:hash`
    let directClient: RedisClientType | undefined

    try {
      directClient = await connectToNodeRedisSlotOwner(redisClient, key)

      assert.deepStrictEqual(
        await directClient.hPersist(key, ['a', 'b']),
        [-2, -2],
      )
      assert.deepStrictEqual(await directClient.hTTL(key, ['a', 'b']), [-2, -2])
      assert.deepStrictEqual(
        await directClient.hpTTL(key, ['a', 'b']),
        [-2, -2],
      )
      assert.strictEqual(await directClient.exists(key), 0)
    } finally {
      await directClient?.del(key)
      directClient?.destroy()
    }
  })

  test('HPERSIST, HTTL and HPTTL errors match Redis', async () => {
    const tag = `{hash-field-ttl-errors:${randomKey()}}`
    const hashKey = `${tag}:hash`
    const stringKey = `${tag}:string`
    let directClient: RedisClientType | undefined

    try {
      directClient = await connectToNodeRedisSlotOwner(redisClient, hashKey)
      await directClient.hSet(hashKey, 'field', 'value')
      await directClient.set(stringKey, 'value')

      for (const command of ['HPERSIST', 'HTTL', 'HPTTL']) {
        const commandName = command.toLowerCase()

        await assert.rejects(
          () =>
            directClient!.sendCommand([
              command,
              stringKey,
              'FIELDS',
              '1',
              'field',
            ]),
          errorWithMessage(
            'WRONGTYPE Operation against a key holding the wrong kind of value',
          ),
        )
        await assert.rejects(
          () => directClient!.sendCommand([command]),
          errorWithMessage(
            `ERR wrong number of arguments for '${commandName}' command`,
          ),
        )
        await assert.rejects(
          () => directClient!.sendCommand([command, hashKey]),
          errorWithMessage(
            `ERR wrong number of arguments for '${commandName}' command`,
          ),
        )
        await assert.rejects(
          () =>
            directClient!.sendCommand([
              command,
              hashKey,
              'FIELD',
              '1',
              'field',
            ]),
          errorWithMessage(
            'ERR Mandatory argument FIELDS is missing or not at the right position',
          ),
        )
        await assert.rejects(
          () =>
            directClient!.sendCommand([
              command,
              hashKey,
              'FIELDS',
              'abc',
              'field',
            ]),
          errorWithMessage('ERR Number of fields must be a positive integer'),
        )
        await assert.rejects(
          () =>
            directClient!.sendCommand([
              command,
              hashKey,
              'FIELDS',
              '0',
              'field',
            ]),
          errorWithMessage('ERR Number of fields must be a positive integer'),
        )
        await assert.rejects(
          () =>
            directClient!.sendCommand([
              command,
              hashKey,
              'FIELDS',
              '-1',
              'field',
            ]),
          errorWithMessage('ERR Number of fields must be a positive integer'),
        )
        await assert.rejects(
          () =>
            directClient!.sendCommand([
              command,
              hashKey,
              'FIELDS',
              '9223372036854775808',
              'field',
            ]),
          errorWithMessage('ERR Number of fields must be a positive integer'),
        )
        await assert.rejects(
          () =>
            directClient!.sendCommand([
              command,
              hashKey,
              'FIELDS',
              '2',
              'field',
            ]),
          errorWithMessage(
            'ERR The `numfields` parameter must match the number of arguments',
          ),
        )
        await assert.rejects(
          () =>
            directClient!.sendCommand([
              command,
              hashKey,
              'FIELDS',
              '1',
              'field',
              'extra',
            ]),
          errorWithMessage(
            'ERR The `numfields` parameter must match the number of arguments',
          ),
        )
      }
    } finally {
      await directClient?.del([hashKey, stringKey])
      directClient?.destroy()
    }
  })
})
