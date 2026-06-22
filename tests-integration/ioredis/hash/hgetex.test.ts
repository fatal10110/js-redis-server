import { test, describe, before, after } from 'node:test'
import assert from 'node:assert'
import { Cluster, Redis } from 'ioredis'
import { TestRunner } from '../../test-config'
import { connectToSlotOwner, errorWithMessage, randomKey } from '../../utils'

const testRunner = new TestRunner()

describe(`Hash Commands Integration (${testRunner.getBackendName()})`, () => {
  let redisClient: Cluster | undefined

  before(async () => {
    redisClient = await testRunner.setupIoredisCluster('hash-integration')
  })

  after(async () => {
    await testRunner.cleanup()
  })

  test('HGETEX returns values and updates field TTLs like Redis', async () => {
    const tag = `{hgetex:${randomKey()}}`
    const key = `${tag}:hash`
    let directClient: Redis | undefined

    try {
      assert.ok(redisClient)
      directClient = await connectToSlotOwner(redisClient, key)
      await directClient.hset(
        key,
        'f1',
        'v1',
        'f2',
        'v2',
        'f3',
        'v3',
        'f4',
        'v4',
      )

      // Plain read behaves like HMGET, including nil for missing fields.
      assert.deepStrictEqual(
        await directClient.call(
          'HGETEX',
          key,
          'FIELDS',
          '3',
          'f1',
          'missing',
          'f2',
        ),
        ['v1', null, 'v2'],
      )
      // Missing key yields all-nil without creating the hash.
      assert.deepStrictEqual(
        await directClient.call(
          'HGETEX',
          `${tag}:nokey`,
          'FIELDS',
          '2',
          'a',
          'b',
        ),
        [null, null],
      )
      assert.strictEqual(await directClient.exists(`${tag}:nokey`), 0)

      // EX sets a relative TTL and returns the value.
      assert.deepStrictEqual(
        await directClient.call(
          'HGETEX',
          key,
          'EX',
          '100',
          'FIELDS',
          '1',
          'f1',
        ),
        ['v1'],
      )
      const ttlSet = (await directClient.call(
        'HTTL',
        key,
        'FIELDS',
        '1',
        'f1',
      )) as number[]
      assert.ok(ttlSet[0] > 90 && ttlSet[0] <= 100, `ttl ${ttlSet[0]}`)

      // No expiration clause leaves the existing TTL untouched.
      assert.deepStrictEqual(
        await directClient.call('HGETEX', key, 'FIELDS', '1', 'f1'),
        ['v1'],
      )
      const ttlKeep = (await directClient.call(
        'HTTL',
        key,
        'FIELDS',
        '1',
        'f1',
      )) as number[]
      assert.ok(ttlKeep[0] > 90 && ttlKeep[0] <= 100, `ttl ${ttlKeep[0]}`)

      // PERSIST clears the TTL.
      assert.deepStrictEqual(
        await directClient.call('HGETEX', key, 'PERSIST', 'FIELDS', '1', 'f1'),
        ['v1'],
      )
      assert.deepStrictEqual(
        await directClient.call('HTTL', key, 'FIELDS', '1', 'f1'),
        [-1],
      )

      // PX sets a millisecond TTL.
      await directClient.call('HGETEX', key, 'PX', '50000', 'FIELDS', '1', 'f2')
      const pttlSet = (await directClient.call(
        'HPTTL',
        key,
        'FIELDS',
        '1',
        'f2',
      )) as number[]
      assert.ok(pttlSet[0] > 40000 && pttlSet[0] <= 50000, `pttl ${pttlSet[0]}`)

      // EXAT in the past returns the value but deletes the field.
      assert.deepStrictEqual(
        await directClient.call(
          'HGETEX',
          key,
          'EXAT',
          '1',
          'FIELDS',
          '1',
          'f3',
        ),
        ['v3'],
      )
      assert.strictEqual(await directClient.hexists(key, 'f3'), 0)

      // EX 0 expires immediately: value is returned, field removed.
      assert.deepStrictEqual(
        await directClient.call('HGETEX', key, 'EX', '0', 'FIELDS', '1', 'f4'),
        ['v4'],
      )
      assert.strictEqual(await directClient.hexists(key, 'f4'), 0)

      // Expiring the last field removes the key entirely.
      const lone = `${tag}:lone`
      await directClient.hset(lone, 'only', 'v')
      assert.deepStrictEqual(
        await directClient.call(
          'HGETEX',
          lone,
          'EXAT',
          '1',
          'FIELDS',
          '1',
          'only',
        ),
        ['v'],
      )
      assert.strictEqual(await directClient.exists(lone), 0)
    } finally {
      await directClient?.del(key)
      directClient?.disconnect()
    }
  })

  test('HGETEX errors match Redis', async () => {
    const tag = `{hgetex-errors:${randomKey()}}`
    const hashKey = `${tag}:hash`
    const stringKey = `${tag}:string`
    let directClient: Redis | undefined

    try {
      assert.ok(redisClient)
      directClient = await connectToSlotOwner(redisClient, hashKey)
      await directClient.hset(hashKey, 'field', 'value')
      await directClient.set(stringKey, 'value')

      await assert.rejects(
        () => directClient?.call('HGETEX', stringKey, 'FIELDS', '1', 'field'),
        errorWithMessage(
          'WRONGTYPE Operation against a key holding the wrong kind of value',
        ),
      )
      await assert.rejects(
        () => directClient?.call('HGETEX'),
        errorWithMessage("ERR wrong number of arguments for 'hgetex' command"),
      )
      await assert.rejects(
        () => directClient?.call('HGETEX', hashKey),
        errorWithMessage("ERR wrong number of arguments for 'hgetex' command"),
      )
      await assert.rejects(
        () => directClient?.call('HGETEX', hashKey, 'FIELDS', '1'),
        errorWithMessage("ERR wrong number of arguments for 'hgetex' command"),
      )
      await assert.rejects(
        () => directClient?.call('HGETEX', hashKey, 'FIELD', '1', 'field'),
        errorWithMessage(
          'ERR Mandatory argument FIELDS is missing or not at the right position',
        ),
      )
      // KEEPTTL is not a valid HGETEX option.
      await assert.rejects(
        () =>
          directClient?.call(
            'HGETEX',
            hashKey,
            'KEEPTTL',
            'FIELDS',
            '1',
            'field',
          ),
        errorWithMessage(
          'ERR Mandatory argument FIELDS is missing or not at the right position',
        ),
      )
      // Only one expiration clause is allowed.
      await assert.rejects(
        () =>
          directClient?.call(
            'HGETEX',
            hashKey,
            'EX',
            '100',
            'PERSIST',
            'FIELDS',
            '1',
            'field',
          ),
        errorWithMessage(
          'ERR Mandatory argument FIELDS is missing or not at the right position',
        ),
      )
      await assert.rejects(
        () => directClient?.call('HGETEX', hashKey, 'FIELDS', 'abc', 'field'),
        errorWithMessage('ERR Number of fields must be a positive integer'),
      )
      await assert.rejects(
        () => directClient?.call('HGETEX', hashKey, 'FIELDS', '0', 'field'),
        errorWithMessage('ERR Number of fields must be a positive integer'),
      )
      await assert.rejects(
        () => directClient?.call('HGETEX', hashKey, 'FIELDS', '2', 'field'),
        errorWithMessage(
          'ERR The `numfields` parameter must match the number of arguments',
        ),
      )
      await assert.rejects(
        () =>
          directClient?.call(
            'HGETEX',
            hashKey,
            'EX',
            'abc',
            'FIELDS',
            '1',
            'field',
          ),
        errorWithMessage('ERR value is not an integer or out of range'),
      )
      await assert.rejects(
        () =>
          directClient?.call(
            'HGETEX',
            hashKey,
            'EX',
            '-5',
            'FIELDS',
            '1',
            'field',
          ),
        errorWithMessage('ERR invalid expire time, must be >= 0'),
      )
      await assert.rejects(
        () =>
          directClient?.call(
            'HGETEX',
            hashKey,
            'EXAT',
            '99999999999',
            'FIELDS',
            '1',
            'field',
          ),
        errorWithMessage("ERR invalid expire time in 'hgetex' command"),
      )
    } finally {
      await directClient?.del(hashKey, stringKey)
      directClient?.disconnect()
    }
  })
})
