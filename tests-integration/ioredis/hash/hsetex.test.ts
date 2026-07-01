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

  test('HSETEX sets fields and expiration like Redis', async () => {
    const tag = `{hsetex:${randomKey()}}`
    const key = `${tag}:hash`
    let directClient: Redis | undefined

    try {
      assert.ok(redisClient)
      directClient = await connectToSlotOwner(redisClient, key)

      // No expiration clause: plain upsert, key created on demand.
      assert.strictEqual(
        await directClient.hsetex(key, 'FIELDS', '2', 'f1', 'v1', 'f2', 'v2'),
        1,
      )
      assert.deepStrictEqual(
        await directClient.httl(key, 'FIELDS', '2', 'f1', 'f2'),
        [-1, -1],
      )

      // EX sets a relative TTL.
      assert.strictEqual(
        await directClient.hsetex(key, 'EX', '100', 'FIELDS', '1', 'f1', 'v1b'),
        1,
      )
      const ttlSet = (await directClient.httl(
        key,
        'FIELDS',
        '1',
        'f1',
      )) as number[]
      assert.ok(ttlSet[0] > 90 && ttlSet[0] <= 100, `ttl ${ttlSet[0]}`)
      assert.strictEqual(await directClient.hget(key, 'f1'), 'v1b')

      // Overwriting without an expiration clause clears the field's TTL
      // (matches real Redis: setting a value on a volatile field removes it).
      assert.strictEqual(
        await directClient.hsetex(key, 'FIELDS', '1', 'f1', 'v1c'),
        1,
      )
      assert.deepStrictEqual(
        await directClient.httl(key, 'FIELDS', '1', 'f1'),
        [-1],
      )

      // KEEPTTL retains the existing TTL while updating the value.
      await directClient.hsetex(key, 'EX', '100', 'FIELDS', '1', 'f1', 'v1d')
      assert.strictEqual(
        await directClient.hsetex(key, 'KEEPTTL', 'FIELDS', '1', 'f1', 'v1e'),
        1,
      )
      const ttlKept = (await directClient.httl(
        key,
        'FIELDS',
        '1',
        'f1',
      )) as number[]
      assert.ok(ttlKept[0] > 90 && ttlKept[0] <= 100, `ttl ${ttlKept[0]}`)
      assert.strictEqual(await directClient.hget(key, 'f1'), 'v1e')

      // PX sets a millisecond TTL.
      await directClient.hsetex(key, 'PX', '50000', 'FIELDS', '1', 'f2', 'v2b')
      const pttlSet = (await directClient.hpttl(
        key,
        'FIELDS',
        '1',
        'f2',
      )) as number[]
      assert.ok(pttlSet[0] > 40000 && pttlSet[0] <= 50000, `pttl ${pttlSet[0]}`)

      // EXAT in the past deletes the field immediately (value still set first).
      assert.strictEqual(
        await directClient.hsetex(
          key,
          'EXAT',
          '1',
          'FIELDS',
          '1',
          'f2',
          'gone',
        ),
        1,
      )
      assert.strictEqual(await directClient.hexists(key, 'f2'), 0)

      // EX 0 also expires immediately.
      assert.strictEqual(
        await directClient.hsetex(key, 'EX', '0', 'FIELDS', '1', 'f1', 'gone'),
        1,
      )
      assert.strictEqual(await directClient.hexists(key, 'f1'), 0)

      // Expiring the last field removes the key entirely.
      const lone = `${tag}:lone`
      assert.strictEqual(
        await directClient.hsetex(
          lone,
          'EXAT',
          '1',
          'FIELDS',
          '1',
          'only',
          'v',
        ),
        1,
      )
      assert.strictEqual(await directClient.exists(lone), 0)
    } finally {
      await directClient?.del(key)
      directClient?.disconnect()
    }
  })

  test('HSETEX FNX/FXX conditions match Redis', async () => {
    const tag = `{hsetex-cond:${randomKey()}}`
    const key = `${tag}:hash`
    let directClient: Redis | undefined

    try {
      assert.ok(redisClient)
      directClient = await connectToSlotOwner(redisClient, key)
      await directClient.hset(key, 'a', '1')

      // FXX on a missing key: no fields exist, nothing set.
      assert.strictEqual(
        await directClient.hsetex(
          `${tag}:nokey`,
          'FXX',
          'FIELDS',
          '1',
          'f1',
          'v1',
        ),
        0,
      )
      assert.strictEqual(await directClient.exists(`${tag}:nokey`), 0)

      // FNX fails if ANY field already exists — nothing is set, not even 'b'.
      assert.strictEqual(
        await directClient.hsetex(
          key,
          'FNX',
          'FIELDS',
          '2',
          'a',
          '2',
          'b',
          '2',
        ),
        0,
      )
      assert.deepStrictEqual(await directClient.hgetall(key), { a: '1' })

      // FNX succeeds when none of the fields exist.
      assert.strictEqual(
        await directClient.hsetex(key, 'FNX', 'FIELDS', '1', 'b', '2'),
        1,
      )
      assert.strictEqual(await directClient.hget(key, 'b'), '2')

      // FXX fails if ANY field is missing.
      assert.strictEqual(
        await directClient.hsetex(
          key,
          'FXX',
          'FIELDS',
          '2',
          'a',
          'x',
          'c',
          'x',
        ),
        0,
      )
      assert.strictEqual(await directClient.hexists(key, 'c'), 0)

      // FXX succeeds when all fields exist.
      assert.strictEqual(
        await directClient.hsetex(
          key,
          'FXX',
          'EX',
          '60',
          'FIELDS',
          '1',
          'a',
          '9',
        ),
        1,
      )
      assert.strictEqual(await directClient.hget(key, 'a'), '9')

      // Token order is flexible: expiration clause before the condition.
      assert.strictEqual(
        await directClient.hsetex(
          key,
          'EX',
          '60',
          'FXX',
          'FIELDS',
          '1',
          'a',
          '10',
        ),
        1,
      )
      assert.strictEqual(await directClient.hget(key, 'a'), '10')
    } finally {
      await directClient?.del(key)
      directClient?.disconnect()
    }
  })

  test('HSETEX errors match Redis', async () => {
    const tag = `{hsetex-errors:${randomKey()}}`
    const hashKey = `${tag}:hash`
    const stringKey = `${tag}:string`
    let directClient: Redis | undefined

    try {
      assert.ok(redisClient)
      directClient = await connectToSlotOwner(redisClient, hashKey)
      await directClient.hset(hashKey, 'field', 'value')
      await directClient.set(stringKey, 'value')

      await assert.rejects(
        () =>
          directClient?.call('HSETEX', stringKey, 'FIELDS', '1', 'field', 'v'),
        errorWithMessage(
          'WRONGTYPE Operation against a key holding the wrong kind of value',
        ),
      )
      await assert.rejects(
        () => directClient?.call('HSETEX'),
        errorWithMessage("ERR wrong number of arguments for 'hsetex' command"),
      )
      await assert.rejects(
        () => directClient?.call('HSETEX', hashKey),
        errorWithMessage("ERR wrong number of arguments for 'hsetex' command"),
      )
      await assert.rejects(
        () => directClient?.call('HSETEX', hashKey, 'FIELDS', '1'),
        errorWithMessage("ERR wrong number of arguments for 'hsetex' command"),
      )
      // Unrecognized token before FIELDS.
      await assert.rejects(
        () =>
          directClient?.call(
            'HSETEX',
            hashKey,
            'BOGUS',
            'FIELDS',
            '1',
            'field',
            'v',
          ),
        errorWithMessage('ERR unknown argument: BOGUS'),
      )
      // FNX and FXX are mutually exclusive.
      await assert.rejects(
        () =>
          directClient?.call(
            'HSETEX',
            hashKey,
            'FNX',
            'FXX',
            'FIELDS',
            '1',
            'field',
            'v',
          ),
        errorWithMessage(
          'ERR Only one of FXX or FNX arguments can be specified',
        ),
      )
      // Only one expiration clause allowed.
      await assert.rejects(
        () =>
          directClient?.call(
            'HSETEX',
            hashKey,
            'EX',
            '60',
            'KEEPTTL',
            'FIELDS',
            '1',
            'field',
            'v',
          ),
        errorWithMessage(
          'ERR Only one of EX, PX, EXAT, PXAT or KEEPTTL arguments can be specified',
        ),
      )
      await assert.rejects(
        () =>
          directClient?.call('HSETEX', hashKey, 'FIELDS', 'abc', 'field', 'v'),
        errorWithMessage('ERR invalid number of fields'),
      )
      await assert.rejects(
        () =>
          directClient?.call('HSETEX', hashKey, 'FIELDS', '0', 'field', 'v'),
        errorWithMessage('ERR invalid number of fields'),
      )
      // numfields declared but fewer pairs supplied.
      await assert.rejects(
        () =>
          directClient?.call('HSETEX', hashKey, 'FIELDS', '2', 'field', 'v'),
        errorWithMessage("ERR wrong number of arguments for 'hsetex' command"),
      )
      // More pairs supplied than declared.
      await assert.rejects(
        () =>
          directClient?.call(
            'HSETEX',
            hashKey,
            'FIELDS',
            '1',
            'field',
            'v',
            'extra',
            'v2',
          ),
        errorWithMessage("ERR wrong number of arguments for 'hsetex' command"),
      )
      await assert.rejects(
        () =>
          directClient?.call(
            'HSETEX',
            hashKey,
            'EX',
            'abc',
            'FIELDS',
            '1',
            'field',
            'v',
          ),
        errorWithMessage('ERR value is not an integer or out of range'),
      )
      await assert.rejects(
        () =>
          directClient?.call(
            'HSETEX',
            hashKey,
            'EX',
            '-5',
            'FIELDS',
            '1',
            'field',
            'v',
          ),
        errorWithMessage('ERR invalid expire time, must be >= 0'),
      )
      await assert.rejects(
        () =>
          directClient?.call(
            'HSETEX',
            hashKey,
            'EXAT',
            '99999999999',
            'FIELDS',
            '1',
            'field',
            'v',
          ),
        errorWithMessage("ERR invalid expire time in 'hsetex' command"),
      )
      // Invalid expire time is reported even when FXX would otherwise skip
      // (missing key) — the timestamp is resolved before the condition check.
      await assert.rejects(
        () =>
          directClient?.call(
            'HSETEX',
            `${tag}:nokey`,
            'FXX',
            'EX',
            '-5',
            'FIELDS',
            '1',
            'field',
            'v',
          ),
        errorWithMessage('ERR invalid expire time, must be >= 0'),
      )
    } finally {
      await directClient?.del(hashKey, stringKey)
      directClient?.disconnect()
    }
  })
})
