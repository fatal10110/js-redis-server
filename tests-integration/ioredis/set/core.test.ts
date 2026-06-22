import { test, describe, before, after } from 'node:test'
import assert from 'node:assert'
import { Cluster, Redis } from 'ioredis'
import { TestRunner } from '../../test-config'
import { connectToSlotOwner, errorWithMessage, randomKey } from '../../utils'

const testRunner = new TestRunner()

describe(`Set Commands Integration (${testRunner.getBackendName()})`, () => {
  let redisClient: Cluster | undefined

  before(async () => {
    redisClient = await testRunner.setupIoredisCluster(
      'set-commands-integration',
    )
  })

  after(async () => {
    await testRunner.cleanup()
  })

  test('SADD and SCARD commands', async () => {
    // SADD single member
    const add1 = await redisClient?.sadd('set1', 'member1')
    assert.strictEqual(add1, 1)

    // SADD duplicate member
    const add2 = await redisClient?.sadd('set1', 'member1')
    assert.strictEqual(add2, 0)

    // SADD multiple members
    const add3 = await redisClient?.sadd(
      'set1',
      'member2',
      'member3',
      'member4',
    )
    assert.strictEqual(add3, 3)

    // Check cardinality
    const card = await redisClient?.scard('set1')
    assert.strictEqual(card, 4)
  })

  test('SMEMBERS command', async () => {
    await redisClient?.sadd('set2', 'a', 'b', 'c')

    const members = await redisClient?.smembers('set2')
    assert.strictEqual(members?.length, 3)
    assert.ok(members?.includes('a'))
    assert.ok(members?.includes('b'))
    assert.ok(members?.includes('c'))
  })

  test('SISMEMBER command', async () => {
    await redisClient?.sadd('set3', 'member1', 'member2')

    const is1 = await redisClient?.sismember('set3', 'member1')
    assert.strictEqual(is1, 1)

    const is2 = await redisClient?.sismember('set3', 'nonexistent')
    assert.strictEqual(is2, 0)
  })

  test('SMISMEMBER command matches Redis', async () => {
    const tag = `{smismember:${randomKey()}}`
    const setKey = `${tag}:set`
    const missingKey = `${tag}:missing`
    const stringKey = `${tag}:string`
    let directClient: Redis | undefined

    try {
      assert.ok(redisClient)
      directClient = await connectToSlotOwner(redisClient, setKey)
      await directClient.sadd(setKey, 'member1', 'member2')

      const result = await directClient.call(
        'SMISMEMBER',
        setKey,
        'member1',
        'missing',
        'member2',
        'member1',
      )
      assert.deepStrictEqual(result, [1, 0, 1, 1])

      const missing = await directClient.call(
        'SMISMEMBER',
        missingKey,
        'member1',
        'member2',
      )
      assert.deepStrictEqual(missing, [0, 0])

      await directClient.set(stringKey, 'value')
      await assert.rejects(
        () => directClient?.call('SMISMEMBER', stringKey, 'member1'),
        errorWithMessage(
          'WRONGTYPE Operation against a key holding the wrong kind of value',
        ),
      )

      await assert.rejects(
        () => directClient?.call('SMISMEMBER'),
        errorWithMessage(
          "ERR wrong number of arguments for 'smismember' command",
        ),
      )
      await assert.rejects(
        () => directClient?.call('SMISMEMBER', setKey),
        errorWithMessage(
          "ERR wrong number of arguments for 'smismember' command",
        ),
      )
    } finally {
      await directClient?.del(setKey, missingKey, stringKey)
      directClient?.disconnect()
    }
  })

  test('SREM command', async () => {
    await redisClient?.sadd('set4', 'a', 'b', 'c', 'd')

    // Remove single member
    const rem1 = await redisClient?.srem('set4', 'a')
    assert.strictEqual(rem1, 1)

    // Remove multiple members
    const rem2 = await redisClient?.srem('set4', 'b', 'c')
    assert.strictEqual(rem2, 2)

    // Remove non-existent member
    const rem3 = await redisClient?.srem('set4', 'nonexistent')
    assert.strictEqual(rem3, 0)

    // Check remaining members
    const remaining = await redisClient?.smembers('set4')
    assert.deepStrictEqual(remaining, ['d'])
  })

  test('SPOP command', async () => {
    await redisClient?.sadd('set5', 'a', 'b', 'c')

    // Pop random member
    const popped = await redisClient?.spop('set5')
    assert.ok(['a', 'b', 'c'].includes(popped!))

    // Check set size decreased
    const card = await redisClient?.scard('set5')
    assert.strictEqual(card, 2)

    // Pop from empty set
    await redisClient?.spop('set5')
    await redisClient?.spop('set5')
    const empty = await redisClient?.spop('set5')
    assert.strictEqual(empty, null)
  })

  test('SPOP command with count', async () => {
    const key = `{spop-count:${randomKey()}}:set`
    const missingKey = `${key}:missing`

    try {
      await redisClient?.sadd(key, 'a', 'b', 'c', 'd')

      const poppedOne = await redisClient?.spop(key, 1)
      assert.ok(Array.isArray(poppedOne))
      assert.strictEqual(poppedOne.length, 1)
      assert.ok(['a', 'b', 'c', 'd'].includes(poppedOne[0]))

      const cardAfterOne = await redisClient?.scard(key)
      assert.strictEqual(cardAfterOne, 3)

      const poppedRest = await redisClient?.spop(key, 10)
      assert.ok(Array.isArray(poppedRest))
      assert.strictEqual(poppedRest.length, 3)
      assert.deepStrictEqual([...poppedOne, ...poppedRest].sort(), [
        'a',
        'b',
        'c',
        'd',
      ])

      const missing = await redisClient?.spop(missingKey, 2)
      assert.deepStrictEqual(missing, [])

      await redisClient?.sadd(key, 'remaining')
      const zero = await redisClient?.spop(key, 0)
      assert.deepStrictEqual(zero, [])

      const cardAfterZero = await redisClient?.scard(key)
      assert.strictEqual(cardAfterZero, 1)

      await assert.rejects(
        () => redisClient?.call('SPOP', key, '-1'),
        errorWithMessage('ERR value is out of range, must be positive'),
      )
    } finally {
      await redisClient?.del(key, missingKey)
    }
  })

  test('SRANDMEMBER command', async () => {
    await redisClient?.sadd('set6', 'a', 'b', 'c')

    // Get random member without removing
    const random = await redisClient?.srandmember('set6')
    assert.ok(['a', 'b', 'c'].includes(random!))

    // Check set size unchanged
    const card = await redisClient?.scard('set6')
    assert.strictEqual(card, 3)

    // Get multiple random members
    const randoms = await redisClient?.srandmember('set6', 2)
    assert.strictEqual(randoms?.length, 2)
  })

  test('Set command errors match Redis', async () => {
    const tag = `{set-errors:${randomKey()}}`
    const setKey = `${tag}:set`
    const stringKey = `${tag}:string`

    try {
      await redisClient?.sadd(setKey, 'a')
      await redisClient?.set(stringKey, 'value')

      await assert.rejects(
        () => redisClient?.sadd(stringKey, 'a'),
        errorWithMessage(
          'WRONGTYPE Operation against a key holding the wrong kind of value',
        ),
      )
      await assert.rejects(
        () => redisClient?.call('SRANDMEMBER', setKey, 'abc'),
        errorWithMessage('ERR value is not an integer or out of range'),
      )
      await assert.rejects(
        () => redisClient?.smove(setKey, stringKey, 'a'),
        errorWithMessage(
          'WRONGTYPE Operation against a key holding the wrong kind of value',
        ),
      )
    } finally {
      await redisClient?.del(setKey, stringKey)
    }
  })
})
