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

describe(`Set Commands Integration (node-redis, ${testRunner.getBackendName()})`, () => {
  let redisClient: RedisClusterType

  before(async () => {
    redisClient = (await testRunner.setupNodeRedisCluster()) as RedisClusterType
    await flushNodeRedisCluster(redisClient)
  })

  after(async () => {
    await testRunner.cleanup()
  })

  test('SADD and SCARD commands', async () => {
    const add1 = await redisClient.sAdd('set1', 'member1')
    assert.strictEqual(add1, 1)

    const add2 = await redisClient.sAdd('set1', 'member1')
    assert.strictEqual(add2, 0)

    const add3 = await redisClient.sAdd('set1', [
      'member2',
      'member3',
      'member4',
    ])
    assert.strictEqual(add3, 3)

    const card = await redisClient.sCard('set1')
    assert.strictEqual(card, 4)
  })

  test('SMEMBERS command', async () => {
    await redisClient.sAdd('set2', ['a', 'b', 'c'])

    const members = await redisClient.sMembers('set2')
    assert.strictEqual(members.length, 3)
    assert.ok(members.includes('a'))
    assert.ok(members.includes('b'))
    assert.ok(members.includes('c'))
  })

  test('SISMEMBER command', async () => {
    await redisClient.sAdd('set3', ['member1', 'member2'])

    const is1 = await redisClient.sIsMember('set3', 'member1')
    assert.strictEqual(is1, 1)

    const is2 = await redisClient.sIsMember('set3', 'nonexistent')
    assert.strictEqual(is2, 0)
  })

  test('SMISMEMBER command matches Redis', async () => {
    const tag = `{smismember:${randomKey()}}`
    const setKey = `${tag}:set`
    const missingKey = `${tag}:missing`
    const stringKey = `${tag}:string`
    let directClient: RedisClientType | undefined

    try {
      directClient = await connectToNodeRedisSlotOwner(redisClient, setKey)
      await directClient.sAdd(setKey, ['member1', 'member2'])

      const result = await directClient.sendCommand([
        'SMISMEMBER',
        setKey,
        'member1',
        'missing',
        'member2',
        'member1',
      ])
      assert.deepStrictEqual(result, [1, 0, 1, 1])

      const missing = await directClient.sendCommand([
        'SMISMEMBER',
        missingKey,
        'member1',
        'member2',
      ])
      assert.deepStrictEqual(missing, [0, 0])

      await directClient.set(stringKey, 'value')
      await assert.rejects(
        () => directClient!.sendCommand(['SMISMEMBER', stringKey, 'member1']),
        errorWithMessage(
          'WRONGTYPE Operation against a key holding the wrong kind of value',
        ),
      )

      await assert.rejects(
        () => directClient!.sendCommand(['SMISMEMBER']),
        errorWithMessage(
          "ERR wrong number of arguments for 'smismember' command",
        ),
      )
      await assert.rejects(
        () => directClient!.sendCommand(['SMISMEMBER', setKey]),
        errorWithMessage(
          "ERR wrong number of arguments for 'smismember' command",
        ),
      )
    } finally {
      await directClient?.del([setKey, missingKey, stringKey])
      directClient?.destroy()
    }
  })

  test('SREM command', async () => {
    await redisClient.sAdd('set4', ['a', 'b', 'c', 'd'])

    const rem1 = await redisClient.sRem('set4', 'a')
    assert.strictEqual(rem1, 1)

    const rem2 = await redisClient.sRem('set4', ['b', 'c'])
    assert.strictEqual(rem2, 2)

    const rem3 = await redisClient.sRem('set4', 'nonexistent')
    assert.strictEqual(rem3, 0)

    const remaining = await redisClient.sMembers('set4')
    assert.deepStrictEqual(remaining, ['d'])
  })

  test('SPOP command', async () => {
    await redisClient.sAdd('set5', ['a', 'b', 'c'])

    const popped = await redisClient.sPop('set5')
    assert.ok(['a', 'b', 'c'].includes(popped!))

    const card = await redisClient.sCard('set5')
    assert.strictEqual(card, 2)

    await redisClient.sPop('set5')
    await redisClient.sPop('set5')
    const empty = await redisClient.sPop('set5')
    assert.strictEqual(empty, null)
  })

  test('SPOP command with count', async () => {
    const key = `{spop-count:${randomKey()}}:set`
    const missingKey = `${key}:missing`

    try {
      await redisClient.sAdd(key, ['a', 'b', 'c', 'd'])

      const poppedOne = await redisClient.sPopCount(key, 1)
      assert.ok(Array.isArray(poppedOne))
      assert.strictEqual(poppedOne.length, 1)
      assert.ok(['a', 'b', 'c', 'd'].includes(poppedOne[0]))

      const cardAfterOne = await redisClient.sCard(key)
      assert.strictEqual(cardAfterOne, 3)

      const poppedRest = await redisClient.sPopCount(key, 10)
      assert.ok(Array.isArray(poppedRest))
      assert.strictEqual(poppedRest.length, 3)
      assert.deepStrictEqual([...poppedOne, ...poppedRest].sort(), [
        'a',
        'b',
        'c',
        'd',
      ])

      const missing = await redisClient.sPopCount(missingKey, 2)
      assert.deepStrictEqual(missing, [])

      await redisClient.sAdd(key, 'remaining')
      const zero = await redisClient.sPopCount(key, 0)
      assert.deepStrictEqual(zero, [])

      const cardAfterZero = await redisClient.sCard(key)
      assert.strictEqual(cardAfterZero, 1)

      await assert.rejects(
        () => redisClient.sendCommand(key, false, ['SPOP', key, '-1']),
        errorWithMessage('ERR value is out of range, must be positive'),
      )
    } finally {
      await redisClient.del([key, missingKey])
    }
  })

  test('SRANDMEMBER command', async () => {
    await redisClient.sAdd('set6', ['a', 'b', 'c'])

    const random = await redisClient.sRandMember('set6')
    assert.ok(['a', 'b', 'c'].includes(random!))

    const card = await redisClient.sCard('set6')
    assert.strictEqual(card, 3)

    const randoms = await redisClient.sRandMemberCount('set6', 2)
    assert.strictEqual(randoms.length, 2)
  })

  test('Set command errors match Redis', async () => {
    const tag = `{set-errors:${randomKey()}}`
    const setKey = `${tag}:set`
    const stringKey = `${tag}:string`

    try {
      await redisClient.sAdd(setKey, 'a')
      await redisClient.set(stringKey, 'value')

      await assert.rejects(
        () => redisClient.sAdd(stringKey, 'a'),
        errorWithMessage(
          'WRONGTYPE Operation against a key holding the wrong kind of value',
        ),
      )
      await assert.rejects(
        () =>
          redisClient.sendCommand(setKey, true, ['SRANDMEMBER', setKey, 'abc']),
        errorWithMessage('ERR value is not an integer or out of range'),
      )
      await assert.rejects(
        () => redisClient.sMove(setKey, stringKey, 'a'),
        errorWithMessage(
          'WRONGTYPE Operation against a key holding the wrong kind of value',
        ),
      )
    } finally {
      await redisClient.del([setKey, stringKey])
    }
  })
})
