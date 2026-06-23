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

  test('SDIFF command', async () => {
    await redisClient.sAdd('{test}setA', ['a', 'b', 'c', 'd'])
    await redisClient.sAdd('{test}setB', ['b', 'd', 'e'])

    const diff = await redisClient.sDiff(['{test}setA', '{test}setB'])
    assert.strictEqual(diff.length, 2)
    assert.ok(diff.includes('a'))
    assert.ok(diff.includes('c'))
  })

  test('SINTER command', async () => {
    await redisClient.sAdd('{test}setX', ['a', 'b', 'c', 'd'])
    await redisClient.sAdd('{test}setY', ['b', 'c', 'e', 'f'])

    const inter = await redisClient.sInter(['{test}setX', '{test}setY'])
    assert.strictEqual(inter.length, 2)
    assert.ok(inter.includes('b'))
    assert.ok(inter.includes('c'))
  })

  test('SINTERCARD command matches Redis', async () => {
    const tag = `{sintercard:${randomKey()}}`
    const setA = `${tag}:a`
    const setB = `${tag}:b`
    const setC = `${tag}:c`
    const missing = `${tag}:missing`
    const stringKey = `${tag}:string`
    const crossSlotKey = `sintercard-cross:${randomKey()}`
    let directClient: RedisClientType | undefined

    try {
      directClient = await connectToNodeRedisSlotOwner(redisClient, setA)

      await directClient.sAdd(setA, ['a', 'b', 'c', 'd'])
      await directClient.sAdd(setB, ['b', 'c', 'd', 'e'])
      await directClient.sAdd(setC, ['c', 'd', 'f'])

      const count = await directClient.sInterCard([setA, setB, setC])
      assert.strictEqual(count, 2)

      const limited = await directClient.sInterCard([setA, setB, setC], {
        LIMIT: 1,
      })
      assert.strictEqual(limited, 1)

      const unlimited = await directClient.sInterCard([setA, setB], {
        LIMIT: 0,
      })
      assert.strictEqual(unlimited, 3)

      const withMissing = await directClient.sInterCard([setA, missing])
      assert.strictEqual(withMissing, 0)

      await directClient.set(stringKey, 'value')
      await assert.rejects(
        () => directClient!.sInterCard([setA, stringKey]),
        errorWithMessage(
          'WRONGTYPE Operation against a key holding the wrong kind of value',
        ),
      )

      await assert.rejects(
        () => directClient!.sInterCard([setA, crossSlotKey]),
        errorWithMessage(
          "CROSSSLOT Keys in request don't hash to the same slot",
        ),
      )

      await assert.rejects(
        () => directClient!.sendCommand(['SINTERCARD']),
        errorWithMessage(
          "ERR wrong number of arguments for 'sintercard' command",
        ),
      )
      await assert.rejects(
        () => directClient!.sendCommand(['SINTERCARD', 'two', setA]),
        errorWithMessage('ERR numkeys should be greater than 0'),
      )
      await assert.rejects(
        () => directClient!.sendCommand(['SINTERCARD', '0', setA]),
        errorWithMessage('ERR numkeys should be greater than 0'),
      )
      await assert.rejects(
        () => directClient!.sendCommand(['SINTERCARD', '2', setA]),
        errorWithMessage(
          "ERR Number of keys can't be greater than number of args",
        ),
      )
      await assert.rejects(
        () => directClient!.sendCommand(['SINTERCARD', '1', setA, setB]),
        errorWithMessage('ERR syntax error'),
      )
      await assert.rejects(
        () => directClient!.sendCommand(['SINTERCARD', '1', setA, 'LIMIT']),
        errorWithMessage('ERR syntax error'),
      )
      await assert.rejects(
        () =>
          directClient!.sendCommand(['SINTERCARD', '1', setA, 'LIMIT', 'abc']),
        errorWithMessage("ERR LIMIT can't be negative"),
      )
      await assert.rejects(
        () =>
          directClient!.sendCommand(['SINTERCARD', '1', setA, 'LIMIT', '-1']),
        errorWithMessage("ERR LIMIT can't be negative"),
      )
      await assert.rejects(
        () =>
          directClient!.sendCommand([
            'SINTERCARD',
            '1',
            setA,
            'LIMIT',
            '1',
            'LIMIT',
          ]),
        errorWithMessage('ERR syntax error'),
      )
    } finally {
      await directClient?.del([setA, setB, setC, missing, stringKey])
      directClient?.destroy()
    }
  })

  test('SUNION command', async () => {
    await redisClient.sAdd('{test}setP', ['a', 'b'])
    await redisClient.sAdd('{test}setQ', ['b', 'c', 'd'])

    const union = await redisClient.sUnion(['{test}setP', '{test}setQ'])
    assert.strictEqual(union.length, 4)
    assert.ok(union.includes('a'))
    assert.ok(union.includes('b'))
    assert.ok(union.includes('c'))
    assert.ok(union.includes('d'))
  })

  test('SMOVE command', async () => {
    await redisClient.sAdd('{test}source', ['a', 'b', 'c'])
    await redisClient.sAdd('{test}dest', ['x', 'y'])

    const move1 = await redisClient.sMove('{test}source', '{test}dest', 'a')
    assert.strictEqual(move1, 1)

    const sourceHas = await redisClient.sIsMember('{test}source', 'a')
    assert.strictEqual(sourceHas, 0)

    const destHas = await redisClient.sIsMember('{test}dest', 'a')
    assert.strictEqual(destHas, 1)

    const move2 = await redisClient.sMove(
      '{test}source',
      '{test}dest',
      'nonexistent',
    )
    assert.strictEqual(move2, 0)
  })
})
