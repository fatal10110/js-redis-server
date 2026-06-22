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

  test('SDIFF command', async () => {
    await redisClient?.sadd('{test}setA', 'a', 'b', 'c', 'd')
    await redisClient?.sadd('{test}setB', 'b', 'd', 'e')

    const diff = await redisClient?.sdiff('{test}setA', '{test}setB')
    assert.strictEqual(diff?.length, 2)
    assert.ok(diff?.includes('a'))
    assert.ok(diff?.includes('c'))
  })

  test('SINTER command', async () => {
    await redisClient?.sadd('{test}setX', 'a', 'b', 'c', 'd')
    await redisClient?.sadd('{test}setY', 'b', 'c', 'e', 'f')

    const inter = await redisClient?.sinter('{test}setX', '{test}setY')
    assert.strictEqual(inter?.length, 2)
    assert.ok(inter?.includes('b'))
    assert.ok(inter?.includes('c'))
  })

  test('SINTERCARD command matches Redis', async () => {
    const tag = `{sintercard:${randomKey()}}`
    const setA = `${tag}:a`
    const setB = `${tag}:b`
    const setC = `${tag}:c`
    const missing = `${tag}:missing`
    const stringKey = `${tag}:string`
    const crossSlotKey = `sintercard-cross:${randomKey()}`
    let directClient: Redis | undefined

    try {
      assert.ok(redisClient)
      directClient = await connectToSlotOwner(redisClient, setA)

      await directClient.sadd(setA, 'a', 'b', 'c', 'd')
      await directClient.sadd(setB, 'b', 'c', 'd', 'e')
      await directClient.sadd(setC, 'c', 'd', 'f')

      const count = await directClient.sintercard('3', setA, setB, setC)
      assert.strictEqual(count, 2)

      const limited = await directClient.sintercard(
        '3',
        setA,
        setB,
        setC,
        'LIMIT',
        '1',
      )
      assert.strictEqual(limited, 1)

      const unlimited = await directClient.sintercard(
        '2',
        setA,
        setB,
        'LIMIT',
        '0',
      )
      assert.strictEqual(unlimited, 3)

      const withMissing = await directClient.sintercard('2', setA, missing)
      assert.strictEqual(withMissing, 0)

      await directClient.set(stringKey, 'value')
      await assert.rejects(
        () => directClient?.sintercard('2', setA, stringKey),
        errorWithMessage(
          'WRONGTYPE Operation against a key holding the wrong kind of value',
        ),
      )

      await assert.rejects(
        () => directClient?.call('SINTERCARD', '2', setA, crossSlotKey),
        errorWithMessage(
          "CROSSSLOT Keys in request don't hash to the same slot",
        ),
      )

      await assert.rejects(
        () => directClient?.call('SINTERCARD'),
        errorWithMessage(
          "ERR wrong number of arguments for 'sintercard' command",
        ),
      )
      await assert.rejects(
        () => directClient?.sintercard('two', setA),
        errorWithMessage('ERR numkeys should be greater than 0'),
      )
      await assert.rejects(
        () => directClient?.sintercard('0', setA),
        errorWithMessage('ERR numkeys should be greater than 0'),
      )
      await assert.rejects(
        () => directClient?.sintercard('2', setA),
        errorWithMessage(
          "ERR Number of keys can't be greater than number of args",
        ),
      )
      await assert.rejects(
        () => directClient?.sintercard('1', setA, setB),
        errorWithMessage('ERR syntax error'),
      )
      await assert.rejects(
        () => directClient?.sintercard('1', setA, 'LIMIT'),
        errorWithMessage('ERR syntax error'),
      )
      await assert.rejects(
        () => directClient?.sintercard('1', setA, 'LIMIT', 'abc'),
        errorWithMessage("ERR LIMIT can't be negative"),
      )
      await assert.rejects(
        () => directClient?.sintercard('1', setA, 'LIMIT', '-1'),
        errorWithMessage("ERR LIMIT can't be negative"),
      )
      await assert.rejects(
        () => directClient?.sintercard('1', setA, 'LIMIT', '1', 'LIMIT'),
        errorWithMessage('ERR syntax error'),
      )
    } finally {
      await directClient?.del(setA, setB, setC, missing, stringKey)
      directClient?.disconnect()
    }
  })

  test('SUNION command', async () => {
    await redisClient?.sadd('{test}setP', 'a', 'b')
    await redisClient?.sadd('{test}setQ', 'b', 'c', 'd')

    const union = await redisClient?.sunion('{test}setP', '{test}setQ')
    assert.strictEqual(union?.length, 4)
    assert.ok(union?.includes('a'))
    assert.ok(union?.includes('b'))
    assert.ok(union?.includes('c'))
    assert.ok(union?.includes('d'))
  })

  test('SMOVE command', async () => {
    await redisClient?.sadd('{test}source', 'a', 'b', 'c')
    await redisClient?.sadd('{test}dest', 'x', 'y')

    // Move existing member
    const move1 = await redisClient?.smove('{test}source', '{test}dest', 'a')
    assert.strictEqual(move1, 1)

    // Check source doesn't have member
    const sourceHas = await redisClient?.sismember('{test}source', 'a')
    assert.strictEqual(sourceHas, 0)

    // Check dest has member
    const destHas = await redisClient?.sismember('{test}dest', 'a')
    assert.strictEqual(destHas, 1)

    // Move non-existent member
    const move2 = await redisClient?.smove(
      '{test}source',
      '{test}dest',
      'nonexistent',
    )
    assert.strictEqual(move2, 0)
  })
})
