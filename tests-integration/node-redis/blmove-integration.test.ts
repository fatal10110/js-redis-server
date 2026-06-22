import { test, describe, before, after } from 'node:test'
import assert from 'node:assert'
import { RedisClusterType } from 'redis'
import { TestRunner } from '../test-config'
import { errorWithMessage, flushNodeRedisCluster, randomKey } from '../utils'

const testRunner = new TestRunner()

function waitForPark(ms = 80): Promise<void> {
  return new Promise(resolve => setTimeout(resolve, ms))
}

describe(`BLMOVE Integration (node-redis, ${testRunner.getBackendName()})`, () => {
  let client1: RedisClusterType
  let client2: RedisClusterType

  before(async () => {
    client1 = (await testRunner.setupNodeRedisCluster()) as RedisClusterType
    client2 = (await testRunner.setupNodeRedisCluster()) as RedisClusterType
    await flushNodeRedisCluster(client1)
  })

  after(async () => {
    await testRunner.cleanup()
  })

  test('BLMOVE returns immediately when source is non-empty', async () => {
    const tag = `{${randomKey()}}`
    const src = `${tag}:src`
    const dst = `${tag}:dst`

    await client1.del([src, dst])
    await client1.rPush(src, ['a', 'b', 'c'])

    assert.strictEqual(await client1.blMove(src, dst, 'LEFT', 'RIGHT', 1), 'a')
    assert.deepStrictEqual(await client1.lRange(src, 0, -1), ['b', 'c'])
    assert.deepStrictEqual(await client1.lRange(dst, 0, -1), ['a'])

    assert.strictEqual(await client1.blMove(src, dst, 'RIGHT', 'LEFT', 1), 'c')
    assert.deepStrictEqual(await client1.lRange(src, 0, -1), ['b'])
    assert.deepStrictEqual(await client1.lRange(dst, 0, -1), ['c', 'a'])
  })

  test('BLMOVE blocks then returns when a push arrives on source', async () => {
    const tag = `{${randomKey()}}`
    const src = `${tag}:src`
    const dst = `${tag}:dst`
    await client1.del([src, dst])

    const blockPromise = client1.blMove(src, dst, 'LEFT', 'RIGHT', 5)
    await waitForPark()
    await client2.rPush(src, 'world')

    assert.strictEqual(await blockPromise, 'world')
    assert.deepStrictEqual(await client1.lRange(dst, 0, -1), ['world'])
    assert.strictEqual(await client1.exists(src), 0)
  })

  test('BLMOVE timeout returns null', async () => {
    const tag = `{${randomKey()}}`
    const src = `${tag}:src`
    const dst = `${tag}:dst`
    await client1.del([src, dst])

    assert.strictEqual(
      await client1.blMove(src, dst, 'LEFT', 'RIGHT', 0.1),
      null,
    )
    assert.strictEqual(await client1.exists(dst), 0)
  })

  test('BLMOVE rotates a list onto itself', async () => {
    const tag = `{${randomKey()}}`
    const key = `${tag}:list`
    await client1.del(key)
    await client1.rPush(key, ['1', '2', '3'])

    assert.strictEqual(await client1.blMove(key, key, 'RIGHT', 'LEFT', 1), '3')
    assert.deepStrictEqual(await client1.lRange(key, 0, -1), ['3', '1', '2'])
  })

  test('BLMOVE error paths match Redis', async () => {
    const tag = `{${randomKey()}}`
    const src = `${tag}:src`
    const dst = `${tag}:dst`
    const stringKey = `${tag}:string`
    const send = (args: string[]) => client1.sendCommand(src, false, args)
    await client1.del([src, dst, stringKey])
    await client1.rPush(src, 'x')

    await assert.rejects(
      () => send(['BLMOVE', src, dst, 'UP', 'LEFT', '1']),
      errorWithMessage('ERR syntax error'),
    )
    await assert.rejects(
      () => send(['BLMOVE', src, dst, 'LEFT', 'RIGHT']),
      errorWithMessage("ERR wrong number of arguments for 'blmove' command"),
    )
    await assert.rejects(
      () => send(['BLMOVE', src, dst, 'LEFT', 'RIGHT', '-1']),
      errorWithMessage('ERR timeout is negative'),
    )
    await assert.rejects(
      () => send(['BLMOVE', src, dst, 'LEFT', 'RIGHT', 'abc']),
      errorWithMessage('ERR timeout is not a float or out of range'),
    )

    await client1.set(stringKey, 'value')
    await assert.rejects(
      () => send(['BLMOVE', stringKey, dst, 'LEFT', 'RIGHT', '1']),
      errorWithMessage(
        'WRONGTYPE Operation against a key holding the wrong kind of value',
      ),
    )
    await assert.rejects(
      () => send(['BLMOVE', src, stringKey, 'LEFT', 'RIGHT', '1']),
      errorWithMessage(
        'WRONGTYPE Operation against a key holding the wrong kind of value',
      ),
    )
    assert.deepStrictEqual(await client1.lRange(src, 0, -1), ['x'])
  })
})
