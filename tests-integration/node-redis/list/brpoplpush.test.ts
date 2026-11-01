import { test, describe, before, after } from 'node:test'
import assert from 'node:assert'
import { RedisClusterType } from 'redis'
import { TestRunner } from '../../test-config'
import { errorWithMessage, randomKey } from '../../utils'

const testRunner = new TestRunner()

function waitForPark(ms = 80): Promise<void> {
  return new Promise(resolve => setTimeout(resolve, ms))
}

const WRONGTYPE =
  'WRONGTYPE Operation against a key holding the wrong kind of value'

// BRPOPLPUSH is the deprecated form of BLMOVE source destination RIGHT LEFT
// (#495).
describe(`BRPOPLPUSH Integration (node-redis, ${testRunner.getBackendName()})`, () => {
  let client1: RedisClusterType
  let client2: RedisClusterType

  before(async () => {
    client1 = (await testRunner.setupNodeRedisCluster()) as RedisClusterType
    client2 = (await testRunner.setupNodeRedisCluster()) as RedisClusterType
  })

  after(async () => {
    await testRunner.cleanup()
  })

  test('BRPOPLPUSH returns immediately when source is non-empty', async () => {
    const tag = `{${randomKey()}}`
    const src = `${tag}:src`
    const dst = `${tag}:dst`

    await client1.rPush(src, ['a', 'b', 'c'])
    await client1.rPush(dst, 'x')

    assert.strictEqual(await client1.brPopLPush(src, dst, 1), 'c')
    assert.deepStrictEqual(await client1.lRange(src, 0, -1), ['a', 'b'])
    assert.deepStrictEqual(await client1.lRange(dst, 0, -1), ['c', 'x'])
  })

  test('BRPOPLPUSH blocks then returns when a push arrives on source', async () => {
    const tag = `{${randomKey()}}`
    const src = `${tag}:src`
    const dst = `${tag}:dst`

    const blockPromise = client1.brPopLPush(src, dst, 5)
    await waitForPark()
    await client2.rPush(src, 'world')

    assert.strictEqual(await blockPromise, 'world')
    assert.deepStrictEqual(await client1.lRange(dst, 0, -1), ['world'])
    assert.strictEqual(await client1.exists(src), 0)
  })

  test('BRPOPLPUSH timeout returns null', async () => {
    const tag = `{${randomKey()}}`
    const src = `${tag}:src`
    const dst = `${tag}:dst`

    assert.strictEqual(await client1.brPopLPush(src, dst, 0.1), null)
    assert.strictEqual(await client1.exists(dst), 0)
  })

  test('BRPOPLPUSH rotates a list onto itself', async () => {
    const key = `{${randomKey()}}:list`
    await client1.rPush(key, ['1', '2', '3'])

    assert.strictEqual(await client1.brPopLPush(key, key, 1), '3')
    assert.deepStrictEqual(await client1.lRange(key, 0, -1), ['3', '1', '2'])
  })

  test('BRPOPLPUSH on a key of another type is a WRONGTYPE error', async () => {
    const tag = `{${randomKey()}}`
    const src = `${tag}:src`
    const stringKey = `${tag}:string`
    await client1.rPush(src, 'x')
    await client1.set(stringKey, 'value')

    await assert.rejects(
      () => client1.brPopLPush(stringKey, src, 1),
      errorWithMessage(WRONGTYPE),
    )
    await assert.rejects(
      () => client1.brPopLPush(src, stringKey, 1),
      errorWithMessage(WRONGTYPE),
    )
    assert.deepStrictEqual(await client1.lRange(src, 0, -1), ['x'])
  })
})
