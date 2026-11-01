import { test, describe, before, after } from 'node:test'
import assert from 'node:assert'
import { Cluster } from 'ioredis'
import { TestRunner } from '../../test-config'
import { randomKey } from '../../utils'

const testRunner = new TestRunner()

// Give a blocking command time to park before the waker fires.
function waitForPark(ms = 80): Promise<void> {
  return new Promise(resolve => setTimeout(resolve, ms))
}

// BRPOPLPUSH is the deprecated form of BLMOVE source destination RIGHT LEFT
// (#495).
describe(`BRPOPLPUSH Integration (${testRunner.getBackendName()})`, () => {
  let client1: Cluster | undefined
  let client2: Cluster | undefined

  before(async () => {
    client1 = await testRunner.setupIoredisCluster()
    client2 = await testRunner.setupIoredisCluster()
  })

  after(async () => {
    await testRunner.cleanup()
  })

  test('BRPOPLPUSH returns immediately when source is non-empty', async () => {
    const tag = `{${randomKey()}}`
    const src = `${tag}:src`
    const dst = `${tag}:dst`

    await client1!.rpush(src, 'a', 'b', 'c')
    await client1!.rpush(dst, 'x')

    // pop RIGHT of src ('c'), push LEFT of dst
    assert.strictEqual(await client1!.brpoplpush(src, dst, 1), 'c')
    assert.deepStrictEqual(await client1!.lrange(src, 0, -1), ['a', 'b'])
    assert.deepStrictEqual(await client1!.lrange(dst, 0, -1), ['c', 'x'])
  })

  test('BRPOPLPUSH blocks then returns when a push arrives on source', async () => {
    const tag = `{${randomKey()}}`
    const src = `${tag}:src`
    const dst = `${tag}:dst`

    const blockPromise = client1!.brpoplpush(src, dst, 5)
    await waitForPark()
    await client2!.rpush(src, 'world')

    assert.strictEqual(await blockPromise, 'world')
    assert.deepStrictEqual(await client1!.lrange(dst, 0, -1), ['world'])
    assert.strictEqual(await client1!.exists(src), 0)
  })

  test('BRPOPLPUSH timeout returns null', async () => {
    const tag = `{${randomKey()}}`
    const src = `${tag}:src`
    const dst = `${tag}:dst`

    assert.strictEqual(await client1!.brpoplpush(src, dst, 0.1), null)
    // a timed-out BRPOPLPUSH must not create the destination
    assert.strictEqual(await client1!.exists(dst), 0)
  })

  test('BRPOPLPUSH rotates a list onto itself', async () => {
    const key = `{${randomKey()}}:list`
    await client1!.rpush(key, '1', '2', '3')

    assert.strictEqual(await client1!.brpoplpush(key, key, 1), '3')
    assert.deepStrictEqual(await client1!.lrange(key, 0, -1), ['3', '1', '2'])
  })

  test('BRPOPLPUSH on a key of another type is a WRONGTYPE error', async () => {
    const tag = `{${randomKey()}}`
    const src = `${tag}:src`
    const stringKey = `${tag}:string`
    await client1!.rpush(src, 'x')
    await client1!.set(stringKey, 'value')

    await assert.rejects(
      () => client1!.brpoplpush(stringKey, src, 1),
      /^ReplyError: WRONGTYPE /,
    )
    await assert.rejects(
      () => client1!.brpoplpush(src, stringKey, 1),
      /^ReplyError: WRONGTYPE /,
    )
    assert.deepStrictEqual(await client1!.lrange(src, 0, -1), ['x'])
  })
})
