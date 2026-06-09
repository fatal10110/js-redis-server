import { test, describe, before, after } from 'node:test'
import assert from 'node:assert'
import { Cluster } from 'ioredis'
import { TestRunner } from '../test-config'
import { randomKey } from '../utils'

const testRunner = new TestRunner()

// Give a blocking command time to park before the waker fires.
function waitForPark(ms = 80): Promise<void> {
  return new Promise(resolve => setTimeout(resolve, ms))
}

describe(`Blocking Commands Integration (${testRunner.getBackendName()})`, () => {
  let client1: Cluster | undefined
  let client2: Cluster | undefined

  before(async () => {
    // No keyPrefix so BLPOP/BRPOP responses return the bare key (ioredis doesn't
    // strip keyPrefix from response key names, causing assertion mismatches).
    // randomKey() provides per-test isolation without a shared prefix.
    client1 = await testRunner.setupIoredisCluster()
    client2 = await testRunner.setupIoredisCluster()
  })

  after(async () => {
    await testRunner.cleanup()
  })

  test('BLPOP returns immediately when list is non-empty', async () => {
    const key = randomKey()
    await client1!.lpush(key, 'hello')
    const result = await client1!.blpop(key, 1)
    assert.deepStrictEqual(result, [key, 'hello'])
  })

  test('BLPOP blocks then returns when LPUSH arrives', async () => {
    const key = randomKey()
    const blockPromise = client1!.blpop(key, 5)
    await waitForPark()
    await client2!.lpush(key, 'world')
    const result = await blockPromise
    assert.deepStrictEqual(result, [key, 'world'])
  })

  test('BLPOP timeout returns null', async () => {
    const key = randomKey()
    const result = await client1!.blpop(key, 0.1)
    assert.strictEqual(result, null)
  })

  test('BRPOP returns immediately when list is non-empty', async () => {
    const key = randomKey()
    await client1!.rpush(key, 'first', 'last')
    const result = await client1!.brpop(key, 1)
    assert.deepStrictEqual(result, [key, 'last'])
  })

  test('BRPOP blocks then returns when RPUSH arrives', async () => {
    const key = randomKey()
    const blockPromise = client1!.brpop(key, 5)
    await waitForPark()
    await client2!.rpush(key, 'pushed')
    const result = await blockPromise
    assert.deepStrictEqual(result, [key, 'pushed'])
  })

  test('BRPOP timeout returns null', async () => {
    const key = randomKey()
    const result = await client1!.brpop(key, 0.1)
    assert.strictEqual(result, null)
  })

  test('XREAD BLOCK returns immediately when entries exist after given id', async () => {
    const key = randomKey()
    await client1!.xadd(key, '1-1', 'f', 'v')
    const result = await client1!.xread('BLOCK', 1000, 'STREAMS', key, '0-0')
    assert.ok(result !== null)
    assert.strictEqual(result!.length, 1)
    assert.strictEqual(result![0][0], key)
  })

  test('XREAD BLOCK with $ blocks then returns when new entry arrives', async () => {
    const key = randomKey()
    const blockPromise = client1!.xread('BLOCK', 5000, 'STREAMS', key, '$')
    await waitForPark()
    await client2!.xadd(key, '*', 'field', 'value')
    const result = await blockPromise
    assert.ok(result !== null)
    assert.strictEqual(result!.length, 1)
    assert.strictEqual(result![0][0], key)
  })

  test('XREAD BLOCK timeout returns null', async () => {
    const key = randomKey()
    const result = await client1!.xread('BLOCK', 100, 'STREAMS', key, '$')
    assert.strictEqual(result, null)
  })

  test('XREAD BLOCK with COUNT limits returned entries', async () => {
    const key = randomKey()
    const blockPromise = client1!.xread(
      'BLOCK',
      5000,
      'COUNT',
      1,
      'STREAMS',
      key,
      '$',
    )
    await waitForPark()
    await client2!.xadd(key, '1-1', 'f', 'v1')
    await client2!.xadd(key, '2-1', 'f', 'v2')
    const result = await blockPromise
    assert.ok(result !== null)
    assert.strictEqual(result![0][1].length, 1)
  })
})
