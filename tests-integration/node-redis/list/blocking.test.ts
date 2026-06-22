import { test, describe, before, after } from 'node:test'
import assert from 'node:assert'
import { RedisClusterType } from 'redis'
import { TestRunner } from '../../test-config'
import { flushNodeRedisCluster, randomKey } from '../../utils'

const testRunner = new TestRunner()

// Give a blocking command time to park before the waker fires.
function waitForPark(ms = 80): Promise<void> {
  return new Promise(resolve => setTimeout(resolve, ms))
}

describe(`Blocking Commands Integration (node-redis, ${testRunner.getBackendName()})`, () => {
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

  test('BLPOP returns immediately when list is non-empty', async () => {
    const key = randomKey()
    await client1.lPush(key, 'hello')
    assert.deepStrictEqual(await client1.blPop(key, 1), {
      key,
      element: 'hello',
    })
  })

  test('BLPOP blocks then returns when LPUSH arrives', async () => {
    const key = randomKey()
    const blockPromise = client1.blPop(key, 5)
    await waitForPark()
    await client2.lPush(key, 'world')
    assert.deepStrictEqual(await blockPromise, { key, element: 'world' })
  })

  test('BLPOP timeout returns null', async () => {
    const key = randomKey()
    assert.strictEqual(await client1.blPop(key, 0.1), null)
  })

  test('BRPOP returns immediately when list is non-empty', async () => {
    const key = randomKey()
    await client1.rPush(key, ['first', 'last'])
    assert.deepStrictEqual(await client1.brPop(key, 1), {
      key,
      element: 'last',
    })
  })

  test('BRPOP blocks then returns when RPUSH arrives', async () => {
    const key = randomKey()
    const blockPromise = client1.brPop(key, 5)
    await waitForPark()
    await client2.rPush(key, 'pushed')
    assert.deepStrictEqual(await blockPromise, { key, element: 'pushed' })
  })

  test('BRPOP timeout returns null', async () => {
    const key = randomKey()
    assert.strictEqual(await client1.brPop(key, 0.1), null)
  })

  test('XREAD BLOCK returns immediately when entries exist after given id', async () => {
    const key = randomKey()
    await client1.xAdd(key, '1-1', { f: 'v' })
    const result = await client1.xRead({ key, id: '0-0' }, { BLOCK: 1000 })
    assert.ok(result !== null)
    assert.strictEqual(result.length, 1)
    assert.strictEqual(result[0].name, key)
  })

  test('XREAD BLOCK with $ blocks then returns when new entry arrives', async () => {
    const key = randomKey()
    const blockPromise = client1.xRead({ key, id: '$' }, { BLOCK: 5000 })
    await waitForPark()
    await client2.xAdd(key, '*', { field: 'value' })
    const result = await blockPromise
    assert.ok(result !== null)
    assert.strictEqual(result.length, 1)
    assert.strictEqual(result[0].name, key)
  })

  test('XREAD BLOCK timeout returns null', async () => {
    const key = randomKey()
    assert.strictEqual(
      await client1.xRead({ key, id: '$' }, { BLOCK: 100 }),
      null,
    )
  })

  test('XREAD BLOCK with COUNT limits returned entries', async () => {
    const key = randomKey()
    const blockPromise = client1.xRead(
      { key, id: '$' },
      { BLOCK: 5000, COUNT: 1 },
    )
    await waitForPark()
    await client2.xAdd(key, '1-1', { f: 'v1' })
    await client2.xAdd(key, '2-1', { f: 'v2' })
    const result = await blockPromise
    assert.ok(result !== null)
    assert.strictEqual(result[0].messages.length, 1)
  })

  test('XREAD COUNT is per-stream: each stream returns up to COUNT entries independently', async () => {
    const base = randomKey()
    const keyA = `{${base}}:a`
    const keyB = `{${base}}:b`
    await client1.xAdd(keyA, '1-1', { f: 'v' })
    await client1.xAdd(keyA, '2-1', { f: 'v' })
    await client1.xAdd(keyA, '3-1', { f: 'v' })
    await client1.xAdd(keyB, '1-1', { f: 'v' })
    await client1.xAdd(keyB, '2-1', { f: 'v' })
    await client1.xAdd(keyB, '3-1', { f: 'v' })

    const result = await client1.xRead(
      [
        { key: keyA, id: '0-0' },
        { key: keyB, id: '0-0' },
      ],
      { COUNT: 2 },
    )
    assert.ok(result !== null)
    assert.strictEqual(result.length, 2, 'both streams returned')
    assert.strictEqual(
      result[0].messages.length,
      2,
      'COUNT=2 applied to stream A',
    )
    assert.strictEqual(
      result[1].messages.length,
      2,
      'COUNT=2 applied to stream B',
    )
  })

  test('BLPOP thundering herd: single push delivers to exactly one of two concurrent waiters', async () => {
    const key = randomKey()

    const block1 = client1.blPop(key, 2)
    const block2 = client2.blPop(key, 2)
    await waitForPark(120)

    const client3 =
      (await testRunner.setupNodeRedisCluster()) as RedisClusterType
    await client3.lPush(key, 'prize')

    const [r1, r2] = await Promise.all([block1, block2])

    const winner = [r1, r2].filter(r => r !== null)
    const loser = [r1, r2].filter(r => r === null)
    assert.strictEqual(winner.length, 1, 'exactly one waiter wins')
    assert.strictEqual(loser.length, 1, 'exactly one waiter gets null')
    assert.deepStrictEqual(winner[0], { key, element: 'prize' })
  })
})
