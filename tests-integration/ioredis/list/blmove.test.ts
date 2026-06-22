import { test, describe, before, after } from 'node:test'
import assert from 'node:assert'
import { Cluster } from 'ioredis'
import { TestRunner } from '../../test-config'
import { errorWithMessage, randomKey } from '../../utils'

const testRunner = new TestRunner()

// Give a blocking command time to park before the waker fires.
function waitForPark(ms = 80): Promise<void> {
  return new Promise(resolve => setTimeout(resolve, ms))
}

describe(`BLMOVE Integration (${testRunner.getBackendName()})`, () => {
  let client1: Cluster | undefined
  let client2: Cluster | undefined

  before(async () => {
    // No keyPrefix so raw key names round-trip; randomKey() isolates per test.
    client1 = await testRunner.setupIoredisCluster()
    client2 = await testRunner.setupIoredisCluster()
  })

  after(async () => {
    await testRunner.cleanup()
  })

  test('BLMOVE returns immediately when source is non-empty', async () => {
    const tag = `{${randomKey()}}`
    const src = `${tag}:src`
    const dst = `${tag}:dst`

    await client1!.del(src, dst)
    await client1!.rpush(src, 'a', 'b', 'c') // [a,b,c]

    // pop LEFT of src ('a'), push RIGHT of dst
    assert.strictEqual(
      await client1!.call('BLMOVE', src, dst, 'LEFT', 'RIGHT', '1'),
      'a',
    )
    assert.deepStrictEqual(await client1!.lrange(src, 0, -1), ['b', 'c'])
    assert.deepStrictEqual(await client1!.lrange(dst, 0, -1), ['a'])

    // pop RIGHT of src ('c'), push LEFT of dst
    assert.strictEqual(
      await client1!.call('BLMOVE', src, dst, 'RIGHT', 'LEFT', '1'),
      'c',
    )
    assert.deepStrictEqual(await client1!.lrange(src, 0, -1), ['b'])
    assert.deepStrictEqual(await client1!.lrange(dst, 0, -1), ['c', 'a'])
  })

  test('BLMOVE blocks then returns when a push arrives on source', async () => {
    const tag = `{${randomKey()}}`
    const src = `${tag}:src`
    const dst = `${tag}:dst`
    await client1!.del(src, dst)

    const blockPromise = client1!.call('BLMOVE', src, dst, 'LEFT', 'RIGHT', '5')
    await waitForPark()
    await client2!.rpush(src, 'world')

    assert.strictEqual(await blockPromise, 'world')
    assert.deepStrictEqual(await client1!.lrange(dst, 0, -1), ['world'])
    assert.strictEqual(await client1!.exists(src), 0)
  })

  test('BLMOVE timeout returns null', async () => {
    const tag = `{${randomKey()}}`
    const src = `${tag}:src`
    const dst = `${tag}:dst`
    await client1!.del(src, dst)

    const result = await client1!.call(
      'BLMOVE',
      src,
      dst,
      'LEFT',
      'RIGHT',
      '0.1',
    )
    assert.strictEqual(result, null)
    // timed-out BLMOVE must not create the destination
    assert.strictEqual(await client1!.exists(dst), 0)
  })

  test('BLMOVE rotates a list onto itself', async () => {
    const tag = `{${randomKey()}}`
    const key = `${tag}:list`
    await client1!.del(key)
    await client1!.rpush(key, '1', '2', '3')

    // pop RIGHT ('3'), push LEFT -> [3,1,2]
    assert.strictEqual(
      await client1!.call('BLMOVE', key, key, 'RIGHT', 'LEFT', '1'),
      '3',
    )
    assert.deepStrictEqual(await client1!.lrange(key, 0, -1), ['3', '1', '2'])
  })

  test('BLMOVE error paths match Redis', async () => {
    const tag = `{${randomKey()}}`
    const src = `${tag}:src`
    const dst = `${tag}:dst`
    const stringKey = `${tag}:string`
    await client1!.del(src, dst, stringKey)
    await client1!.rpush(src, 'x')

    // invalid direction keyword -> syntax error
    await assert.rejects(
      () => client1!.call('BLMOVE', src, dst, 'UP', 'LEFT', '1'),
      errorWithMessage('ERR syntax error'),
    )

    // too few arguments
    await assert.rejects(
      () => client1!.call('BLMOVE', src, dst, 'LEFT', 'RIGHT'),
      errorWithMessage("ERR wrong number of arguments for 'blmove' command"),
    )

    // negative timeout
    await assert.rejects(
      () => client1!.call('BLMOVE', src, dst, 'LEFT', 'RIGHT', '-1'),
      errorWithMessage('ERR timeout is negative'),
    )

    // non-float timeout
    await assert.rejects(
      () => client1!.call('BLMOVE', src, dst, 'LEFT', 'RIGHT', 'abc'),
      errorWithMessage('ERR timeout is not a float or out of range'),
    )

    // wrong type source
    await client1!.set(stringKey, 'value')
    await assert.rejects(
      () => client1!.call('BLMOVE', stringKey, dst, 'LEFT', 'RIGHT', '1'),
      errorWithMessage(
        'WRONGTYPE Operation against a key holding the wrong kind of value',
      ),
    )

    // wrong type destination -> source left unchanged
    await assert.rejects(
      () => client1!.call('BLMOVE', src, stringKey, 'LEFT', 'RIGHT', '1'),
      errorWithMessage(
        'WRONGTYPE Operation against a key holding the wrong kind of value',
      ),
    )
    assert.deepStrictEqual(await client1!.lrange(src, 0, -1), ['x'])
  })
})
