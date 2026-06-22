import { test, describe, before, after } from 'node:test'
import assert from 'node:assert'
import { Cluster } from 'ioredis'
import { TestRunner } from '../../test-config'
import { connectToSlotOwner, errorWithMessage, randomKey } from '../../utils'

const testRunner = new TestRunner()

function waitForPark(ms = 80): Promise<void> {
  return new Promise(resolve => setTimeout(resolve, ms))
}

describe(`BZPOPMIN / BZPOPMAX Integration (${testRunner.getBackendName()})`, () => {
  let client1: Cluster | undefined
  let client2: Cluster | undefined

  before(async () => {
    client1 = await testRunner.setupIoredisCluster()
    client2 = await testRunner.setupIoredisCluster()
  })

  after(async () => {
    await testRunner.cleanup()
  })

  test('BZPOPMIN pops the lowest-score member from the first non-empty key', async () => {
    const tag = `{${randomKey()}}`
    const empty = `${tag}:empty`
    const first = `${tag}:first`
    const second = `${tag}:second`

    try {
      await client1!.del(empty, first, second)
      await client1!.zadd(first, 1, 'a', 2, 'b', 3, 'c')
      await client1!.zadd(second, 10, 'x')

      assert.deepStrictEqual(
        await client1!.call('BZPOPMIN', empty, first, second, '0'),
        [first, 'a', '1'],
      )
      assert.deepStrictEqual(await client1!.zrange(first, 0, -1), ['b', 'c'])
      assert.deepStrictEqual(await client1!.zrange(second, 0, -1), ['x'])
    } finally {
      await client1!.del(empty, first, second)
    }
  })

  test('BZPOPMAX pops the highest-score member and deletes the set when empty', async () => {
    const tag = `{${randomKey()}}`
    const zset = `${tag}:zset`

    try {
      await client1!.del(zset)
      await client1!.zadd(zset, 1, 'a', 2, 'b')

      assert.deepStrictEqual(await client1!.call('BZPOPMAX', zset, '0'), [
        zset,
        'b',
        '2',
      ])
      assert.deepStrictEqual(await client1!.call('BZPOPMAX', zset, '0'), [
        zset,
        'a',
        '1',
      ])
      assert.strictEqual(await client1!.exists(zset), 0)
    } finally {
      await client1!.del(zset)
    }
  })

  test('BZPOPMIN times out and returns null when all keys are empty', async () => {
    const tag = `{${randomKey()}}`
    const first = `${tag}:first`
    const second = `${tag}:second`

    try {
      await client1!.del(first, second)
      assert.strictEqual(
        await client1!.call('BZPOPMIN', first, second, '0.1'),
        null,
      )
    } finally {
      await client1!.del(first, second)
    }
  })

  test('BZPOPMIN blocks then returns when a zadd arrives on a watched key', async () => {
    const tag = `{${randomKey()}}`
    const first = `${tag}:first`
    const second = `${tag}:second`

    try {
      await client1!.del(first, second)

      const blockPromise = client1!.call('BZPOPMIN', first, second, '5')
      await waitForPark()
      await client2!.zadd(second, 7, 'hello')

      assert.deepStrictEqual(await blockPromise, [second, 'hello', '7'])
      assert.strictEqual(await client1!.exists(second), 0)
    } finally {
      await client1!.del(first, second)
    }
  })

  test('BZPOPMAX blocks then returns the highest-score member when a zadd arrives', async () => {
    const tag = `{${randomKey()}}`
    const zset = `${tag}:zset`

    try {
      await client1!.del(zset)

      const blockPromise = client1!.call('BZPOPMAX', zset, '5')
      await waitForPark()
      await client2!.zadd(zset, 1, 'low', 9, 'high')

      assert.deepStrictEqual(await blockPromise, [zset, 'high', '9'])
      assert.deepStrictEqual(await client1!.zrange(zset, 0, -1), ['low'])
    } finally {
      await client1!.del(zset)
    }
  })

  test('BZPOPMIN / BZPOPMAX error paths match Redis', async () => {
    const tag = `{${randomKey()}}`
    const zset = `${tag}:zset`
    const stringKey = `${tag}:string`

    try {
      await client1!.del(zset, stringKey)
      await client1!.zadd(zset, 1, 'value')
      await client1!.set(stringKey, 'not-a-zset')

      await assert.rejects(
        () => client1!.call('BZPOPMIN'),
        errorWithMessage(
          "ERR wrong number of arguments for 'bzpopmin' command",
        ),
      )
      await assert.rejects(
        () => client1!.call('BZPOPMIN', zset),
        errorWithMessage(
          "ERR wrong number of arguments for 'bzpopmin' command",
        ),
      )
      await assert.rejects(
        () => client1!.call('BZPOPMAX'),
        errorWithMessage(
          "ERR wrong number of arguments for 'bzpopmax' command",
        ),
      )
      await assert.rejects(
        () => client1!.call('BZPOPMIN', zset, '-1'),
        errorWithMessage('ERR timeout is negative'),
      )
      await assert.rejects(
        () => client1!.call('BZPOPMIN', zset, 'abc'),
        errorWithMessage('ERR timeout is not a float or out of range'),
      )
      await assert.rejects(
        () => client1!.call('BZPOPMIN', stringKey, '0.1'),
        errorWithMessage(
          'WRONGTYPE Operation against a key holding the wrong kind of value',
        ),
      )

      const directClient = await connectToSlotOwner(client1!, zset)
      try {
        await assert.rejects(
          () => directClient.call('BZPOPMIN', zset, 'other-slot-key', '1'),
          errorWithMessage(
            "CROSSSLOT Keys in request don't hash to the same slot",
          ),
        )
        await assert.rejects(
          () => directClient.call('BZPOPMAX', zset, 'other-slot-key', '1'),
          errorWithMessage(
            "CROSSSLOT Keys in request don't hash to the same slot",
          ),
        )
      } finally {
        directClient.disconnect()
      }
    } finally {
      await client1!.del(zset, stringKey)
    }
  })
})
