import { test, describe, before, after } from 'node:test'
import assert from 'node:assert'
import { RedisClusterType } from 'redis'
import { TestRunner } from '../../test-config'
import {
  connectToNodeRedisSlotOwner,
  errorWithMessage,
  flushNodeRedisCluster,
  randomKey,
} from '../../utils'

const testRunner = new TestRunner()

function waitForPark(ms = 80): Promise<void> {
  return new Promise(resolve => setTimeout(resolve, ms))
}

describe(`BZPOPMIN / BZPOPMAX Integration (node-redis, ${testRunner.getBackendName()})`, () => {
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

  test('BZPOPMIN pops the lowest-score member from the first non-empty key', async () => {
    const tag = `{${randomKey()}}`
    const empty = `${tag}:empty`
    const first = `${tag}:first`
    const second = `${tag}:second`

    try {
      await client1.del([empty, first, second])
      await client1.zAdd(first, [
        { score: 1, value: 'a' },
        { score: 2, value: 'b' },
        { score: 3, value: 'c' },
      ])
      await client1.zAdd(second, { score: 10, value: 'x' })

      assert.deepStrictEqual(
        await client1.bzPopMin([empty, first, second], 0),
        {
          key: first,
          value: 'a',
          score: 1,
        },
      )
      assert.deepStrictEqual(await client1.zRange(first, 0, -1), ['b', 'c'])
      assert.deepStrictEqual(await client1.zRange(second, 0, -1), ['x'])
    } finally {
      await client1.del([empty, first, second])
    }
  })

  test('BZPOPMAX pops the highest-score member and deletes the set when empty', async () => {
    const tag = `{${randomKey()}}`
    const zset = `${tag}:zset`

    try {
      await client1.del(zset)
      await client1.zAdd(zset, [
        { score: 1, value: 'a' },
        { score: 2, value: 'b' },
      ])

      assert.deepStrictEqual(await client1.bzPopMax(zset, 0), {
        key: zset,
        value: 'b',
        score: 2,
      })
      assert.deepStrictEqual(await client1.bzPopMax(zset, 0), {
        key: zset,
        value: 'a',
        score: 1,
      })
      assert.strictEqual(await client1.exists(zset), 0)
    } finally {
      await client1.del(zset)
    }
  })

  test('BZPOPMIN times out and returns null when all keys are empty', async () => {
    const tag = `{${randomKey()}}`
    const first = `${tag}:first`
    const second = `${tag}:second`

    try {
      await client1.del([first, second])
      assert.strictEqual(await client1.bzPopMin([first, second], 0.1), null)
    } finally {
      await client1.del([first, second])
    }
  })

  test('BZPOPMIN blocks then returns when a zadd arrives on a watched key', async () => {
    const tag = `{${randomKey()}}`
    const first = `${tag}:first`
    const second = `${tag}:second`

    try {
      await client1.del([first, second])

      const blockPromise = client1.bzPopMin([first, second], 5)
      await waitForPark()
      await client2.zAdd(second, { score: 7, value: 'hello' })

      assert.deepStrictEqual(await blockPromise, {
        key: second,
        value: 'hello',
        score: 7,
      })
      assert.strictEqual(await client1.exists(second), 0)
    } finally {
      await client1.del([first, second])
    }
  })

  test('BZPOPMAX blocks then returns the highest-score member when a zadd arrives', async () => {
    const tag = `{${randomKey()}}`
    const zset = `${tag}:zset`

    try {
      await client1.del(zset)

      const blockPromise = client1.bzPopMax(zset, 5)
      await waitForPark()
      await client2.zAdd(zset, [
        { score: 1, value: 'low' },
        { score: 9, value: 'high' },
      ])

      assert.deepStrictEqual(await blockPromise, {
        key: zset,
        value: 'high',
        score: 9,
      })
      assert.deepStrictEqual(await client1.zRange(zset, 0, -1), ['low'])
    } finally {
      await client1.del(zset)
    }
  })

  test('BZPOPMIN / BZPOPMAX error paths match Redis', async () => {
    const tag = `{${randomKey()}}`
    const zset = `${tag}:zset`
    const stringKey = `${tag}:string`
    const send = (args: string[]) => client1.sendCommand(zset, false, args)

    try {
      await client1.del([zset, stringKey])
      await client1.zAdd(zset, { score: 1, value: 'value' })
      await client1.set(stringKey, 'not-a-zset')

      await assert.rejects(
        () => client1.sendCommand(undefined, false, ['BZPOPMIN']),
        errorWithMessage(
          "ERR wrong number of arguments for 'bzpopmin' command",
        ),
      )
      await assert.rejects(
        () => send(['BZPOPMIN', zset]),
        errorWithMessage(
          "ERR wrong number of arguments for 'bzpopmin' command",
        ),
      )
      await assert.rejects(
        () => client1.sendCommand(undefined, false, ['BZPOPMAX']),
        errorWithMessage(
          "ERR wrong number of arguments for 'bzpopmax' command",
        ),
      )
      await assert.rejects(
        () => send(['BZPOPMIN', zset, '-1']),
        errorWithMessage('ERR timeout is negative'),
      )
      await assert.rejects(
        () => send(['BZPOPMIN', zset, 'abc']),
        errorWithMessage('ERR timeout is not a float or out of range'),
      )
      await assert.rejects(
        () =>
          client1.sendCommand(stringKey, false, ['BZPOPMIN', stringKey, '0.1']),
        errorWithMessage(
          'WRONGTYPE Operation against a key holding the wrong kind of value',
        ),
      )

      const directClient = await connectToNodeRedisSlotOwner(client1, zset)
      try {
        await assert.rejects(
          () =>
            directClient.sendCommand(['BZPOPMIN', zset, 'other-slot-key', '1']),
          errorWithMessage(
            "CROSSSLOT Keys in request don't hash to the same slot",
          ),
        )
        await assert.rejects(
          () =>
            directClient.sendCommand(['BZPOPMAX', zset, 'other-slot-key', '1']),
          errorWithMessage(
            "CROSSSLOT Keys in request don't hash to the same slot",
          ),
        )
      } finally {
        directClient.destroy()
      }
    } finally {
      await client1.del([zset, stringKey])
    }
  })
})
