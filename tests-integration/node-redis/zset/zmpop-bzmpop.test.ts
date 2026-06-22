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

describe(`ZMPOP / BZMPOP Integration (node-redis, ${testRunner.getBackendName()})`, () => {
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

  test('ZMPOP pops from the first non-empty key with the requested side and count', async () => {
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
      await client1.zAdd(second, [
        { score: 10, value: 'x' },
        { score: 20, value: 'y' },
      ])

      assert.deepStrictEqual(
        await client1.zmPop([empty, first, second], 'MIN', { COUNT: 2 }),
        {
          key: first,
          members: [
            { value: 'a', score: 1 },
            { value: 'b', score: 2 },
          ],
        },
      )
      assert.deepStrictEqual(await client1.zRange(first, 0, -1), ['c'])
      assert.deepStrictEqual(await client1.zRange(second, 0, -1), ['x', 'y'])

      assert.deepStrictEqual(await client1.zmPop([first, second], 'MAX'), {
        key: first,
        members: [{ value: 'c', score: 3 }],
      })
      assert.strictEqual(await client1.exists(first), 0)

      assert.deepStrictEqual(
        await client1.zmPop([first, second], 'MAX', { COUNT: 5 }),
        {
          key: second,
          members: [
            { value: 'y', score: 20 },
            { value: 'x', score: 10 },
          ],
        },
      )
      assert.strictEqual(await client1.exists(second), 0)
    } finally {
      await client1.del([empty, first, second])
    }
  })

  test('ZMPOP returns null when all keys are empty or missing', async () => {
    const tag = `{${randomKey()}}`
    const first = `${tag}:first`
    const second = `${tag}:second`

    try {
      await client1.del([first, second])
      assert.strictEqual(await client1.zmPop([first, second], 'MIN'), null)
    } finally {
      await client1.del([first, second])
    }
  })

  test('BZMPOP returns immediately when a sorted set is non-empty', async () => {
    const tag = `{${randomKey()}}`
    const empty = `${tag}:empty`
    const zset = `${tag}:zset`

    try {
      await client1.del([empty, zset])
      await client1.zAdd(zset, [
        { score: 1, value: 'one' },
        { score: 2, value: 'two' },
        { score: 3, value: 'three' },
      ])

      assert.deepStrictEqual(
        await client1.bzmPop(1, [empty, zset], 'MAX', { COUNT: 2 }),
        {
          key: zset,
          members: [
            { value: 'three', score: 3 },
            { value: 'two', score: 2 },
          ],
        },
      )
      assert.deepStrictEqual(await client1.zRange(zset, 0, -1), ['one'])
    } finally {
      await client1.del([empty, zset])
    }
  })

  test('BZMPOP blocks then returns when a zset write arrives', async () => {
    const tag = `{${randomKey()}}`
    const first = `${tag}:first`
    const second = `${tag}:second`

    try {
      await client1.del([first, second])

      const blockPromise = client1.bzmPop(5, [first, second], 'MIN', {
        COUNT: 2,
      })
      await waitForPark()
      await client2.zAdd(second, [
        { score: 1, value: 'a' },
        { score: 2, value: 'b' },
      ])

      assert.deepStrictEqual(await blockPromise, {
        key: second,
        members: [
          { value: 'a', score: 1 },
          { value: 'b', score: 2 },
        ],
      })
      assert.strictEqual(await client1.exists(second), 0)
    } finally {
      await client1.del([first, second])
    }
  })

  test('BZMPOP timeout returns null', async () => {
    const tag = `{${randomKey()}}`
    const first = `${tag}:first`
    const second = `${tag}:second`

    try {
      await client1.del([first, second])
      assert.strictEqual(
        await client1.bzmPop(0.1, [first, second], 'MIN'),
        null,
      )
    } finally {
      await client1.del([first, second])
    }
  })

  test('ZMPOP / BZMPOP error paths match Redis', async () => {
    const tag = `{${randomKey()}}`
    const zset = `${tag}:zset`
    const other = `${tag}:other`
    const stringKey = `${tag}:string`
    const send = (args: string[]) => client1.sendCommand(zset, false, args)

    try {
      await client1.del([zset, other, stringKey])
      await client1.zAdd(zset, { score: 1, value: 'value' })
      await client1.set(stringKey, 'not-a-zset')

      await assert.rejects(
        () => client1.sendCommand(undefined, false, ['ZMPOP']),
        errorWithMessage("ERR wrong number of arguments for 'zmpop' command"),
      )
      await assert.rejects(
        () => send(['ZMPOP', '0', zset, 'MIN']),
        errorWithMessage('ERR numkeys should be greater than 0'),
      )
      await assert.rejects(
        () => send(['ZMPOP', 'abc', zset, 'MIN']),
        errorWithMessage('ERR numkeys should be greater than 0'),
      )
      await assert.rejects(
        () => send(['ZMPOP', '2', zset, other]),
        errorWithMessage('ERR syntax error'),
      )
      await assert.rejects(
        () => send(['ZMPOP', '1', zset, 'MIDDLE']),
        errorWithMessage('ERR syntax error'),
      )
      await assert.rejects(
        () => send(['ZMPOP', '1', zset, 'MIN', 'LIMIT', '1']),
        errorWithMessage('ERR syntax error'),
      )
      await assert.rejects(
        () => send(['ZMPOP', '1', zset, 'MIN', 'COUNT']),
        errorWithMessage('ERR syntax error'),
      )
      await assert.rejects(
        () => send(['ZMPOP', '1', zset, 'MIN', 'COUNT', '0']),
        errorWithMessage('ERR count should be greater than 0'),
      )
      await assert.rejects(
        () => send(['ZMPOP', '1', zset, 'MIN', 'COUNT', '-1']),
        errorWithMessage('ERR count should be greater than 0'),
      )
      await assert.rejects(
        () => send(['ZMPOP', '1', zset, 'MIN', 'COUNT', 'abc']),
        errorWithMessage('ERR count should be greater than 0'),
      )
      await assert.rejects(
        () => send(['ZMPOP', '1', zset, 'MIN', 'COUNT', '1', 'COUNT', '1']),
        errorWithMessage('ERR syntax error'),
      )
      await assert.rejects(
        () =>
          client1.sendCommand(stringKey, false, [
            'ZMPOP',
            '1',
            stringKey,
            'MIN',
          ]),
        errorWithMessage(
          'WRONGTYPE Operation against a key holding the wrong kind of value',
        ),
      )

      await assert.rejects(
        () => send(['BZMPOP', '-1', '1', zset, 'MIN']),
        errorWithMessage('ERR timeout is negative'),
      )
      await assert.rejects(
        () => send(['BZMPOP', 'abc', '1', zset, 'MIN']),
        errorWithMessage('ERR timeout is not a float or out of range'),
      )

      const directClient = await connectToNodeRedisSlotOwner(client1, zset)
      try {
        await assert.rejects(
          () =>
            directClient.sendCommand([
              'ZMPOP',
              '2',
              zset,
              'other-slot-key',
              'MIN',
            ]),
          errorWithMessage(
            "CROSSSLOT Keys in request don't hash to the same slot",
          ),
        )
        await assert.rejects(
          () =>
            directClient.sendCommand([
              'BZMPOP',
              '1',
              '2',
              zset,
              'other-slot-key',
              'MIN',
            ]),
          errorWithMessage(
            "CROSSSLOT Keys in request don't hash to the same slot",
          ),
        )
      } finally {
        directClient.destroy()
      }
    } finally {
      await client1.del([zset, other, stringKey])
    }
  })
})
