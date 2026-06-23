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

describe(`LPOS / LMOVE Integration (node-redis, ${testRunner.getBackendName()})`, () => {
  let redisClient: RedisClusterType

  before(async () => {
    redisClient = (await testRunner.setupNodeRedisCluster()) as RedisClusterType
    await flushNodeRedisCluster(redisClient)
  })

  after(async () => {
    await testRunner.cleanup()
  })

  test('LPOS finds matches with RANK, COUNT and MAXLEN', async () => {
    const tag = `{lpos:${randomKey()}}`
    const key = `${tag}:list`
    const client = await connectToNodeRedisSlotOwner(redisClient, key)

    try {
      await client.del(key)
      // indexes: 0:a 1:b 2:c 3:a 4:b 5:c 6:a
      await client.rPush(key, ['a', 'b', 'c', 'a', 'b', 'c', 'a'])

      // default: first match scanning head -> tail
      assert.strictEqual(await client.lPos(key, 'a'), 0)

      // RANK 2 -> second match
      assert.strictEqual(await client.lPos(key, 'a', { RANK: 2 }), 3)

      // negative RANK -> first match scanning tail -> head (absolute index)
      assert.strictEqual(await client.lPos(key, 'a', { RANK: -1 }), 6)

      // COUNT 0 -> all matches as array, ascending
      assert.deepStrictEqual(await client.lPosCount(key, 'a', 0), [0, 3, 6])

      // COUNT N -> first N matches
      assert.deepStrictEqual(await client.lPosCount(key, 'a', 2), [0, 3])

      // negative RANK + COUNT -> scan from tail, descending indexes
      assert.deepStrictEqual(
        await client.lPosCount(key, 'a', 2, { RANK: -1 }),
        [6, 3],
      )

      // MAXLEN limits elements scanned from head; only index 0,1 examined
      assert.deepStrictEqual(
        await client.lPosCount(key, 'a', 0, { MAXLEN: 2 }),
        [0],
      )

      // MAXLEN with negative RANK limits elements scanned from tail
      assert.deepStrictEqual(
        await client.lPosCount(key, 'a', 0, { RANK: -1, MAXLEN: 2 }),
        [6],
      )

      // missing element, no COUNT -> nil
      assert.strictEqual(await client.lPos(key, 'z'), null)

      // missing element, COUNT 0 -> empty array
      assert.deepStrictEqual(await client.lPosCount(key, 'z', 0), [])

      // nonexistent key, no COUNT -> nil
      assert.strictEqual(await client.lPos(`${tag}:missing`, 'a'), null)

      // nonexistent key, COUNT 0 -> empty array
      assert.deepStrictEqual(
        await client.lPosCount(`${tag}:missing`, 'a', 0),
        [],
      )
    } finally {
      await client.del(key)
      client.destroy()
    }
  })

  test('LPOS error and edge paths match Redis', async () => {
    const tag = `{lpos-err:${randomKey()}}`
    const key = `${tag}:list`
    const stringKey = `${tag}:string`
    const client = await connectToNodeRedisSlotOwner(redisClient, key)

    try {
      await client.del(key)
      await client.rPush(key, ['a', 'b', 'a'])

      await assert.rejects(
        () => client.lPos(key, 'a', { RANK: 0 }),
        errorWithMessage(
          "ERR RANK can't be zero: use 1 to start from the first match, 2 from the second ... or use negative to start from the end of the list",
        ),
      )

      await assert.rejects(
        () => client.lPosCount(key, 'a', -1),
        errorWithMessage("ERR COUNT can't be negative"),
      )

      await assert.rejects(
        () => client.sendCommand(['LPOS', key, 'a', 'MAXLEN', '-1']),
        errorWithMessage("ERR MAXLEN can't be negative"),
      )

      await assert.rejects(
        () => client.sendCommand(['LPOS', key, 'a', 'RANK', 'x']),
        errorWithMessage('ERR value is not an integer or out of range'),
      )

      await assert.rejects(
        () => client.sendCommand(['LPOS', key]),
        errorWithMessage("ERR wrong number of arguments for 'lpos' command"),
      )

      await client.set(stringKey, 'value')
      await assert.rejects(
        () => client.lPos(stringKey, 'a'),
        errorWithMessage(
          'WRONGTYPE Operation against a key holding the wrong kind of value',
        ),
      )
    } finally {
      await client.del([key, stringKey])
      client.destroy()
    }
  })

  test('LMOVE moves elements between list ends', async () => {
    const tag = `{lmove:${randomKey()}}`
    const src = `${tag}:src`
    const dst = `${tag}:dst`
    const client = await connectToNodeRedisSlotOwner(redisClient, src)

    try {
      await client.del([src, dst])
      await client.rPush(src, ['a', 'b', 'c']) // [a,b,c]

      // pop left of src ('a'), push right of dst
      assert.strictEqual(await client.lMove(src, dst, 'LEFT', 'RIGHT'), 'a')
      assert.deepStrictEqual(await client.lRange(src, 0, -1), ['b', 'c'])
      assert.deepStrictEqual(await client.lRange(dst, 0, -1), ['a'])

      // pop right of src ('c'), push left of dst
      assert.strictEqual(await client.lMove(src, dst, 'RIGHT', 'LEFT'), 'c')
      assert.deepStrictEqual(await client.lRange(src, 0, -1), ['b'])
      assert.deepStrictEqual(await client.lRange(dst, 0, -1), ['c', 'a'])
    } finally {
      await client.del([src, dst])
      client.destroy()
    }
  })

  test('LMOVE rotates a list onto itself', async () => {
    const tag = `{lmove-rot:${randomKey()}}`
    const key = `${tag}:list`
    const client = await connectToNodeRedisSlotOwner(redisClient, key)

    try {
      await client.del(key)
      await client.rPush(key, ['1', '2', '3'])

      // pop right ('3'), push left -> [3,1,2]
      assert.strictEqual(await client.lMove(key, key, 'RIGHT', 'LEFT'), '3')
      assert.deepStrictEqual(await client.lRange(key, 0, -1), ['3', '1', '2'])
    } finally {
      await client.del(key)
      client.destroy()
    }
  })

  test('LMOVE on missing source returns nil and deletes emptied source', async () => {
    const tag = `{lmove-empty:${randomKey()}}`
    const src = `${tag}:src`
    const dst = `${tag}:dst`
    const client = await connectToNodeRedisSlotOwner(redisClient, src)

    try {
      await client.del([src, dst])

      // missing source -> nil, no destination created
      assert.strictEqual(await client.lMove(src, dst, 'LEFT', 'RIGHT'), null)
      assert.strictEqual(await client.exists(dst), 0)

      // moving the only element deletes the now-empty source key
      await client.rPush(src, 'only')
      assert.strictEqual(await client.lMove(src, dst, 'LEFT', 'RIGHT'), 'only')
      assert.strictEqual(await client.exists(src), 0)
      assert.deepStrictEqual(await client.lRange(dst, 0, -1), ['only'])
    } finally {
      await client.del([src, dst])
      client.destroy()
    }
  })

  test('LMOVE error paths match Redis', async () => {
    const tag = `{lmove-err:${randomKey()}}`
    const src = `${tag}:src`
    const dst = `${tag}:dst`
    const stringKey = `${tag}:string`
    const client = await connectToNodeRedisSlotOwner(redisClient, src)

    try {
      await client.del([src, dst, stringKey])
      await client.rPush(src, 'x')

      // invalid direction keyword -> syntax error
      await assert.rejects(
        () => client.sendCommand(['LMOVE', src, dst, 'UP', 'LEFT']),
        errorWithMessage('ERR syntax error'),
      )

      // too few arguments
      await assert.rejects(
        () => client.sendCommand(['LMOVE', src, dst, 'LEFT']),
        errorWithMessage("ERR wrong number of arguments for 'lmove' command"),
      )

      // wrong type source
      await client.set(stringKey, 'value')
      await assert.rejects(
        () => client.sendCommand(['LMOVE', stringKey, dst, 'LEFT', 'RIGHT']),
        errorWithMessage(
          'WRONGTYPE Operation against a key holding the wrong kind of value',
        ),
      )

      // wrong type destination -> source left unchanged
      await assert.rejects(
        () => client.sendCommand(['LMOVE', src, stringKey, 'LEFT', 'RIGHT']),
        errorWithMessage(
          'WRONGTYPE Operation against a key holding the wrong kind of value',
        ),
      )
      assert.deepStrictEqual(await client.lRange(src, 0, -1), ['x'])
    } finally {
      await client.del([src, dst, stringKey])
      client.destroy()
    }
  })
})
