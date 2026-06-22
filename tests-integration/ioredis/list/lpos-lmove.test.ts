import { test, describe, before, after } from 'node:test'
import assert from 'node:assert'
import { Cluster } from 'ioredis'
import { TestRunner } from '../../test-config'
import { connectToSlotOwner, errorWithMessage, randomKey } from '../../utils'

const testRunner = new TestRunner()

describe(`LPOS / LMOVE Integration (${testRunner.getBackendName()})`, () => {
  let redisClient: Cluster | undefined

  before(async () => {
    redisClient = await testRunner.setupIoredisCluster('lpos-lmove-integration')
  })

  after(async () => {
    await testRunner.cleanup()
  })

  test('LPOS finds matches with RANK, COUNT and MAXLEN', async () => {
    const tag = `{lpos:${randomKey()}}`
    const key = `${tag}:list`
    const client = await connectToSlotOwner(redisClient!, key)

    try {
      await client.del(key)
      // indexes: 0:a 1:b 2:c 3:a 4:b 5:c 6:a
      await client.rpush(key, 'a', 'b', 'c', 'a', 'b', 'c', 'a')

      // default: first match scanning head -> tail
      assert.strictEqual(await client.lpos(key, 'a'), 0)

      // RANK 2 -> second match
      assert.strictEqual(await client.lpos(key, 'a', 'RANK', '2'), 3)

      // negative RANK -> first match scanning tail -> head (absolute index)
      assert.strictEqual(await client.lpos(key, 'a', 'RANK', '-1'), 6)

      // COUNT 0 -> all matches as array, ascending
      assert.deepStrictEqual(
        await client.lpos(key, 'a', 'COUNT', '0'),
        [0, 3, 6],
      )

      // COUNT N -> first N matches
      assert.deepStrictEqual(await client.lpos(key, 'a', 'COUNT', '2'), [0, 3])

      // negative RANK + COUNT -> scan from tail, descending indexes
      assert.deepStrictEqual(
        await client.lpos(key, 'a', 'RANK', '-1', 'COUNT', '2'),
        [6, 3],
      )

      // MAXLEN limits elements scanned from head; only index 0,1 examined
      assert.deepStrictEqual(
        await client.lpos(key, 'a', 'MAXLEN', '2', 'COUNT', '0'),
        [0],
      )

      // MAXLEN with negative RANK limits elements scanned from tail
      assert.deepStrictEqual(
        await client.lpos(key, 'a', 'RANK', '-1', 'MAXLEN', '2', 'COUNT', '0'),
        [6],
      )

      // missing element, no COUNT -> nil
      assert.strictEqual(await client.lpos(key, 'z'), null)

      // missing element, COUNT 0 -> empty array
      assert.deepStrictEqual(await client.lpos(key, 'z', 'COUNT', '0'), [])

      // nonexistent key, no COUNT -> nil
      assert.strictEqual(await client.lpos(`${tag}:missing`, 'a'), null)

      // nonexistent key, COUNT 0 -> empty array
      assert.deepStrictEqual(
        await client.lpos(`${tag}:missing`, 'a', 'COUNT', '0'),
        [],
      )
    } finally {
      await client.del(key)
      client.disconnect()
    }
  })

  test('LPOS error and edge paths match Redis', async () => {
    const tag = `{lpos-err:${randomKey()}}`
    const key = `${tag}:list`
    const stringKey = `${tag}:string`
    const client = await connectToSlotOwner(redisClient!, key)

    try {
      await client.del(key)
      await client.rpush(key, 'a', 'b', 'a')

      await assert.rejects(
        () => client.lpos(key, 'a', 'RANK', '0'),
        errorWithMessage(
          "ERR RANK can't be zero: use 1 to start from the first match, 2 from the second ... or use negative to start from the end of the list",
        ),
      )

      await assert.rejects(
        () => client.lpos(key, 'a', 'COUNT', '-1'),
        errorWithMessage("ERR COUNT can't be negative"),
      )

      await assert.rejects(
        () => client.call('LPOS', key, 'a', 'MAXLEN', '-1'),
        errorWithMessage("ERR MAXLEN can't be negative"),
      )

      await assert.rejects(
        () => client.call('LPOS', key, 'a', 'RANK', 'x'),
        errorWithMessage('ERR value is not an integer or out of range'),
      )

      await assert.rejects(
        () => client.call('LPOS', key),
        errorWithMessage("ERR wrong number of arguments for 'lpos' command"),
      )

      await client.set(stringKey, 'value')
      await assert.rejects(
        () => client.lpos(stringKey, 'a'),
        errorWithMessage(
          'WRONGTYPE Operation against a key holding the wrong kind of value',
        ),
      )
    } finally {
      await client.del(key, stringKey)
      client.disconnect()
    }
  })

  test('LMOVE moves elements between list ends', async () => {
    const tag = `{lmove:${randomKey()}}`
    const src = `${tag}:src`
    const dst = `${tag}:dst`
    const client = await connectToSlotOwner(redisClient!, src)

    try {
      await client.del(src, dst)
      await client.rpush(src, 'a', 'b', 'c') // [a,b,c]

      // pop left of src ('a'), push right of dst
      assert.strictEqual(await client.lmove(src, dst, 'LEFT', 'RIGHT'), 'a')
      assert.deepStrictEqual(await client.lrange(src, 0, -1), ['b', 'c'])
      assert.deepStrictEqual(await client.lrange(dst, 0, -1), ['a'])

      // pop right of src ('c'), push left of dst
      assert.strictEqual(await client.lmove(src, dst, 'RIGHT', 'LEFT'), 'c')
      assert.deepStrictEqual(await client.lrange(src, 0, -1), ['b'])
      assert.deepStrictEqual(await client.lrange(dst, 0, -1), ['c', 'a'])
    } finally {
      await client.del(src, dst)
      client.disconnect()
    }
  })

  test('LMOVE rotates a list onto itself', async () => {
    const tag = `{lmove-rot:${randomKey()}}`
    const key = `${tag}:list`
    const client = await connectToSlotOwner(redisClient!, key)

    try {
      await client.del(key)
      await client.rpush(key, '1', '2', '3')

      // pop right ('3'), push left -> [3,1,2]
      assert.strictEqual(await client.lmove(key, key, 'RIGHT', 'LEFT'), '3')
      assert.deepStrictEqual(await client.lrange(key, 0, -1), ['3', '1', '2'])
    } finally {
      await client.del(key)
      client.disconnect()
    }
  })

  test('LMOVE on missing source returns nil and deletes emptied source', async () => {
    const tag = `{lmove-empty:${randomKey()}}`
    const src = `${tag}:src`
    const dst = `${tag}:dst`
    const client = await connectToSlotOwner(redisClient!, src)

    try {
      await client.del(src, dst)

      // missing source -> nil, no destination created
      assert.strictEqual(await client.lmove(src, dst, 'LEFT', 'RIGHT'), null)
      assert.strictEqual(await client.exists(dst), 0)

      // moving the only element deletes the now-empty source key
      await client.rpush(src, 'only')
      assert.strictEqual(await client.lmove(src, dst, 'LEFT', 'RIGHT'), 'only')
      assert.strictEqual(await client.exists(src), 0)
      assert.deepStrictEqual(await client.lrange(dst, 0, -1), ['only'])
    } finally {
      await client.del(src, dst)
      client.disconnect()
    }
  })

  test('LMOVE error paths match Redis', async () => {
    const tag = `{lmove-err:${randomKey()}}`
    const src = `${tag}:src`
    const dst = `${tag}:dst`
    const stringKey = `${tag}:string`
    const client = await connectToSlotOwner(redisClient!, src)

    try {
      await client.del(src, dst, stringKey)
      await client.rpush(src, 'x')

      // invalid direction keyword -> syntax error
      await assert.rejects(
        () => client.call('LMOVE', src, dst, 'UP', 'LEFT'),
        errorWithMessage('ERR syntax error'),
      )

      // too few arguments
      await assert.rejects(
        () => client.call('LMOVE', src, dst, 'LEFT'),
        errorWithMessage("ERR wrong number of arguments for 'lmove' command"),
      )

      // wrong type source
      await client.set(stringKey, 'value')
      await assert.rejects(
        () => client.lmove(stringKey, dst, 'LEFT', 'RIGHT'),
        errorWithMessage(
          'WRONGTYPE Operation against a key holding the wrong kind of value',
        ),
      )

      // wrong type destination -> source left unchanged
      await assert.rejects(
        () => client.lmove(src, stringKey, 'LEFT', 'RIGHT'),
        errorWithMessage(
          'WRONGTYPE Operation against a key holding the wrong kind of value',
        ),
      )
      assert.deepStrictEqual(await client.lrange(src, 0, -1), ['x'])
    } finally {
      await client.del(src, dst, stringKey)
      client.disconnect()
    }
  })
})
