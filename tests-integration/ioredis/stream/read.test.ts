import { test, describe, before, after } from 'node:test'
import assert from 'node:assert'
import { Cluster } from 'ioredis'
import { TestRunner } from '../../test-config'
import {
  commandFrame,
  connectToSlotOwner,
  findSlotOwner,
  randomKey,
} from '../../utils'
import {
  RawRedisConnection,
  respMapGet,
  respText,
} from '../../raw-tcp/raw-connection'

const testRunner = new TestRunner()

describe(`Stream Commands Integration (${testRunner.getBackendName()})`, () => {
  let redisClient: Cluster | undefined

  before(async () => {
    redisClient = await testRunner.setupIoredisCluster('stream-integration')
  })

  after(async () => {
    await testRunner.cleanup()
  })

  // XTRIM

  // XADD with NOMKSTREAM and MAXLEN options

  // XREAD

  // Stream consumer groups

  test('XREAD returns entries after the given id', async () => {
    const key = randomKey()
    const node = await connectToSlotOwner(redisClient!, key)
    try {
      await node.xadd(key, '1-1', 'f', '1')
      await node.xadd(key, '2-1', 'f', '2')
      await node.xadd(key, '3-1', 'f', '3')

      const result = (await node.xread('STREAMS', key, '1-1')) as [
        string,
        [string, string[]][],
      ][]
      assert.ok(Array.isArray(result))
      assert.strictEqual(result.length, 1)
      assert.strictEqual(result[0][0], key)
      assert.deepStrictEqual(result[0][1], [
        ['2-1', ['f', '2']],
        ['3-1', ['f', '3']],
      ])
    } finally {
      node.disconnect()
    }
  })

  test('XREAD returns a RESP3 map after HELLO 3', async () => {
    assert.ok(redisClient)

    const key = `{stream-resp3:${randomKey()}}:stream`
    const [host, port] = await findSlotOwner(redisClient, key)
    const connection = await RawRedisConnection.connect(host, port)

    try {
      connection.write(commandFrame('HELLO', '3'))
      assert.ok((await connection.readFrame()) instanceof Map)

      connection.write(commandFrame('XADD', key, '*', 'field1', 'value1'))
      assert.match(respText(await connection.readFrame()), /^\d+-\d+$/)

      connection.write(commandFrame('XREAD', 'STREAMS', key, '0-0'))
      const reply = await connection.readFrame()
      assert.ok(reply instanceof Map)

      const entries = respMapGet(reply, key)
      assert.ok(Array.isArray(entries))
      assert.strictEqual(entries.length, 1)

      const entry = entries[0]
      assert.ok(Array.isArray(entry))
      assert.strictEqual(entry.length, 2)
      assert.match(respText(entry[0]), /^\d+-\d+$/)
      assert.deepStrictEqual(
        (entry[1] as unknown[]).map(value => respText(value)),
        ['field1', 'value1'],
      )
    } finally {
      connection.close()
      await redisClient.del(key)
    }
  })

  test('XREAD COUNT limits the number of returned entries', async () => {
    const key = randomKey()
    const node = await connectToSlotOwner(redisClient!, key)
    try {
      await node.xadd(key, '1-1', 'f', '1')
      await node.xadd(key, '2-1', 'f', '2')
      await node.xadd(key, '3-1', 'f', '3')

      const result = (await node.xread('COUNT', 1, 'STREAMS', key, '0-0')) as [
        string,
        [string, string[]][],
      ][]
      assert.ok(Array.isArray(result))
      assert.strictEqual(result[0][1].length, 1)
      assert.strictEqual(result[0][1][0][0], '1-1')
    } finally {
      node.disconnect()
    }
  })

  test('XREAD returns null when no new entries exist for the given id', async () => {
    const key = randomKey()
    const node = await connectToSlotOwner(redisClient!, key)
    try {
      await node.xadd(key, '1-1', 'f', '1')
      const result = await node.xread('STREAMS', key, '1-1')
      assert.strictEqual(result, null)
    } finally {
      node.disconnect()
    }
  })

  test('XREAD with $ id returns null (no entries after current last)', async () => {
    const key = randomKey()
    const node = await connectToSlotOwner(redisClient!, key)
    try {
      await node.xadd(key, '1-1', 'f', '1')
      const result = await node.xread('STREAMS', key, '$')
      assert.strictEqual(result, null)
    } finally {
      node.disconnect()
    }
  })

  test('XREAD on a missing key returns null', async () => {
    const key = randomKey()
    const node = await connectToSlotOwner(redisClient!, key)
    try {
      const result = await node.xread('STREAMS', key, '0-0')
      assert.strictEqual(result, null)
    } finally {
      node.disconnect()
    }
  })

  test('XREAD from multiple streams on same slot returns combined results', async () => {
    // Use hashtag to pin both keys to the same cluster slot.
    const tag = Math.random().toString(36).substring(2, 8)
    const key1 = `{${tag}}s1`
    const key2 = `{${tag}}s2`
    const node = await connectToSlotOwner(redisClient!, key1)
    try {
      await node.xadd(key1, '1-1', 'a', '1')
      await node.xadd(key2, '2-1', 'b', '2')

      const result = (await node.xread(
        'STREAMS',
        key1,
        key2,
        '0-0',
        '0-0',
      )) as [string, [string, string[]][]][]
      assert.ok(Array.isArray(result))
      assert.strictEqual(result.length, 2)
    } finally {
      node.disconnect()
    }
  })
})
