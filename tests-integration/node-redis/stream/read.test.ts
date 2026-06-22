import { test, describe, before, after } from 'node:test'
import assert from 'node:assert'
import { RedisClusterType } from 'redis'
import { TestRunner } from '../../test-config'
import {
  connectToNodeRedisSlotOwner,
  flushNodeRedisCluster,
  randomKey,
} from '../../utils'

const testRunner = new TestRunner()

// XINFO replies are RESP3 maps (objects) on node-redis; access raw via
// sendCommand so the same kebab-case keys work on mock and real backends.

describe(`Stream Commands Integration (node-redis, ${testRunner.getBackendName()})`, () => {
  let redisClient: RedisClusterType

  before(async () => {
    redisClient = (await testRunner.setupNodeRedisCluster()) as RedisClusterType
    await flushNodeRedisCluster(redisClient)
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
    const node = await connectToNodeRedisSlotOwner(redisClient, key)
    try {
      await node.xAdd(key, '1-1', { f: '1' })
      await node.xAdd(key, '2-1', { f: '2' })
      await node.xAdd(key, '3-1', { f: '3' })

      const result = await node.xRead({ key, id: '1-1' })
      assert.ok(result)
      assert.strictEqual(result.length, 1)
      assert.strictEqual(result[0].name, key)
      assert.deepStrictEqual(result[0].messages, [
        { id: '2-1', message: { f: '2' } },
        { id: '3-1', message: { f: '3' } },
      ])
    } finally {
      node.destroy()
    }
  })

  test('XREAD parses entries over a default RESP3 connection', async () => {
    const key = `{stream-resp3:${randomKey()}}:stream`
    const node = await connectToNodeRedisSlotOwner(redisClient, key)
    try {
      const id = await node.xAdd(key, '*', { field1: 'value1' })
      assert.match(id, /^\d+-\d+$/)

      const result = await node.xRead({ key, id: '0-0' })
      assert.ok(result)
      assert.strictEqual(result.length, 1)
      assert.strictEqual(result[0].name, key)
      assert.strictEqual(result[0].messages.length, 1)
      assert.strictEqual(result[0].messages[0].id, id)
      assert.deepStrictEqual(result[0].messages[0].message, {
        field1: 'value1',
      })
    } finally {
      await node.del(key)
      node.destroy()
    }
  })

  test('XREAD COUNT limits the number of returned entries', async () => {
    const key = randomKey()
    const node = await connectToNodeRedisSlotOwner(redisClient, key)
    try {
      await node.xAdd(key, '1-1', { f: '1' })
      await node.xAdd(key, '2-1', { f: '2' })
      await node.xAdd(key, '3-1', { f: '3' })

      const result = await node.xRead({ key, id: '0-0' }, { COUNT: 1 })
      assert.ok(result)
      assert.strictEqual(result[0].messages.length, 1)
      assert.strictEqual(result[0].messages[0].id, '1-1')
    } finally {
      node.destroy()
    }
  })

  test('XREAD returns null when no new entries exist for the given id', async () => {
    const key = randomKey()
    const node = await connectToNodeRedisSlotOwner(redisClient, key)
    try {
      await node.xAdd(key, '1-1', { f: '1' })
      assert.strictEqual(await node.xRead({ key, id: '1-1' }), null)
    } finally {
      node.destroy()
    }
  })

  test('XREAD with $ id returns null (no entries after current last)', async () => {
    const key = randomKey()
    const node = await connectToNodeRedisSlotOwner(redisClient, key)
    try {
      await node.xAdd(key, '1-1', { f: '1' })
      assert.strictEqual(await node.xRead({ key, id: '$' }), null)
    } finally {
      node.destroy()
    }
  })

  test('XREAD on a missing key returns null', async () => {
    const key = randomKey()
    const node = await connectToNodeRedisSlotOwner(redisClient, key)
    try {
      assert.strictEqual(await node.xRead({ key, id: '0-0' }), null)
    } finally {
      node.destroy()
    }
  })

  test('XREAD from multiple streams on same slot returns combined results', async () => {
    const tag = Math.random().toString(36).substring(2, 8)
    const key1 = `{${tag}}s1`
    const key2 = `{${tag}}s2`
    const node = await connectToNodeRedisSlotOwner(redisClient, key1)
    try {
      await node.xAdd(key1, '1-1', { a: '1' })
      await node.xAdd(key2, '2-1', { b: '2' })

      const result = await node.xRead([
        { key: key1, id: '0-0' },
        { key: key2, id: '0-0' },
      ])
      assert.ok(result)
      assert.strictEqual(result.length, 2)
    } finally {
      node.destroy()
    }
  })
})
