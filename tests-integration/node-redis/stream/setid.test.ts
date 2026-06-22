import { test, describe, before, after } from 'node:test'
import assert from 'node:assert'
import { RedisClientType, RedisClusterType } from 'redis'
import { TestRunner } from '../../test-config'
import {
  connectToNodeRedisSlotOwner,
  errorWithMessage,
  flushNodeRedisCluster,
  randomKey,
} from '../../utils'

const testRunner = new TestRunner()

// XINFO replies are RESP3 maps (objects) on node-redis; access raw via
// sendCommand so the same kebab-case keys work on mock and real backends.
type StreamInfo = Record<string, unknown>

describe(`Stream Commands Integration (node-redis, ${testRunner.getBackendName()})`, () => {
  let redisClient: RedisClusterType

  before(async () => {
    redisClient = (await testRunner.setupNodeRedisCluster()) as RedisClusterType
    await flushNodeRedisCluster(redisClient)
  })

  after(async () => {
    await testRunner.cleanup()
  })

  function xinfo(c: RedisClientType, args: string[]): Promise<StreamInfo> {
    return c.sendCommand(args) as Promise<StreamInfo>
  }

  // XTRIM

  // XADD with NOMKSTREAM and MAXLEN options

  // XREAD

  // Stream consumer groups

  test('XSETID sets last-generated-id and advances generated XADD ids', async () => {
    const key = `{xsetid:${randomKey()}}`
    const node = await connectToNodeRedisSlotOwner(redisClient, key)
    const futureMs = BigInt(Date.now()) + 60_000n

    try {
      await node.xAdd(key, '1-0', { f: 'v' })
      assert.strictEqual(await node.xSetId(key, `${futureMs}-0`), 'OK')

      const streamInfo = await xinfo(node, ['XINFO', 'STREAM', key])
      assert.strictEqual(streamInfo['length'], 1)
      assert.strictEqual(streamInfo['last-generated-id'], `${futureMs}-0`)
      assert.strictEqual(streamInfo['entries-added'], 1)

      assert.strictEqual(await node.xAdd(key, '*', { f: 'v' }), `${futureMs}-1`)
    } finally {
      await node.del(key)
      node.destroy()
    }
  })

  test('XSETID ENTRIESADDED updates XINFO STREAM metadata', async () => {
    const key = `{xsetid-meta:${randomKey()}}`
    const node = await connectToNodeRedisSlotOwner(redisClient, key)

    try {
      await node.xAdd(key, '1-0', { f: 'v' })
      assert.strictEqual(
        await node.xSetId(key, '5-0', { ENTRIESADDED: 42 }),
        'OK',
      )

      const streamInfo = await xinfo(node, ['XINFO', 'STREAM', key])
      assert.strictEqual(streamInfo['last-generated-id'], '5-0')
      assert.strictEqual(streamInfo['entries-added'], 42)
      assert.strictEqual(await node.xAdd(key, '5-*', { f: 'next' }), '5-1')

      const updatedInfo = await xinfo(node, ['XINFO', 'STREAM', key])
      assert.strictEqual(updatedInfo['entries-added'], 43)
    } finally {
      await node.del(key)
      node.destroy()
    }
  })

  test('XSETID MAXDELETEDID updates XINFO STREAM metadata', async () => {
    const key = `{xsetid-maxdeleted:${randomKey()}}`
    const node = await connectToNodeRedisSlotOwner(redisClient, key)

    try {
      await node.xAdd(key, '1-0', { f: 'v' })
      assert.strictEqual(
        await node.xSetId(key, '5-0', {
          MAXDELETEDID: '2-0',
          ENTRIESADDED: 42,
        }),
        'OK',
      )

      const streamInfo = await xinfo(node, ['XINFO', 'STREAM', key])
      assert.strictEqual(streamInfo['last-generated-id'], '5-0')
      assert.strictEqual(streamInfo['max-deleted-entry-id'], '2-0')
      assert.strictEqual(streamInfo['entries-added'], 42)

      // Duplicate options: last one wins (raw command form).
      assert.strictEqual(
        await node.sendCommand([
          'XSETID',
          key,
          '6-0',
          'ENTRIESADDED',
          '7',
          'MAXDELETEDID',
          '3-0',
          'ENTRIESADDED',
          '9',
          'MAXDELETEDID',
          '4-0',
        ]),
        'OK',
      )

      const duplicateInfo = await xinfo(node, ['XINFO', 'STREAM', key])
      assert.strictEqual(duplicateInfo['last-generated-id'], '6-0')
      assert.strictEqual(duplicateInfo['max-deleted-entry-id'], '4-0')
      assert.strictEqual(duplicateInfo['entries-added'], 9)
    } finally {
      await node.del(key)
      node.destroy()
    }
  })

  test('XSETID rejects lower ids, invalid options, and wrong types', async () => {
    const tag = `{xsetid-errors:${randomKey()}}`
    const key = `${tag}:stream`
    const stringKey = `${tag}:string`
    const node = await connectToNodeRedisSlotOwner(redisClient, key)

    try {
      await node.xAdd(key, '5-0', { f: 'v' })
      await node.set(stringKey, 'not-a-stream')

      await assert.rejects(
        () => node.xSetId(`${tag}:missing`, '1-0'),
        errorWithMessage('ERR no such key'),
      )
      await assert.rejects(
        () => node.xSetId(key, '4-0'),
        errorWithMessage(
          'ERR The ID specified in XSETID is smaller than the target stream top item',
        ),
      )
      await assert.rejects(
        () => node.xSetId(key, 'not-an-id'),
        errorWithMessage(
          'ERR Invalid stream ID specified as stream command argument',
        ),
      )
      await assert.rejects(
        () => node.sendCommand(['XSETID', key, '6-0', 'ENTRIESADDED', 'nope']),
        errorWithMessage('ERR value is not an integer or out of range'),
      )
      await assert.rejects(
        () => node.sendCommand(['XSETID', key, '6-0', 'ENTRIESADDED']),
        errorWithMessage('ERR syntax error'),
      )
      await assert.rejects(
        () =>
          node.sendCommand(['XSETID', key, '6-0', 'MAXDELETEDID', 'bad-id']),
        errorWithMessage(
          'ERR Invalid stream ID specified as stream command argument',
        ),
      )
      await assert.rejects(
        () => node.sendCommand(['XSETID', key, '6-0', 'MAXDELETEDID']),
        errorWithMessage('ERR syntax error'),
      )
      await assert.rejects(
        () => node.sendCommand(['XSETID', key, '6-0', 'BOGUS', '1']),
        errorWithMessage('ERR syntax error'),
      )
      await assert.rejects(
        () => node.xSetId(stringKey, '6-0'),
        errorWithMessage(
          'WRONGTYPE Operation against a key holding the wrong kind of value',
        ),
      )
    } finally {
      await node.del([key, stringKey])
      node.destroy()
    }
  })
})
