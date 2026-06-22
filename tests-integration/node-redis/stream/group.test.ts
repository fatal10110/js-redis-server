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

  test('XGROUP creates, mutates, and destroys consumer groups', async () => {
    const key = randomKey()
    const node = await connectToNodeRedisSlotOwner(redisClient, key)
    try {
      await node.xAdd(key, '1-0', { f: '1' })
      await node.xAdd(key, '2-0', { f: '2' })

      assert.strictEqual(await node.xGroupCreate(key, 'workers', '0'), 'OK')
      assert.strictEqual(
        await node.xGroupCreateConsumer(key, 'workers', 'alice'),
        1,
      )
      assert.strictEqual(
        await node.xGroupCreateConsumer(key, 'workers', 'alice'),
        0,
      )

      const read = await node.xReadGroup(
        'workers',
        'alice',
        { key, id: '>' },
        { COUNT: 2 },
      )
      assert.deepStrictEqual(read, [
        {
          name: key,
          messages: [
            { id: '1-0', message: { f: '1' } },
            { id: '2-0', message: { f: '2' } },
          ],
        },
      ])

      const pendingSummary = await node.xPending(key, 'workers')
      assert.strictEqual(pendingSummary.pending, 2)
      assert.strictEqual(pendingSummary.firstId, '1-0')
      assert.strictEqual(pendingSummary.lastId, '2-0')
      assert.ok(Array.isArray(pendingSummary.consumers))

      assert.strictEqual(await node.xAck(key, 'workers', '1-0'), 1)
      const pendingDetails = await node.xPendingRange(
        key,
        'workers',
        '-',
        '+',
        10,
      )
      assert.strictEqual(pendingDetails.length, 1)
      assert.strictEqual(pendingDetails[0].id, '2-0')
      assert.strictEqual(pendingDetails[0].consumer, 'alice')
      assert.strictEqual(pendingDetails[0].deliveriesCounter, 1)

      const consumers = await node.xInfoConsumers(key, 'workers')
      const alice = consumers.find(item => item.name === 'alice')
      assert.ok(alice)
      assert.strictEqual(alice.pending, 1)

      assert.strictEqual(
        await node.xGroupDelConsumer(key, 'workers', 'alice'),
        1,
      )
      assert.strictEqual(await node.xGroupDestroy(key, 'workers'), 1)
      assert.strictEqual(await node.xGroupDestroy(key, 'workers'), 0)
    } finally {
      node.destroy()
    }
  })

  test('XGROUP MKSTREAM and SETID control group delivery position', async () => {
    const key = randomKey()
    const node = await connectToNodeRedisSlotOwner(redisClient, key)
    try {
      assert.strictEqual(
        await node.xGroupCreate(key, 'workers', '$', { MKSTREAM: true }),
        'OK',
      )
      assert.strictEqual(await node.xLen(key), 0)

      await node.xAdd(key, '1-0', { f: '1' })
      await node.xAdd(key, '2-0', { f: '2' })
      assert.deepStrictEqual(
        await node.xReadGroup('workers', 'alice', { key, id: '>' }),
        [
          {
            name: key,
            messages: [
              { id: '1-0', message: { f: '1' } },
              { id: '2-0', message: { f: '2' } },
            ],
          },
        ],
      )

      assert.strictEqual(await node.xGroupSetId(key, 'workers', '$'), 'OK')
      await node.xAdd(key, '3-0', { f: '3' })
      assert.deepStrictEqual(
        await node.xReadGroup('workers', 'bob', { key, id: '>' }),
        [{ name: key, messages: [{ id: '3-0', message: { f: '3' } }] }],
      )
    } finally {
      node.destroy()
    }
  })

  test('XCLAIM and XAUTOCLAIM transfer pending stream entries', async () => {
    const key = randomKey()
    const node = await connectToNodeRedisSlotOwner(redisClient, key)
    try {
      await node.xAdd(key, '1-0', { f: '1' })
      await node.xAdd(key, '2-0', { f: '2' })
      await node.xGroupCreate(key, 'workers', '0')
      await node.xReadGroup('workers', 'alice', { key, id: '>' }, { COUNT: 2 })

      assert.deepStrictEqual(
        await node.xClaim(key, 'workers', 'bob', 0, '1-0'),
        [{ id: '1-0', message: { f: '1' } }],
      )

      const claimed = await node.xAutoClaim(key, 'workers', 'carol', 0, '0-0', {
        COUNT: 10,
      })
      assert.strictEqual(claimed.nextId, '0-0')
      assert.deepStrictEqual(
        claimed.messages.map(entry => entry.id),
        ['1-0', '2-0'],
      )
      assert.deepStrictEqual(claimed.deletedMessages, [])

      const pendingDetails = await node.xPendingRange(
        key,
        'workers',
        '-',
        '+',
        10,
      )
      assert.deepStrictEqual(
        pendingDetails.map(item => [item.id, item.consumer]),
        [
          ['1-0', 'carol'],
          ['2-0', 'carol'],
        ],
      )
    } finally {
      node.destroy()
    }
  })

  test('XREADGROUP history keeps deleted pending entries visible', async () => {
    const key = randomKey()
    const node = await connectToNodeRedisSlotOwner(redisClient, key)
    try {
      await node.xAdd(key, '1-1', { f: '1' })
      await node.xAdd(key, '2-2', { f: '2' })
      await node.xGroupCreate(key, 'workers', '0')
      await node.xReadGroup('workers', 'alice', { key, id: '>' }, { COUNT: 10 })
      assert.strictEqual(await node.xDel(key, '1-1'), 1)

      // A deleted PEL entry comes back with a nil message; node-redis' typed
      // xReadGroup transform throws on that, so read the raw reply instead.
      const history = (await node.sendCommand([
        'XREADGROUP',
        'GROUP',
        'workers',
        'alice',
        'STREAMS',
        key,
        '0',
      ])) as Record<string, unknown>
      assert.deepStrictEqual(history[key], [
        ['1-1', null],
        ['2-2', ['f', '2']],
      ])
    } finally {
      node.destroy()
    }
  })

  test('XREADGROUP history returns an empty per-key list for consumers with no pending entries', async () => {
    const key = randomKey()
    const node = await connectToNodeRedisSlotOwner(redisClient, key)
    try {
      await node.xAdd(key, '1-1', { f: '1' })
      await node.xGroupCreate(key, 'workers', '0')

      assert.deepStrictEqual(
        await node.xReadGroup('workers', 'alice', { key, id: '0' }),
        [{ name: key, messages: [] }],
      )
    } finally {
      node.destroy()
    }
  })

  test('XINFO reports stream, group, and consumer metadata', async () => {
    const key = randomKey()
    const node = await connectToNodeRedisSlotOwner(redisClient, key)
    try {
      await node.xAdd(key, '1-0', { f: '1' })
      await node.xAdd(key, '2-0', { f: '2' })
      await node.xGroupCreate(key, 'workers', '0')
      await node.xReadGroup('workers', 'alice', { key, id: '>' }, { COUNT: 1 })

      const streamInfo = await xinfo(node, [
        'XINFO',
        'STREAM',
        key,
        'FULL',
        'COUNT',
        '1',
      ])
      assert.strictEqual(streamInfo['length'], 2)
      assert.strictEqual(streamInfo['last-generated-id'], '2-0')
      assert.deepStrictEqual(streamInfo['entries'], [['1-0', ['f', '1']]])
      assert.ok(Array.isArray(streamInfo['groups']))

      const groupsInfo = await node.xInfoGroups(key)
      assert.strictEqual(groupsInfo.length, 1)
      assert.strictEqual(groupsInfo[0].name, 'workers')
      assert.strictEqual(groupsInfo[0].consumers, 1)
      assert.strictEqual(groupsInfo[0].pending, 1)

      const consumersInfo = await node.xInfoConsumers(key, 'workers')
      assert.strictEqual(consumersInfo.length, 1)
      assert.strictEqual(consumersInfo[0].name, 'alice')
      assert.strictEqual(consumersInfo[0].pending, 1)
    } finally {
      node.destroy()
    }
  })

  test('XINFO STREAM FULL defaults to 10 stream entries and PEL rows', async () => {
    const key = randomKey()
    const node = await connectToNodeRedisSlotOwner(redisClient, key)
    try {
      for (let i = 1; i <= 12; i++) {
        await node.xAdd(key, `${i}-0`, { f: `${i}` })
      }
      await node.xGroupCreate(key, 'workers', '0')
      await node.xReadGroup('workers', 'alice', { key, id: '>' }, { COUNT: 12 })

      const streamInfo = await xinfo(node, ['XINFO', 'STREAM', key, 'FULL'])
      const entries = streamInfo['entries'] as unknown[][]
      assert.strictEqual(entries.length, 10)
      assert.strictEqual(entries[0][0], '1-0')
      assert.strictEqual(entries[9][0], '10-0')

      const groups = streamInfo['groups'] as StreamInfo[]
      const pending = groups[0]['pending'] as unknown[][]
      assert.strictEqual(pending.length, 10)
      assert.strictEqual(pending[0][0], '1-0')
      assert.strictEqual(pending[9][0], '10-0')
    } finally {
      node.destroy()
    }
  })

  test('stream consumer group commands report Redis-compatible errors', async () => {
    const tag = randomKey()
    const key = `{${tag}}:stream`
    const node = await connectToNodeRedisSlotOwner(redisClient, key)
    try {
      await assert.rejects(
        () => node.xGroupCreate(key, 'workers', '0'),
        errorWithMessage(
          'ERR The XGROUP subcommand requires the key to exist. Note that for CREATE you may want to use the MKSTREAM option to create an empty stream automatically.',
        ),
      )

      await node.xAdd(key, '1-0', { f: '1' })
      assert.strictEqual(await node.xGroupCreate(key, 'workers', '0'), 'OK')
      await assert.rejects(
        () => node.xGroupCreate(key, 'workers', '0'),
        errorWithMessage('BUSYGROUP Consumer Group name already exists'),
      )
      await assert.rejects(
        () => node.xReadGroup('missing', 'alice', { key, id: '>' }),
        errorWithMessage(
          `NOGROUP No such key '${key}' or consumer group 'missing' in XREADGROUP with GROUP option`,
        ),
      )

      const stringKey = `{${tag}}:string`
      await node.set(stringKey, 'value')
      await assert.rejects(
        () => node.xInfoGroups(stringKey),
        errorWithMessage(
          'WRONGTYPE Operation against a key holding the wrong kind of value',
        ),
      )
    } finally {
      node.destroy()
    }
  })
})
