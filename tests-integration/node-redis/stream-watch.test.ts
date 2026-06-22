import { RedisClientType, RedisClusterType, WatchError } from 'redis'
import { after, before, describe, test } from 'node:test'
import assert from 'node:assert'
import { TestRunner } from '../test-config'
import { connectToNodeRedisSlotOwner, randomKey } from '../utils'

const testRunner = new TestRunner()

// Real Redis does NOT invalidate a WATCH on a stream key for consumer-group /
// pending-entry / last-id metadata changes. Only changes to the stream's entry
// set (XADD, XDEL, ...) touch the watched key. These tests pin that behavior so
// the tracked-stream helpers never re-introduce the spurious WATCH dirtying that
// the old unconditional forceWrite() produced.
describe('Stream metadata WATCH semantics (node-redis)', () => {
  let redisClient: RedisClusterType

  before(async () => {
    redisClient = (await testRunner.setupNodeRedisCluster()) as RedisClusterType
  })

  after(async () => {
    await testRunner.cleanup()
  })

  type Case = {
    name: string
    initialize(client: RedisClientType, key: string): Promise<unknown>
    mutate(client: RedisClientType, key: string): Promise<unknown>
  }

  // Every command below mutates consumer-group / pending / last-id metadata but
  // must leave the WATCH intact (the queued SET still runs -> EXEC === ['OK']).
  const cleanCases: Case[] = [
    {
      name: 'XSETID to a different id',
      initialize: (c, key) => c.sendCommand(['XADD', key, '5-5', 'f', 'v']),
      mutate: (c, key) => c.sendCommand(['XSETID', key, '9-9']),
    },
    {
      name: 'XGROUP SETID to a different id',
      initialize: async (c, key) => {
        await c.sendCommand(['XADD', key, '1-1', 'f', 'v'])
        await c.sendCommand(['XADD', key, '2-2', 'f', 'v'])
        await c.sendCommand(['XGROUP', 'CREATE', key, 'g', '0'])
      },
      mutate: (c, key) => c.sendCommand(['XGROUP', 'SETID', key, 'g', '2']),
    },
    {
      name: 'XREADGROUP > delivering new entries',
      initialize: async (c, key) => {
        await c.sendCommand(['XADD', key, '1-1', 'f', 'v'])
        await c.sendCommand(['XGROUP', 'CREATE', key, 'g', '0'])
      },
      mutate: (c, key) =>
        c.sendCommand([
          'XREADGROUP',
          'GROUP',
          'g',
          'c',
          'COUNT',
          '10',
          'STREAMS',
          key,
          '>',
        ]),
    },
    {
      name: 'XREADGROUP history read (explicit id)',
      initialize: async (c, key) => {
        await c.sendCommand(['XADD', key, '1-1', 'f', 'v'])
        await c.sendCommand(['XGROUP', 'CREATE', key, 'g', '0'])
        await c.sendCommand([
          'XREADGROUP',
          'GROUP',
          'g',
          'c',
          'STREAMS',
          key,
          '>',
        ])
      },
      mutate: (c, key) =>
        c.sendCommand(['XREADGROUP', 'GROUP', 'g', 'c', 'STREAMS', key, '0']),
    },
    {
      name: 'XREADGROUP > with no new entries',
      initialize: async (c, key) => {
        await c.sendCommand(['XADD', key, '1-1', 'f', 'v'])
        await c.sendCommand(['XGROUP', 'CREATE', key, 'g', '$'])
      },
      mutate: (c, key) =>
        c.sendCommand(['XREADGROUP', 'GROUP', 'g', 'c', 'STREAMS', key, '>']),
    },
    {
      name: 'XCLAIM that claims a pending entry',
      initialize: async (c, key) => {
        await c.sendCommand(['XADD', key, '1-1', 'f', 'v'])
        await c.sendCommand(['XGROUP', 'CREATE', key, 'g', '0'])
        await c.sendCommand([
          'XREADGROUP',
          'GROUP',
          'g',
          'c1',
          'STREAMS',
          key,
          '>',
        ])
      },
      mutate: (c, key) => c.sendCommand(['XCLAIM', key, 'g', 'c2', '0', '1-1']),
    },
    {
      name: 'XCLAIM that claims nothing',
      initialize: async (c, key) => {
        await c.sendCommand(['XADD', key, '1-1', 'f', 'v'])
        await c.sendCommand(['XGROUP', 'CREATE', key, 'g', '0'])
      },
      mutate: (c, key) => c.sendCommand(['XCLAIM', key, 'g', 'c2', '0', '9-9']),
    },
    {
      name: 'XAUTOCLAIM that claims a pending entry',
      initialize: async (c, key) => {
        await c.sendCommand(['XADD', key, '1-1', 'f', 'v'])
        await c.sendCommand(['XGROUP', 'CREATE', key, 'g', '0'])
        await c.sendCommand([
          'XREADGROUP',
          'GROUP',
          'g',
          'c1',
          'STREAMS',
          key,
          '>',
        ])
      },
      mutate: (c, key) =>
        c.sendCommand(['XAUTOCLAIM', key, 'g', 'c2', '0', '0']),
    },
    {
      name: 'XAUTOCLAIM that claims nothing',
      initialize: async (c, key) => {
        await c.sendCommand(['XADD', key, '1-1', 'f', 'v'])
        await c.sendCommand(['XGROUP', 'CREATE', key, 'g', '0'])
      },
      mutate: (c, key) =>
        c.sendCommand(['XAUTOCLAIM', key, 'g', 'c2', '0', '0']),
    },
    {
      name: 'XACK acknowledging a pending entry',
      initialize: async (c, key) => {
        await c.sendCommand(['XADD', key, '1-1', 'f', 'v'])
        await c.sendCommand(['XGROUP', 'CREATE', key, 'g', '0'])
        await c.sendCommand([
          'XREADGROUP',
          'GROUP',
          'g',
          'c',
          'STREAMS',
          key,
          '>',
        ])
      },
      mutate: (c, key) => c.sendCommand(['XACK', key, 'g', '1-1']),
    },
    {
      name: 'XGROUP CREATE on an existing stream',
      initialize: (c, key) => c.sendCommand(['XADD', key, '1-1', 'f', 'v']),
      mutate: (c, key) => c.sendCommand(['XGROUP', 'CREATE', key, 'g', '0']),
    },
    {
      name: 'XGROUP DESTROY removing a group',
      initialize: async (c, key) => {
        await c.sendCommand(['XADD', key, '1-1', 'f', 'v'])
        await c.sendCommand(['XGROUP', 'CREATE', key, 'g', '0'])
      },
      mutate: (c, key) => c.sendCommand(['XGROUP', 'DESTROY', key, 'g']),
    },
    {
      name: 'XGROUP CREATECONSUMER creating a consumer',
      initialize: async (c, key) => {
        await c.sendCommand(['XADD', key, '1-1', 'f', 'v'])
        await c.sendCommand(['XGROUP', 'CREATE', key, 'g', '0'])
      },
      mutate: (c, key) =>
        c.sendCommand(['XGROUP', 'CREATECONSUMER', key, 'g', 'c']),
    },
    {
      name: 'XGROUP DELCONSUMER removing a consumer',
      initialize: async (c, key) => {
        await c.sendCommand(['XADD', key, '1-1', 'f', 'v'])
        await c.sendCommand(['XGROUP', 'CREATE', key, 'g', '0'])
        await c.sendCommand(['XGROUP', 'CREATECONSUMER', key, 'g', 'c'])
      },
      mutate: (c, key) =>
        c.sendCommand(['XGROUP', 'DELCONSUMER', key, 'g', 'c']),
    },
  ]

  for (const item of cleanCases) {
    test(`${item.name} does not dirty a watched stream`, async () => {
      const key = `stream:{watch:${randomKey()}}`
      const directClient = await connectToNodeRedisSlotOwner(redisClient, key)

      try {
        await directClient.del(key)
        await item.initialize(directClient, key)

        await directClient.watch(key)
        await item.mutate(directClient, key)

        const result = await directClient.multi().set(key, 'after').exec()
        assert.deepStrictEqual(result, ['OK'], item.name)
      } finally {
        await directClient.unwatch().catch(() => undefined)
        await directClient.del(key).catch(() => undefined)
        directClient.destroy()
      }
    })
  }

  // Creating the watched key itself is a write, even via XGROUP CREATE MKSTREAM:
  // real Redis dirties a WATCH on a key that comes into existence.
  test('XGROUP CREATE MKSTREAM creating the watched key dirties it', async () => {
    const key = `stream:{watch:${randomKey()}}`
    const directClient = await connectToNodeRedisSlotOwner(redisClient, key)

    try {
      await directClient.del(key)

      await directClient.watch(key)
      assert.strictEqual(
        await directClient.sendCommand([
          'XGROUP',
          'CREATE',
          key,
          'g',
          '0',
          'MKSTREAM',
        ]),
        'OK',
      )

      await assert.rejects(
        () => directClient.multi().set(key, 'after').exec(),
        WatchError,
      )
    } finally {
      await directClient.unwatch().catch(() => undefined)
      await directClient.del(key).catch(() => undefined)
      directClient.destroy()
    }
  })

  test('XADD still dirties a watched stream', async () => {
    const key = `stream:{watch:${randomKey()}}`
    const directClient = await connectToNodeRedisSlotOwner(redisClient, key)

    try {
      await directClient.del(key)
      await directClient.sendCommand(['XADD', key, '1-1', 'f', 'v'])

      await directClient.watch(key)
      await directClient.sendCommand(['XADD', key, '2-2', 'f', 'v'])

      await assert.rejects(
        () => directClient.multi().set(key, 'after').exec(),
        WatchError,
      )
    } finally {
      await directClient.unwatch().catch(() => undefined)
      await directClient.del(key).catch(() => undefined)
      directClient.destroy()
    }
  })
})
