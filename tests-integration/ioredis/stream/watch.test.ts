import { Cluster, Redis } from 'ioredis'
import { after, before, describe, test } from 'node:test'
import assert from 'node:assert'
import { TestRunner } from '../../test-config'
import { connectToSlotOwner, randomKey } from '../../utils'

const testRunner = new TestRunner()

// Real Redis does NOT invalidate a WATCH on a stream key for consumer-group /
// pending-entry / last-id metadata changes. Only changes to the stream's entry
// set (XADD, XDEL, ...) touch the watched key. These tests pin that behavior so
// the tracked-stream helpers never re-introduce the spurious WATCH dirtying that
// the old unconditional forceWrite() produced.
describe('Stream metadata WATCH semantics', () => {
  let redisClient: Cluster | undefined

  before(async () => {
    redisClient = await testRunner.setupIoredisCluster('stream-watch')
  })

  after(async () => {
    await testRunner.cleanup()
  })

  type Case = {
    name: string
    initialize(client: Redis, key: string): Promise<unknown>
    mutate(client: Redis, key: string): Promise<unknown>
  }

  // Every command below mutates consumer-group / pending / last-id metadata but
  // must leave the WATCH intact (the queued SET still runs -> EXEC === ['OK']).
  const cleanCases: Case[] = [
    {
      name: 'XSETID to a different id',
      initialize: (client, key) => client.xadd(key, '5-5', 'f', 'v'),
      mutate: (client, key) => client.xsetid(key, '9-9'),
    },
    {
      name: 'XGROUP SETID to a different id',
      initialize: async (client, key) => {
        await client.xadd(key, '1-1', 'f', 'v')
        await client.xadd(key, '2-2', 'f', 'v')
        await client.xgroup('CREATE', key, 'g', '0')
      },
      mutate: (client, key) => client.xgroup('SETID', key, 'g', '2'),
    },
    {
      name: 'XREADGROUP > delivering new entries',
      initialize: async (client, key) => {
        await client.xadd(key, '1-1', 'f', 'v')
        await client.xgroup('CREATE', key, 'g', '0')
      },
      mutate: (client, key) =>
        client.xreadgroup(
          'GROUP',
          'g',
          'c',
          'COUNT',
          '10',
          'STREAMS',
          key,
          '>',
        ),
    },
    {
      name: 'XREADGROUP history read (explicit id)',
      initialize: async (client, key) => {
        await client.xadd(key, '1-1', 'f', 'v')
        await client.xgroup('CREATE', key, 'g', '0')
        await client.xreadgroup('GROUP', 'g', 'c', 'STREAMS', key, '>')
      },
      mutate: (client, key) =>
        client.xreadgroup('GROUP', 'g', 'c', 'STREAMS', key, '0'),
    },
    {
      name: 'XREADGROUP > with no new entries',
      initialize: async (client, key) => {
        await client.xadd(key, '1-1', 'f', 'v')
        await client.xgroup('CREATE', key, 'g', '$')
      },
      mutate: (client, key) =>
        client.xreadgroup('GROUP', 'g', 'c', 'STREAMS', key, '>'),
    },
    {
      name: 'XCLAIM that claims a pending entry',
      initialize: async (client, key) => {
        await client.xadd(key, '1-1', 'f', 'v')
        await client.xgroup('CREATE', key, 'g', '0')
        await client.xreadgroup('GROUP', 'g', 'c1', 'STREAMS', key, '>')
      },
      mutate: (client, key) => client.xclaim(key, 'g', 'c2', '0', '1-1'),
    },
    {
      name: 'XCLAIM that claims nothing',
      initialize: async (client, key) => {
        await client.xadd(key, '1-1', 'f', 'v')
        await client.xgroup('CREATE', key, 'g', '0')
      },
      mutate: (client, key) => client.xclaim(key, 'g', 'c2', '0', '9-9'),
    },
    {
      name: 'XAUTOCLAIM that claims a pending entry',
      initialize: async (client, key) => {
        await client.xadd(key, '1-1', 'f', 'v')
        await client.xgroup('CREATE', key, 'g', '0')
        await client.xreadgroup('GROUP', 'g', 'c1', 'STREAMS', key, '>')
      },
      mutate: (client, key) => client.xautoclaim(key, 'g', 'c2', '0', '0'),
    },
    {
      name: 'XAUTOCLAIM that claims nothing',
      initialize: async (client, key) => {
        await client.xadd(key, '1-1', 'f', 'v')
        await client.xgroup('CREATE', key, 'g', '0')
      },
      mutate: (client, key) => client.xautoclaim(key, 'g', 'c2', '0', '0'),
    },
    {
      name: 'XACK acknowledging a pending entry',
      initialize: async (client, key) => {
        await client.xadd(key, '1-1', 'f', 'v')
        await client.xgroup('CREATE', key, 'g', '0')
        await client.xreadgroup('GROUP', 'g', 'c', 'STREAMS', key, '>')
      },
      mutate: (client, key) => client.xack(key, 'g', '1-1'),
    },
    {
      name: 'XGROUP CREATE on an existing stream',
      initialize: (client, key) => client.xadd(key, '1-1', 'f', 'v'),
      mutate: (client, key) => client.xgroup('CREATE', key, 'g', '0'),
    },
    {
      name: 'XGROUP DESTROY removing a group',
      initialize: async (client, key) => {
        await client.xadd(key, '1-1', 'f', 'v')
        await client.xgroup('CREATE', key, 'g', '0')
      },
      mutate: (client, key) => client.xgroup('DESTROY', key, 'g'),
    },
    {
      name: 'XGROUP CREATECONSUMER creating a consumer',
      initialize: async (client, key) => {
        await client.xadd(key, '1-1', 'f', 'v')
        await client.xgroup('CREATE', key, 'g', '0')
      },
      mutate: (client, key) => client.xgroup('CREATECONSUMER', key, 'g', 'c'),
    },
    {
      name: 'XGROUP DELCONSUMER removing a consumer',
      initialize: async (client, key) => {
        await client.xadd(key, '1-1', 'f', 'v')
        await client.xgroup('CREATE', key, 'g', '0')
        await client.xgroup('CREATECONSUMER', key, 'g', 'c')
      },
      mutate: (client, key) => client.xgroup('DELCONSUMER', key, 'g', 'c'),
    },
  ]

  for (const item of cleanCases) {
    test(`${item.name} does not dirty a watched stream`, async () => {
      const key = `stream:{watch:${randomKey()}}`
      const directClient = await connectToSlotOwner(redisClient!, key)

      try {
        await directClient.del(key)
        await item.initialize(directClient, key)

        assert.strictEqual(await directClient.call('WATCH', key), 'OK')
        await item.mutate(directClient, key)

        assert.strictEqual(await directClient.call('MULTI'), 'OK')
        assert.strictEqual(await directClient.set(key, 'after'), 'QUEUED')

        const result = await directClient.call('EXEC')
        assert.deepStrictEqual(result, ['OK'], item.name)
      } finally {
        await directClient.call('UNWATCH').catch(() => undefined)
        await directClient.del(key).catch(() => undefined)
        directClient.disconnect()
      }
    })
  }

  // Creating the watched key itself is a write, even via XGROUP CREATE MKSTREAM:
  // real Redis dirties a WATCH on a key that comes into existence.
  test('XGROUP CREATE MKSTREAM creating the watched key dirties it', async () => {
    const key = `stream:{watch:${randomKey()}}`
    const directClient = await connectToSlotOwner(redisClient!, key)

    try {
      await directClient.del(key)

      assert.strictEqual(await directClient.call('WATCH', key), 'OK')
      assert.strictEqual(
        await directClient.xgroup('CREATE', key, 'g', '0', 'MKSTREAM'),
        'OK',
      )

      assert.strictEqual(await directClient.call('MULTI'), 'OK')
      assert.strictEqual(await directClient.set(key, 'after'), 'QUEUED')

      const result = await directClient.call('EXEC')
      assert.strictEqual(result, null)
    } finally {
      await directClient.call('UNWATCH').catch(() => undefined)
      await directClient.del(key).catch(() => undefined)
      directClient.disconnect()
    }
  })

  test('XADD still dirties a watched stream', async () => {
    const key = `stream:{watch:${randomKey()}}`
    const directClient = await connectToSlotOwner(redisClient!, key)

    try {
      await directClient.del(key)
      await directClient.xadd(key, '1-1', 'f', 'v')

      assert.strictEqual(await directClient.call('WATCH', key), 'OK')
      await directClient.xadd(key, '2-2', 'f', 'v')

      assert.strictEqual(await directClient.call('MULTI'), 'OK')
      assert.strictEqual(await directClient.set(key, 'after'), 'QUEUED')

      const result = await directClient.call('EXEC')
      assert.strictEqual(result, null)
    } finally {
      await directClient.call('UNWATCH').catch(() => undefined)
      await directClient.del(key).catch(() => undefined)
      directClient.disconnect()
    }
  })
})
