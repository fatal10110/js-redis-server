import { after, before, describe, test } from 'node:test'
import assert from 'node:assert'
import type { Cluster, Redis } from 'ioredis'

import { TestRunner } from '../test-config'
import {
  activeProfile,
  connectToSlotOwner,
  randomKey,
  type ProfileName,
} from '../utils'

const testRunner = new TestRunner()
const profile = activeProfile

/**
 * `set.listpack-encoding` — Redis 7.2 (and every Valkey) creates a set as an
 * intset only when SADD's member count fits `set-max-intset-entries`. Before
 * that, an integer first member always makes an intset, so duplicates that
 * keep the set within the limit leave it sorted (#504). Verified against
 * redis-server 6.2.24, 7.0.15, 7.2.16, 7.4, 8.0.6 and Valkey 8.0 / 9.0.
 */
const sizeHintProfiles: ProfileName[] = [
  'redis-7.2',
  'redis-7.4',
  'redis-8.0',
  'valkey-8.0',
  'valkey-9.0',
]

/**
 * `set.union-diff-hashtable` — from Redis 8.0 / Valkey 8.0 a non-STORE SUNION
 * or SDIFF (and SPOP with a count that covers the set, which replies an
 * SUNION of the key) builds its result as a hashtable when a source is not an
 * intset, so the integers of a listpack keep their order; before that the
 * result starts as an intset and sorts them. The STORE forms still start from
 * an intset. Valkey 9.0.0 gives the walk order pinned here; Redis 8.0's and
 * Valkey 8.0's hashtable order is undefined, and the mock gives the same walk
 * order there. Verified against redis-server 6.2.24, 7.2.16, 7.4, 8.0.6 and
 * Valkey 7.2.14 / 8.0 / 9.0.
 */
const hashtableResultProfiles: ProfileName[] = [
  'redis-8.0',
  'valkey-8.0',
  'valkey-9.0',
]

describe(
  `Set encoding gates (${testRunner.getBackendName()}, ${profile})`,
  // CONFIG SET would leak into the shared real cluster.
  { skip: testRunner.backend === 'real' && 'profiles are mock-only' },
  () => {
    let redisClient: Cluster

    before(async () => {
      redisClient = await testRunner.setupIoredisCluster('set-encoding-gates')
    })

    after(async () => {
      await testRunner.cleanup()
    })

    async function withOps(
      fn: (client: Redis, k: (name: string) => string) => Promise<void>,
    ): Promise<void> {
      const tag = `{set-gate:${randomKey()}}`
      const k = (name: string) => `${tag}:${name}`
      const directClient = await connectToSlotOwner(redisClient, k('seed'))
      try {
        await fn(directClient, k)
      } finally {
        await directClient.config('SET', 'set-max-intset-entries', '512')
        directClient.disconnect()
      }
    }

    test('an integer set is sorted on every profile', async () => {
      await withOps(async (c, k) => {
        await c.sadd(k('s'), '3', '1', '2')
        assert.deepStrictEqual(await c.smembers(k('s')), ['1', '2', '3'])
      })
    })

    test('SADD past set-max-intset-entries skips the intset only from 7.2', async () => {
      await withOps(async (c, k) => {
        await c.config('SET', 'set-max-intset-entries', '2')
        await c.sadd(k('s'), '3', '1', '3')
        assert.deepStrictEqual(
          await c.smembers(k('s')),
          sizeHintProfiles.includes(profile) ? ['3', '1'] : ['1', '3'],
        )
      })
    })

    test('SUNION / SDIFF / SPOP of a non-intset keep its order only from 8.0', async () => {
      await withOps(async (c, k) => {
        const walked = hashtableResultProfiles.includes(profile)
        await c.sadd(k('u'), 'x', '3', '1')
        await c.srem(k('u'), 'x')
        await c.sadd(k('a'), '5', '2')

        assert.deepStrictEqual(
          await c.sunion(k('u')),
          walked ? ['3', '1'] : ['1', '3'],
        )
        assert.deepStrictEqual(
          await c.sdiff(k('u'), k('missing')),
          walked ? ['3', '1'] : ['1', '3'],
        )
        assert.deepStrictEqual(
          await c.sunion(k('a'), k('u')),
          walked ? ['2', '5', '3', '1'] : ['1', '2', '3', '5'],
        )
        // Intset sources alone still give an intset.
        assert.deepStrictEqual(await c.sunion(k('a')), ['2', '5'])
        // The STORE forms start from an intset on every profile.
        assert.strictEqual(await c.sunionstore(k('d'), k('u')), 2)
        assert.deepStrictEqual(await c.smembers(k('d')), ['1', '3'])

        assert.deepStrictEqual(
          await c.spop(k('u'), 9),
          walked ? ['3', '1'] : ['1', '3'],
        )
      })
    })
  },
)
