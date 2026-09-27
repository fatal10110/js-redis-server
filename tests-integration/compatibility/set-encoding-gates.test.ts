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
  },
)
