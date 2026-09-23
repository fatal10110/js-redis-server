import { after, before, describe, test } from 'node:test'
import assert from 'node:assert'
import type { Cluster, Redis } from 'ioredis'

import { TestRunner } from '../test-config'
import {
  activeProfile,
  connectToSlotOwner,
  errorWithMessage,
  randomKey,
  type ProfileName,
} from '../utils'

const testRunner = new TestRunner()
const profile = activeProfile

/**
 * `sort.cluster-pattern-slot` — Redis 7.4 / Valkey 8.0 replaced the blanket
 * "denied in Cluster mode" refusal with a per-pattern slot comparison
 * (`patternHashSlot()` vs the sort key's slot) and the longer error wording.
 */
const patternSlotProfiles: ProfileName[] = [
  'redis-7.4',
  'redis-8.0',
  'valkey-8.0',
  'valkey-9.0',
]

/**
 * `sort.cluster-get-hash` — Redis 7.4.2 / Valkey 8.0.2 additionally exempt the
 * `GET #` self pattern from that slot comparison. Before that, `GET #` is
 * hashed like any other pattern and therefore refused. Bisected on single-node
 * clusters: redis 7.4.0/7.4.1 refuse, 7.4.2+ accept; valkey 8.0.0/8.0.1
 * refuse, 8.0.2+ accept.
 *
 * The preset versions decide which profiles land on which side: `redis-7.4`
 * pins 7.4.4 (exempt) while `valkey-8.0` pins 8.0.0 (still refused).
 */
const getHashProfiles: ProfileName[] = ['redis-7.4', 'redis-8.0', 'valkey-9.0']

const comparesPatternSlots = patternSlotProfiles.includes(profile)
const allowsGetHash = getHashProfiles.includes(profile)

const byError = comparesPatternSlots
  ? 'ERR BY option of SORT denied in Cluster mode when keys formed by the pattern may be in different slots.'
  : 'ERR BY option of SORT denied in Cluster mode.'
const getError = comparesPatternSlots
  ? 'ERR GET option of SORT denied in Cluster mode when keys formed by the pattern may be in different slots.'
  : 'ERR GET option of SORT denied in Cluster mode.'

describe(
  `SORT cluster gates (${testRunner.getBackendName()}, ${profile})`,
  { skip: testRunner.backend === 'real' && 'profiles are mock-only' },
  () => {
    let redisClient: Cluster

    before(async () => {
      redisClient = await testRunner.setupIoredisCluster('sort-cluster-gates')
    })

    after(async () => {
      await testRunner.cleanup()
    })

    async function withOps(
      fn: (client: Redis, k: (name: string) => string) => Promise<void>,
    ): Promise<void> {
      const tag = `{sort-gate:${randomKey()}}`
      const k = (name: string) => `${tag}:${name}`
      const directClient = await connectToSlotOwner(redisClient, k('seed'))
      try {
        await fn(directClient, k)
      } finally {
        directClient.disconnect()
      }
    }

    test('BY nosort is accepted on every profile', async () => {
      await withOps(async (c, k) => {
        await c.rpush(k('l'), '3', '1', '2')
        assert.deepStrictEqual(await c.sort(k('l'), 'BY', 'nosort'), [
          '3',
          '1',
          '2',
        ])
      })
    })

    test('a same-slot BY glob is accepted only once pattern slots are compared', async () => {
      await withOps(async (c, k) => {
        await c.rpush(k('ids'), '2', '1')
        await c.set(k('weight:1'), '20')
        await c.set(k('weight:2'), '10')

        if (!comparesPatternSlots) {
          await assert.rejects(
            () => c.sort(k('ids'), 'BY', k('weight:*')),
            errorWithMessage(byError),
          )
          return
        }

        assert.deepStrictEqual(await c.sort(k('ids'), 'BY', k('weight:*')), [
          '2',
          '1',
        ])
      })
    })

    test('a cross-slot BY glob is always refused, with profile-specific wording', async () => {
      await withOps(async (c, k) => {
        await c.rpush(k('ids'), '1')
        await assert.rejects(
          () => c.sort(k('ids'), 'BY', `{sort-other:${randomKey()}}:weight:*`),
          errorWithMessage(byError),
        )
      })
    })

    test('a same-slot GET glob is accepted only once pattern slots are compared', async () => {
      await withOps(async (c, k) => {
        await c.rpush(k('ids'), '1')
        await c.set(k('name:1'), 'one')

        if (!comparesPatternSlots) {
          await assert.rejects(
            () => c.sort(k('ids'), 'GET', k('name:*')),
            errorWithMessage(getError),
          )
          return
        }

        assert.deepStrictEqual(await c.sort(k('ids'), 'GET', k('name:*')), [
          'one',
        ])
      })
    })

    test("GET '#' is exempt from the slot comparison only on the newest profiles", async () => {
      await withOps(async (c, k) => {
        await c.rpush(k('ids'), '2', '1')

        if (!allowsGetHash) {
          await assert.rejects(
            () => c.sort(k('ids'), 'GET', '#'),
            errorWithMessage(getError),
          )
          return
        }

        assert.deepStrictEqual(await c.sort(k('ids'), 'GET', '#'), ['1', '2'])
      })
    })

    // #417: the guard runs inside SORT's own left-to-right option scan, so the
    // ordering rules hold under both wordings. The globs below have a '*'
    // before any hash tag and are refused on every profile.
    test('the first denied option in argument order is reported', async () => {
      await withOps(async (c, k) => {
        await c.rpush(k('ids'), '1')
        await assert.rejects(
          () => c.sort(k('ids'), 'GET', 'n_*', 'BY', 'w_*'),
          errorWithMessage(getError),
        )
        await assert.rejects(
          () => c.sort(k('ids'), 'BY', 'w_*', 'GET', 'n_*'),
          errorWithMessage(byError),
        )
      })
    })

    test('a denied pattern is reported before a later syntax error', async () => {
      await withOps(async (c, k) => {
        await c.rpush(k('ids'), '1')
        await assert.rejects(
          () => c.sort(k('ids'), 'BY', 'w_*', 'BADARG'),
          errorWithMessage(byError),
        )
        await assert.rejects(
          () => c.sort(k('ids'), 'BADARG', 'BY', 'w_*'),
          errorWithMessage('ERR syntax error'),
        )
      })
    })

    test('a denied pattern inside MULTI queues and fails in EXEC', async () => {
      await withOps(async (c, k) => {
        await c.rpush(k('ids'), '1')
        const replies = await c
          .multi()
          .sort(k('ids'), 'BY', 'w_*')
          .sort(k('ids'), 'GET', 'n_*')
          .sort(k('ids'))
          .exec()

        assert.ok(replies)
        assert.deepStrictEqual(
          replies.map(([err]) => err?.message ?? null),
          [byError, getError, null],
        )
        assert.deepStrictEqual(replies[2]?.[1], ['1'])
      })
    })
  },
)
