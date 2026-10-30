import { after, before, describe, test } from 'node:test'
import assert from 'node:assert'
import type { RedisClientType, RedisClusterType } from 'redis'
import { TestRunner } from '../../test-config'
import {
  connectToNodeRedisEndpoint,
  eventually,
  findNodeRedisSlotMasterAndReplica,
  randomKey,
} from '../../utils'

const testRunner = new TestRunner()

// Real Redis propagates in-place consumer-group / last-id changes to its
// replicas (XGROUP and XSETID as themselves; XACK; XREADGROUP, XCLAIM and
// XAUTOCLAIM as idempotent XCLAIM / XGROUP SETID / CREATECONSUMER). No XADD
// follows them here: a full-value write would mask a missing propagation.

async function replicaGroupState(replica: RedisClientType, key: string) {
  const state: Record<string, unknown> = {}
  for (const group of await replica.xInfoGroups(key)) {
    const name = String(group.name)
    const consumers = await replica.xInfoConsumers(key, name)
    const pending = await replica.xPendingRange(key, name, '-', '+', 10)
    state[name] = {
      lastDeliveredId: group['last-delivered-id'],
      pending: group.pending,
      consumers: consumers.map(c => [c.name, c.pending]),
      pel: pending.map(p => [p.id, p.consumer, p.deliveriesCounter]),
    }
  }
  const stream = await replica.xInfoStream(key)
  return { lastId: stream['last-generated-id'], groups: state }
}

describe(`Stream group replication (node-redis, ${testRunner.getBackendName()})`, () => {
  let redisClient: RedisClusterType

  before(async () => {
    redisClient = (await testRunner.setupNodeRedisCluster({
      replicasPerMaster: 1,
    })) as RedisClusterType
  })

  after(async () => {
    await testRunner.cleanup()
  })

  test('replicas receive in-place consumer-group and last-id changes', async () => {
    const key = `{repl:${randomKey()}}:s`
    const { master, replica } = await findNodeRedisSlotMasterAndReplica(
      redisClient,
      key,
    )
    const masterClient = await connectToNodeRedisEndpoint(master)
    const replicaClient = await connectToNodeRedisEndpoint(replica)

    try {
      for (const id of ['1-1', '2-1', '3-1']) {
        await masterClient.xAdd(key, id, { f: 'v' })
      }
      assert.strictEqual(await replicaClient.sendCommand(['READONLY']), 'OK')
      await eventually(async () => {
        assert.strictEqual(await replicaClient.xLen(key), 3)
      })

      await masterClient.xGroupCreate(key, 'g', '0')
      assert.strictEqual(
        await masterClient.xGroupCreateConsumer(key, 'g', 'c1'),
        1,
      )
      await masterClient.xReadGroup('g', 'c2', { key, id: '>' }, { COUNT: 2 })
      assert.strictEqual(await masterClient.xAck(key, 'g', '1-1'), 1)
      await masterClient.xClaim(key, 'g', 'c1', 0, '2-1')
      await masterClient.xReadGroup('g', 'c3', { key, id: '>' })
      await masterClient.xAutoClaim(key, 'g', 'c4', 0, '3-1')
      await eventually(async () => {
        assert.deepStrictEqual(await replicaGroupState(replicaClient, key), {
          lastId: '3-1',
          groups: {
            g: {
              lastDeliveredId: '3-1',
              pending: 2,
              consumers: [
                ['c1', 1],
                ['c2', 0],
                ['c3', 0],
                ['c4', 1],
              ],
              pel: [
                ['2-1', 'c1', 2],
                ['3-1', 'c4', 2],
              ],
            },
          },
        })
      })

      assert.strictEqual(
        await masterClient.xGroupDelConsumer(key, 'g', 'c2'),
        0,
      )
      assert.strictEqual(await masterClient.xGroupSetId(key, 'g', '1-1'), 'OK')
      assert.strictEqual(await masterClient.xSetId(key, '10-0'), 'OK')

      await eventually(async () => {
        assert.deepStrictEqual(await replicaGroupState(replicaClient, key), {
          lastId: '10-0',
          groups: {
            g: {
              lastDeliveredId: '1-1',
              pending: 2,
              consumers: [
                ['c1', 1],
                ['c3', 0],
                ['c4', 1],
              ],
              pel: [
                ['2-1', 'c1', 2],
                ['3-1', 'c4', 2],
              ],
            },
          },
        })
      })

      await masterClient.xGroupCreate(key, 'g2', '$')
      assert.strictEqual(await masterClient.xGroupDestroy(key, 'g'), 1)

      await eventually(async () => {
        assert.deepStrictEqual(await replicaGroupState(replicaClient, key), {
          lastId: '10-0',
          groups: {
            g2: {
              lastDeliveredId: '10-0',
              pending: 0,
              consumers: [],
              pel: [],
            },
          },
        })
      })
    } finally {
      await masterClient.del(key)
      masterClient.destroy()
      replicaClient.destroy()
    }
  })
})
