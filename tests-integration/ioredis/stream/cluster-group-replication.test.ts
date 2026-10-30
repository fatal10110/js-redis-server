import { after, before, describe, test } from 'node:test'
import assert from 'node:assert'
import type { Cluster, Redis } from 'ioredis'
import { TestRunner } from '../../test-config'
import {
  connectToEndpoint,
  eventually,
  findSlotMasterAndReplica,
  randomKey,
} from '../../utils'

const testRunner = new TestRunner()

// Real Redis propagates in-place consumer-group / last-id changes to its
// replicas (XGROUP and XSETID as themselves; XACK; XREADGROUP, XCLAIM and
// XAUTOCLAIM as idempotent XCLAIM / XGROUP SETID / CREATECONSUMER). No XADD
// follows them here: a full-value write would mask a missing propagation.

function pairs(reply: unknown): Record<string, unknown> {
  const items = reply as unknown[]
  const out: Record<string, unknown> = {}
  for (let i = 0; i + 1 < items.length; i += 2) {
    out[String(items[i])] = items[i + 1]
  }
  return out
}

async function replicaGroupState(replica: Redis, key: string) {
  const groups = ((await replica.xinfo('GROUPS', key)) as unknown[]).map(pairs)
  const state: Record<string, unknown> = {}
  for (const group of groups) {
    const name = String(group.name)
    const consumers = (
      (await replica.xinfo('CONSUMERS', key, name)) as unknown[]
    ).map(pairs)
    const pending = (await replica.xpending(key, name, '-', '+', 10)) as [
      string,
      string,
      number,
      number,
    ][]
    state[name] = {
      lastDeliveredId: group['last-delivered-id'],
      pending: group.pending,
      consumers: consumers.map(c => [c.name, c.pending]),
      pel: pending.map(([id, consumer, , count]) => [id, consumer, count]),
    }
  }
  const stream = pairs(await replica.xinfo('STREAM', key))
  return { lastId: stream['last-generated-id'], groups: state }
}

describe(`Stream group replication (ioredis, ${testRunner.getBackendName()})`, () => {
  let redisClient: Cluster | undefined

  before(async () => {
    redisClient = await testRunner.setupIoredisCluster(
      'stream-group-replication',
      { replicasPerMaster: 1 },
    )
  })

  after(async () => {
    await testRunner.cleanup()
  })

  test('replicas receive in-place consumer-group and last-id changes', async () => {
    const key = `{repl:${randomKey()}}:s`
    const { master, replica } = await findSlotMasterAndReplica(
      redisClient!,
      key,
    )
    const masterClient = await connectToEndpoint(master)
    const replicaClient = await connectToEndpoint(replica)

    try {
      for (const id of ['1-1', '2-1', '3-1']) {
        await masterClient.xadd(key, id, 'f', 'v')
      }
      assert.strictEqual(await replicaClient.readonly(), 'OK')
      await eventually(async () => {
        assert.strictEqual(await replicaClient.xlen(key), 3)
      })

      await masterClient.xgroup('CREATE', key, 'g', '0')
      assert.strictEqual(
        await masterClient.xgroup('CREATECONSUMER', key, 'g', 'c1'),
        1,
      )
      await masterClient.xreadgroup(
        'GROUP',
        'g',
        'c2',
        'COUNT',
        2,
        'STREAMS',
        key,
        '>',
      )
      assert.strictEqual(await masterClient.xack(key, 'g', '1-1'), 1)
      await masterClient.xclaim(key, 'g', 'c1', 0, '2-1')
      await masterClient.xreadgroup('GROUP', 'g', 'c3', 'STREAMS', key, '>')
      await masterClient.xautoclaim(key, 'g', 'c4', 0, '3-1')
      assert.strictEqual(
        await masterClient.xgroup('DELCONSUMER', key, 'g', 'c2'),
        0,
      )
      assert.strictEqual(
        await masterClient.xgroup('SETID', key, 'g', '1-1'),
        'OK',
      )
      assert.strictEqual(await masterClient.xsetid(key, '10-0'), 'OK')

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

      await masterClient.xgroup('CREATE', key, 'g2', '$')
      assert.strictEqual(await masterClient.xgroup('DESTROY', key, 'g'), 1)

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
      masterClient.disconnect()
      replicaClient.disconnect()
    }
  })
})
