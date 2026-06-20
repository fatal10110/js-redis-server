import { describe, test } from 'node:test'
import assert from 'node:assert'
import { setTimeout as sleep } from 'node:timers/promises'
import { createRedisCluster, computeSlotRange } from '../src/cluster'

describe('ClusterNetwork slot distribution', () => {
  test('computeSlotRange covers all slots without overlap', () => {
    const masters = 5
    const ranges = Array.from({ length: masters }, (_, i) =>
      computeSlotRange(i, masters),
    )

    assert.strictEqual(ranges[0][0], 0)
    assert.strictEqual(ranges[masters - 1][1], 16383)

    let totalSlots = 0
    for (let i = 0; i < ranges.length; i++) {
      const [start, end] = ranges[i]
      totalSlots += end - start + 1
      if (i > 0) {
        assert.strictEqual(start, ranges[i - 1][1] + 1)
      }
    }

    assert.strictEqual(totalSlots, 16384)
  })

  test('computeSlotRange rejects invalid inputs', () => {
    assert.throws(() => computeSlotRange(0, 0), /Invalid masters count/)
    assert.throws(() => computeSlotRange(-1, 3), /Invalid master index/)
    assert.throws(() => computeSlotRange(3, 3), /Invalid master index/)
  })

  test('listen cleans up nodes that already started when a later node fails', async () => {
    const cluster = createRedisCluster({ masters: 2, basePort: 0 })
    const servers = (
      cluster as unknown as {
        servers: Array<{
          listen(port?: number): Promise<void>
          server: { address(): unknown }
        }>
      }
    ).servers

    servers[1].listen = async () => {
      throw new Error('forced listen failure')
    }

    await assert.rejects(cluster.listen(), /forced listen failure/)
    assert.strictEqual(servers[0].server.address(), null)
  })

  test('replica state updates can be delayed', async () => {
    const cluster = createRedisCluster({
      masters: 1,
      replicasPerMaster: 1,
      basePort: 0,
      replicaUpdateDelayMs: 20,
    })
    const master = cluster.nodes.find(node => node.role === 'master')
    const replica = cluster.nodes.find(node => node.role === 'replica')
    assert.ok(master)
    assert.ok(replica)

    const key = Buffer.from('delayed-replica-key')
    master.server.getDatabase(0).setString(key, Buffer.from('value'))

    try {
      assert.strictEqual(replica.server.getDatabase(0).getString(key), null)

      await sleep(40)

      assert.deepStrictEqual(
        replica.server.getDatabase(0).getString(key),
        Buffer.from('value'),
      )
    } finally {
      await cluster.close()
    }
  })

  test('replica states do not actively expire replicated keys on their own clock', async () => {
    const cluster = createRedisCluster({
      masters: 1,
      replicasPerMaster: 1,
      basePort: 0,
    })
    const replica = cluster.nodes.find(node => node.role === 'replica')
    assert.ok(replica)

    const key = Buffer.from('replica-active-expiry-key')
    const events: string[] = []
    const replicaDb = replica.server.getDatabase(0)
    replicaDb.subscribeKey(key, event => events.push(event.type))

    try {
      replicaDb.setString(key, Buffer.from('value'), {
        expiresAt: Date.now() + 10,
      })

      await sleep(150)

      assert.deepStrictEqual(events, ['write'])
    } finally {
      await cluster.close()
    }
  })
})
