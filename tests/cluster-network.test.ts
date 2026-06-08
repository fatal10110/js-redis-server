import { describe, test } from 'node:test'
import assert from 'node:assert'
import { setTimeout as sleep } from 'node:timers/promises'
import { buildRedisCluster, computeSlotRange } from '../src/cluster'

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

  test('replica state updates can be delayed', async () => {
    const cluster = buildRedisCluster({
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

    assert.strictEqual(replica.server.getDatabase(0).getString(key), null)

    await sleep(40)

    assert.deepStrictEqual(
      replica.server.getDatabase(0).getString(key),
      Buffer.from('value'),
    )
  })
})
