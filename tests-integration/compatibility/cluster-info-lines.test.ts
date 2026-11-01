import { after, before, describe, test } from 'node:test'
import assert from 'node:assert'
import { needsCluster, TestRunner } from '../test-config'
import { activeProfile, commandFrame, randomKey } from '../utils'
import { RawRedisConnection } from '../raw-tcp/raw-connection'
import { connectToRawSlotOwner } from '../raw-tcp/helpers'

// #604: CLUSTER INFO's line set per version. Probed on redis 6.2.14, 7.0.15,
// 7.2.4, 7.4.4, 8.0.0 and valkey 8.0.0, 8.1.0, 9.0.0:
// - 6.2 ends at `cluster_stats_messages_received`;
// - redis 7.0+ and valkey 7.2+ add `total_cluster_links_buffer_limit_exceeded`;
// - valkey 9.0 adds the node pfail / fail counts after `cluster_slots_fail`.

const testRunner = new TestRunner()

const base = [
  'cluster_state',
  'cluster_slots_assigned',
  'cluster_slots_ok',
  'cluster_slots_pfail',
  'cluster_slots_fail',
  'cluster_known_nodes',
  'cluster_size',
  'cluster_current_epoch',
  'cluster_my_epoch',
  'cluster_stats_messages_sent',
  'cluster_stats_messages_received',
]
const nodeFailCounts = [
  'cluster_nodes_pfail',
  'cluster_nodes_fail',
  'cluster_voting_nodes_pfail',
  'cluster_voting_nodes_fail',
]

function expectedFields(): string[] {
  if (activeProfile === 'redis-6.2') {
    return base
  }
  const fields =
    activeProfile === 'valkey-9.0'
      ? [...base.slice(0, 5), ...nodeFailCounts, ...base.slice(5)]
      : base
  return [...fields, 'total_cluster_links_buffer_limit_exceeded']
}

const clusterSuite = `CLUSTER INFO lines (${testRunner.getBackendName()}, ${activeProfile})`
describe(clusterSuite, needsCluster, () => {
  let conn: RawRedisConnection

  before(async () => {
    const ports = await testRunner.setupRawCluster()
    conn = await connectToRawSlotOwner(ports, `cluster-info:${randomKey()}`)
  })

  after(async () => {
    conn?.close()
    await testRunner.cleanup()
  })

  test('has the profile line set, in order', async () => {
    conn.write(commandFrame('CLUSTER', 'INFO'))
    const frame = (await conn.readRawFrame()).toString()
    const header = frame.indexOf('\r\n')
    assert.ok(frame.startsWith('$'), frame)
    const body = frame.slice(header + 2, -2)
    assert.ok(body.endsWith('\r\n'), body)
    const lines = body.slice(0, -2).split('\r\n')

    assert.deepStrictEqual(
      lines.map(line => line.split(':')[0]),
      expectedFields(),
    )
    for (const line of lines) {
      if (/^(cluster_(voting_)?nodes_p?fail|total_cluster_links)/.test(line)) {
        assert.strictEqual(line.split(':')[1], '0', line)
      }
    }
  })
})
