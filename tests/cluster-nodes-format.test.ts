import { describe, test } from 'node:test'
import assert from 'node:assert'
import {
  ClientSession,
  RedisClusterTopology,
  RedisServerState,
  createClusterCommands,
  createClusterPolicy,
  createRedisCommandExecutor,
} from '../src'

describe('CLUSTER NODES output format', () => {
  test('reports bus port as client port + 10000 (#55)', async () => {
    const text = await clusterNodes()

    for (const line of text.split('\n').filter(l => l.trim().length > 0)) {
      const address = line.split(' ')[1]
      const match = address.match(/^(.+):(\d+)@(\d+)$/)
      assert.ok(match, `address field malformed: ${address}`)
      const clientPort = Number(match[2])
      const busPort = Number(match[3])
      assert.strictEqual(busPort, clientPort + 10000)
    }
  })

  test('formats a single-slot range as a bare integer, not N-N (#63)', async () => {
    const text = await clusterNodes()

    // local owns exactly one slot (100) plus a multi-slot range (200-300)
    const localLine = text.split('\n').find(line => line.startsWith('local '))
    assert.ok(localLine, 'expected a line for the local node')

    const slotFields = localLine.trim().split(' ').slice(8)
    assert.deepStrictEqual(slotFields, ['100', '200-300'])
  })
})

async function clusterNodes(): Promise<string> {
  const topology = new RedisClusterTopology([
    {
      id: 'local',
      role: 'master',
      host: '127.0.0.1',
      port: 7000,
      slots: [
        [100, 100],
        [200, 300],
      ],
    },
    {
      id: 'remote',
      role: 'master',
      host: '127.0.0.1',
      port: 7001,
      slots: [[8192, 16383]],
    },
  ])
  const server = new RedisServerState({ clusterTopology: topology })
  const executor = createRedisCommandExecutor({
    extraCommands: createClusterCommands('local'),
    policies: [createClusterPolicy({ localNodeId: 'local' })],
  })
  const session = new ClientSession({ server, executor })

  const result = await session.execute('cluster', [Buffer.from('nodes')])
  const value = result.value
  if (value.kind !== 'bulk-string' || value.value === null) {
    throw new Error(`expected a non-null bulk string, got ${value.kind}`)
  }
  return value.value.toString()
}
