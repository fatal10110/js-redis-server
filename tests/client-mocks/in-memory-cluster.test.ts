import { describe, test } from 'node:test'
import assert from 'node:assert'
import type { RedisValue } from '../../src/core/redis-value'
import { ClientSession } from '../../src/internal'
import { createInMemoryCluster } from '../../src/client-mocks/in-memory-cluster'

function asText(value: RedisValue): string | null {
  const inner = (value as { value?: unknown }).value
  if (inner === null || inner === undefined) {
    return null
  }
  return Buffer.isBuffer(inner) ? inner.toString() : String(inner)
}

describe('InMemoryCluster (client-side slot routing)', () => {
  test('round-trips a value through the slot owner', async () => {
    const cluster = createInMemoryCluster({ masters: 3 })
    try {
      assert.strictEqual(
        asText(await cluster.execute(['SET', 'foo', 'bar'])),
        'OK',
      )
      assert.strictEqual(asText(await cluster.execute(['GET', 'foo'])), 'bar')
    } finally {
      cluster.close()
    }
  })

  test('spreads keys across more than one master — no MOVED needed', () => {
    const cluster = createInMemoryCluster({ masters: 3 })
    try {
      const sessions = new Set<ClientSession>()
      for (let i = 0; i < 50; i++) {
        sessions.add(cluster.route(['GET', `key:${i}`]))
      }
      assert.ok(
        sessions.size > 1,
        `expected keys to route across masters, got ${sessions.size}`,
      )
    } finally {
      cluster.close()
    }
  })

  test('routes the same slot to the same cached session', () => {
    const cluster = createInMemoryCluster({ masters: 3 })
    try {
      const a = cluster.route(['GET', 'samekey'])
      const b = cluster.route(['SET', 'samekey', 'x'])
      assert.strictEqual(a, b)
    } finally {
      cluster.close()
    }
  })

  test('runs keyless commands on the first master', async () => {
    const cluster = createInMemoryCluster({ masters: 3 })
    try {
      assert.strictEqual(asText(await cluster.execute(['PING'])), 'PONG')
    } finally {
      cluster.close()
    }
  })

  test('execute rejects after close', async () => {
    const cluster = createInMemoryCluster({ masters: 2 })
    cluster.close()
    await assert.rejects(() => cluster.execute(['GET', 'k']), /closed/)
  })
})
