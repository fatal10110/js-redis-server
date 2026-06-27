import { describe, test } from 'node:test'
import assert from 'node:assert'

import {
  createRedisCluster,
  createRedisMock,
  createRedisServer,
} from '../../src'

describe('compatibility builder wiring', () => {
  test('createRedisServer applies compatibility to standalone state', async () => {
    const handle = await createRedisServer({ compatibility: 'redis-6.2' })
    try {
      assert.strictEqual(handle.state.profile.version, '6.2.14')
    } finally {
      await handle.close()
    }
  })

  test('createRedisMock applies compatibility to standalone mocks', async () => {
    const mock = await createRedisMock({ compatibility: 'redis-6.2' })
    try {
      assert.strictEqual(mock.state?.profile.version, '6.2.14')
    } finally {
      await mock.close()
    }
  })

  test('createRedisCluster applies compatibility to every node', async () => {
    const cluster = createRedisCluster({
      masters: 2,
      replicasPerMaster: 1,
      basePort: 0,
      compatibility: 'valkey-9.0',
    })
    try {
      for (const node of cluster.nodes) {
        assert.strictEqual(node.server.profile.flavor, 'valkey')
        assert.strictEqual(node.server.profile.version, '9.0.0')
      }
    } finally {
      await cluster.close()
    }
  })
})
