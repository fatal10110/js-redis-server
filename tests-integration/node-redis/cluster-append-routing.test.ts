import { test, describe, before, after } from 'node:test'
import assert from 'node:assert'
import diagnosticsChannel from 'node:diagnostics_channel'
import { RedisClusterType } from 'redis'
import { TestRunner } from '../test-config'
import { clusterAppend, randomKey } from '../utils'

/**
 * #621: node-redis 6.0's typed `append()` does not register its key, so the
 * cluster client sends APPEND to a random master and leans on MOVED to find
 * the owner. Every MOVED re-randomizes the pick, and about once in a thousand
 * calls all 17 picks miss: "MOVED <slot>" after 16 redirections. The suites
 * route APPEND through `clusterAppend`, which names the key.
 *
 * node-redis publishes every redirection it follows on the `node-redis:error`
 * diagnostics channel, so a key-routed APPEND must publish none. Twelve keys
 * in different slots: a random router would follow a MOVED for about two in
 * three of them.
 */
const testRunner = new TestRunner()

describe(`APPEND routing through a cluster (node-redis, ${testRunner.getBackendName()})`, () => {
  let cluster: RedisClusterType

  before(async () => {
    cluster = (await testRunner.setupNodeRedisCluster()) as RedisClusterType
  })

  after(async () => {
    await testRunner.cleanup()
  })

  test('clusterAppend reaches the slot owner without a MOVED', async () => {
    const redirections: string[] = []
    const onError = (message: unknown): void => {
      const { error } = message as { error?: Error }
      if (error?.message.startsWith('MOVED')) {
        redirections.push(error.message)
      }
    }
    diagnosticsChannel.subscribe('node-redis:error', onError)
    try {
      const run = randomKey()
      for (let i = 0; i < 12; i++) {
        const key = `{append-route-${i}:${run}}k`
        assert.strictEqual(await clusterAppend(cluster, key, 'ab'), 2)
        assert.strictEqual(await clusterAppend(cluster, key, 'cd'), 4)
        assert.strictEqual(await cluster.get(key), 'abcd')
      }
    } finally {
      diagnosticsChannel.unsubscribe('node-redis:error', onError)
    }
    assert.deepStrictEqual(redirections, [])
  })
})
