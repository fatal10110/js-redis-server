import { Redis } from 'ioredis'
import { after, before, describe, test } from 'node:test'
import assert from 'node:assert'
import { TestRunner } from '../test-config'

const testRunner = new TestRunner()

describe(`INFO command standalone integration (${testRunner.getBackendName()})`, () => {
  let client: Redis | undefined

  before(async () => {
    client = await testRunner.setupIoredisStandalone()
  })

  after(async () => {
    await testRunner.cleanup()
  })

  test('INFO cluster omits cluster-only state fields for standalone servers', async () => {
    const info = (await client!.call('INFO', 'cluster')) as string

    assert.match(info, /cluster_enabled:0/)
    assert.doesNotMatch(info, /^cluster_state:/m)
    assert.doesNotMatch(info, /^cluster_slots_assigned:/m)
  })

  test('INFO replication exposes Redis-compatible replication identifiers', async () => {
    const info = (await client!.call('INFO', 'replication')) as string

    assert.match(info, /^role:master$/m)
    assert.match(info, /^connected_slaves:0$/m)
    assert.match(info, /^master_failover_state:no-failover$/m)
    assert.match(info, /^master_replid:[0-9a-f]{40}$/m)
    assert.match(info, /^master_repl_offset:\d+$/m)
    assert.match(info, /^second_repl_offset:-1$/m)
  })
})
