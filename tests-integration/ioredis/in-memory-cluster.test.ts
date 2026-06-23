import { describe, test, before, after, beforeEach } from 'node:test'
import assert from 'node:assert'
import type { Cluster } from 'ioredis'
import { createIoredisMock } from '../../src/index'

describe('createIoredisMock — cluster', () => {
  let cluster: Cluster

  before(async () => {
    cluster = (await createIoredisMock({
      cluster: { masters: 3 },
    })) as Cluster
  })

  after(async () => {
    await cluster.quit()
  })

  beforeEach(async () => {
    await Promise.all(cluster.nodes('master').map(node => node.flushall()))
  })

  test('routes keyed commands to the owning node (MOVED follow)', async () => {
    // Spread keys across slots; the client must follow MOVED to land each on
    // its owning master.
    await cluster.set('alpha', '1')
    await cluster.set('beta', '2')
    await cluster.set('gamma', '3')

    assert.strictEqual(await cluster.get('alpha'), '1')
    assert.strictEqual(await cluster.get('beta'), '2')
    assert.strictEqual(await cluster.get('gamma'), '3')
  })

  test('hash-tagged keys share a slot and allow multi-key ops', async () => {
    await cluster.mset('{tag}:a', '1', '{tag}:b', '2')
    assert.deepStrictEqual(await cluster.mget('{tag}:a', '{tag}:b'), ['1', '2'])
  })

  test('cross-slot multi-key op is rejected with CROSSSLOT', async () => {
    await assert.rejects(() => cluster.mget('k1', 'k2', 'k3'), /CROSSSLOT/)
  })

  test('discovers all masters from CLUSTER SLOTS', async () => {
    assert.strictEqual(cluster.nodes('master').length, 3)
  })
})
