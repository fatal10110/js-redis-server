import { test, describe, afterEach } from 'node:test'
import assert from 'node:assert'
import {
  createNodeRedisMock,
  type NodeRedisMockClient,
  type NodeRedisMockCluster,
} from '../../src/client-mocks/node-redis-mock'
import { keyHashSlot } from '../../src/state/cluster-topology'

// #490: the node-redis cluster surface — `slots`, `masters`,
// `sendCommand(firstKey, isReadonly, args)` and a synchronous `duplicate()`
// sharing the keyspace — in the shapes of node-redis 6.0.0's
// `cluster/index.js` and `cluster/cluster-slots.js`.

const slotOf = (key: string) => keyHashSlot(Buffer.from(key))

describe('node-redis mock cluster: node-redis cluster API', () => {
  const clusters: NodeRedisMockCluster[] = []

  afterEach(async () => {
    // Duplicates first: the creator owns the nodes.
    while (clusters.length > 0) {
      await clusters.pop()?.quit()
    }
  })

  async function makeCluster(): Promise<NodeRedisMockCluster> {
    const cluster = (await createNodeRedisMock({
      cluster: { masters: 3 },
    })) as NodeRedisMockCluster
    clusters.push(cluster)
    return cluster
  }

  test('masters lists { id, host, port, address, readonly } per master', async () => {
    const cluster = await makeCluster()
    const masters = cluster.masters
    assert.strictEqual(masters.length, 3)
    for (const node of masters) {
      assert.deepStrictEqual(Object.keys(node).sort(), [
        'address',
        'host',
        'id',
        'port',
        'readonly',
      ])
      assert.strictEqual(node.address, `${node.host}:${node.port}`)
      assert.strictEqual(node.readonly, false)
    }
    // Each node has its own address.
    assert.strictEqual(new Set(masters.map(node => node.address)).size, 3)
    // The same objects on every read, as node-redis keeps them.
    assert.strictEqual(cluster.masters[0], masters[0])
  })

  test('slots maps every hash slot to the { master } shard owning it', async () => {
    const cluster = await makeCluster()
    const slots = cluster.slots
    assert.strictEqual(slots.length, 16384)
    const masters = new Set(cluster.masters)
    for (const shard of slots) {
      assert.ok(masters.has(shard.master))
      assert.deepStrictEqual(Object.keys(shard), ['master'])
    }
    // The slot owner is the node that answers for the key.
    for (const key of ['alpha', 'abc', 'k1']) {
      const owner = slots[slotOf(key)].master
      assert.strictEqual(
        await cluster.sendCommand(key, false, ['CLUSTER', 'MYID']),
        owner.id,
      )
    }
    assert.strictEqual(new Set(slots.map(shard => shard.master)).size, 3)
  })

  test('sendCommand(firstKey, isReadonly, args) runs the command', async () => {
    const cluster = await makeCluster()
    assert.strictEqual(
      await cluster.sendCommand('k', false, ['SET', 'k', 'v']),
      'OK',
    )
    assert.strictEqual(await cluster.sendCommand('k', true, ['GET', 'k']), 'v')
    // A keyed command reaches its own keys' owner whatever firstKey names,
    // as node-redis following MOVED does.
    assert.strictEqual(
      await cluster.sendCommand(undefined, true, ['GET', 'k']),
      'v',
    )
    assert.strictEqual(
      await cluster.sendCommand('alpha', false, ['GET', 'k']),
      'v',
    )
    // A keyless command runs on firstKey's slot owner.
    assert.strictEqual(await cluster.sendCommand('k', false, ['DBSIZE']), 1)
    const elsewhere = ['alpha', 'abc', 'k1'].find(
      key =>
        cluster.slots[slotOf(key)].master !== cluster.slots[slotOf('k')].master,
    )
    assert.ok(elsewhere)
    assert.strictEqual(
      await cluster.sendCommand(elsewhere, false, ['DBSIZE']),
      0,
    )
    // The old `sendCommand(args)` form is refused with a pointer to the new one.
    await assert.rejects(
      () =>
        (
          cluster as unknown as {
            sendCommand(args: string[]): Promise<unknown>
          }
        ).sendCommand(['GET', 'k']),
      TypeError,
    )
  })

  test('duplicate() returns a second client on the same keyspace, synchronously', async () => {
    const cluster = await makeCluster()
    const dup = cluster.duplicate()
    clusters.push(dup)
    assert.ok(!(dup instanceof Promise))
    assert.strictEqual(dup.isOpen, true)
    assert.strictEqual(await dup.connect(), dup)
    await cluster.set('shared', '1')
    assert.strictEqual(await dup.get('shared'), '1')
    assert.deepStrictEqual(dup.masters, cluster.masters)

    // Its own connection per node: a blocked pop there does not hold this one.
    const popped = dup.sendCommand('q', false, ['BLPOP', 'q', '0'])
    assert.strictEqual(await cluster.rPush('q', 'x'), 1)
    assert.deepStrictEqual(await popped, ['q', 'x'])
  })

  test('closing a duplicate leaves the creator and its keyspace working', async () => {
    const cluster = await makeCluster()
    const dup = cluster.duplicate({ RESP: 3 })
    await dup.set('k', 'v')
    await dup.quit()
    assert.strictEqual(dup.isOpen, false)
    assert.strictEqual(cluster.isOpen, true)
    assert.strictEqual(await cluster.get('k'), 'v')
    // A fresh duplicate still sees it.
    const again = cluster.duplicate()
    clusters.push(again)
    assert.strictEqual(await again.get('k'), 'v')
  })
})

describe('node-redis mock: standalone duplicate()', () => {
  test('returns an open client on the same keyspace synchronously', async () => {
    const base = (await createNodeRedisMock()) as NodeRedisMockClient
    const dup = base.duplicate()
    try {
      assert.ok(!(dup instanceof Promise))
      assert.strictEqual(dup.isOpen, true)
      assert.strictEqual(dup.isReady, true)
      assert.strictEqual(await dup.connect(), dup)
      await base.set('k', 'v')
      assert.strictEqual(await dup.get('k'), 'v')
    } finally {
      await dup.quit()
      assert.strictEqual(dup.isOpen, false)
      await base.quit()
    }
  })
})
