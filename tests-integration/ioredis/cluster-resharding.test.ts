import { after, before, describe, test } from 'node:test'
import assert from 'node:assert'
import clusterKeySlot from 'cluster-key-slot'
import type { Cluster, Redis } from 'ioredis'
import { TestRunner } from '../test-config'
import { randomKey } from '../utils'

/**
 * #85: a slot resharded under a live ioredis cluster client. While the slot
 * is open (CLUSTER SETSLOT IMPORTING on the target, MIGRATING on the source)
 * the source serves the keys it still holds and answers `-ASK` for the
 * others; ioredis follows it (ASKING, then the command, on the target). Once
 * SETSLOT NODE hands the slot over, the client follows the `-MOVED`. Keys
 * move with DUMP on the source, RESTORE-ASKING on the target (served there
 * without ASKING) and DEL on the source. The slot is moved back the same way
 * at the end, so the cluster is left as it was.
 */
const testRunner = new TestRunner()

type SlotRange = [number, number, [string, number, string, ...unknown[]]]

describe(`Cluster resharding with ASK (ioredis, ${testRunner.getBackendName()})`, () => {
  let cluster: Cluster

  before(async () => {
    // No key prefix: the nodes' GETKEYSINSLOT / DUMP see the bare names.
    cluster = await testRunner.setupIoredisCluster()
  })

  after(async () => {
    await testRunner.cleanup()
  })

  test('the client follows ASK during the move and MOVED after it', async () => {
    const tag = `{reshard:${randomKey()}}`
    const slot = clusterKeySlot(tag)
    const keys = [`${tag}a`, `${tag}b`]
    const slots = (await cluster.cluster('SLOTS')) as unknown as SlotRange[]
    const sourceRange = slots.find(([min, max]) => min <= slot && slot <= max)
    assert.ok(sourceRange)
    const targetRange = slots.find(range => range[2][2] !== sourceRange[2][2])
    assert.ok(targetRange)
    const masters = cluster.nodes('master')
    const nodeOn = (range: SlotRange): Redis => {
      const node = masters.find(
        master => master.options.port === Number(range[2][1]),
      )
      assert.ok(node, `no ioredis node for port ${range[2][1]}`)
      return node
    }
    const source = { node: nodeOn(sourceRange), id: sourceRange[2][2] }
    const target = { node: nodeOn(targetRange), id: targetRange[2][2] }
    assert.strictEqual(await source.node.cluster('MYID'), source.id)
    assert.strictEqual(await target.node.cluster('MYID'), target.id)
    const count = (node: Redis) =>
      node.cluster('COUNTKEYSINSLOT', slot) as Promise<number>

    /** Move the slot's keys and ownership from `from` to `to`. */
    const reshard = async (from: typeof source, to: typeof source) => {
      assert.strictEqual(
        await to.node.cluster('SETSLOT', slot, 'IMPORTING', from.id),
        'OK',
      )
      assert.strictEqual(
        await from.node.cluster('SETSLOT', slot, 'MIGRATING', to.id),
        'OK',
      )
      const inSlot = (await from.node.cluster(
        'GETKEYSINSLOT',
        slot,
        100,
      )) as string[]
      assert.deepStrictEqual([...inSlot].sort(), [...keys].sort())
      for (const key of inSlot) {
        const payload = await from.node.dumpBuffer(key)
        assert.ok(payload)
        assert.strictEqual(
          await to.node.call('RESTORE-ASKING', key, '0', payload),
          'OK',
        )
        assert.strictEqual(await from.node.del(key), 1)
        // Gone from `from`: the client is ASKed to `to`.
        assert.ok(await cluster.get(key))
      }
      assert.strictEqual(await count(from.node), 0)
      assert.strictEqual(
        await to.node.cluster('SETSLOT', slot, 'NODE', to.id),
        'OK',
      )
      assert.strictEqual(
        await from.node.cluster('SETSLOT', slot, 'NODE', to.id),
        'OK',
      )
    }

    assert.strictEqual(await cluster.set(keys[0], 'one'), 'OK')
    assert.strictEqual(await cluster.set(keys[1], 'two'), 'OK')
    let open = false
    try {
      assert.strictEqual(
        await target.node.cluster('SETSLOT', slot, 'IMPORTING', source.id),
        'OK',
      )
      assert.strictEqual(
        await source.node.cluster('SETSLOT', slot, 'MIGRATING', target.id),
        'OK',
      )
      open = true

      // A key still on the source is served there; a new one is ASKed to
      // the target, and read back from it the same way.
      assert.strictEqual(await cluster.get(keys[0]), 'one')
      assert.strictEqual(await cluster.set(`${tag}new`, 'n'), 'OK')
      assert.strictEqual(await cluster.get(`${tag}new`), 'n')
      assert.strictEqual(await count(target.node), 1)
      assert.strictEqual(await cluster.del(`${tag}new`), 1)
      assert.strictEqual(await count(target.node), 0)

      // Undo the opening: `reshard` opens the slot itself.
      assert.strictEqual(
        await target.node.cluster('SETSLOT', slot, 'STABLE'),
        'OK',
      )
      assert.strictEqual(
        await source.node.cluster('SETSLOT', slot, 'STABLE'),
        'OK',
      )
      open = false

      await reshard(source, target)
      assert.strictEqual(await count(target.node), 2)
      assert.strictEqual(await cluster.get(keys[0]), 'one')
      assert.strictEqual(await cluster.get(keys[1]), 'two')

      await reshard(target, source)
      assert.strictEqual(await count(source.node), 2)
      assert.deepStrictEqual(await cluster.mget(keys), ['one', 'two'])
    } finally {
      if (open) {
        await target.node.cluster('SETSLOT', slot, 'STABLE')
        await source.node.cluster('SETSLOT', slot, 'STABLE')
      }
      await cluster.del(...keys)
    }
  })
})
