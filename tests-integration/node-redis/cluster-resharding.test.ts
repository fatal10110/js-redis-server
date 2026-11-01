import { after, before, describe, test } from 'node:test'
import assert from 'node:assert'
import clusterKeySlot from 'cluster-key-slot'
import { RedisClusterType } from 'redis'
import { TestRunner } from '../test-config'
import { randomKey } from '../utils'

/**
 * #85: a slot resharded under a live node-redis cluster client. While the
 * slot is open (CLUSTER SETSLOT IMPORTING on the target, MIGRATING on the
 * source) the source serves the keys it still holds and answers `-ASK` for
 * the others; node-redis follows it (ASKING, then the command, on the
 * target). Once SETSLOT NODE hands the slot over, the client follows the
 * `-MOVED`. The keys move by GET / DEL / SET: the SET of a key the source no
 * longer holds is itself ASKed to the target. The slot is moved back the same
 * way at the end, so the cluster is left as it was.
 */
const testRunner = new TestRunner()

type SlotRange = [number, number, [string, number, string, ...unknown[]]]

describe(`Cluster resharding with ASK (node-redis, ${testRunner.getBackendName()})`, () => {
  let cluster: RedisClusterType

  before(async () => {
    cluster = (await testRunner.setupNodeRedisCluster()) as RedisClusterType
  })

  after(async () => {
    await testRunner.cleanup()
  })

  /** A key whose slot is in `range` and is not `avoid`. */
  function keyIn([min, max]: SlotRange, avoid: number): string {
    for (;;) {
      const key = `reshard:${randomKey()}`
      const slot = clusterKeySlot(key)
      if (slot >= min && slot <= max && slot !== avoid) {
        return key
      }
    }
  }

  test('the client follows ASK during the move and MOVED after it', async () => {
    const tag = `{reshard:${randomKey()}}`
    const slot = String(clusterKeySlot(tag))
    const keys = [`${tag}a`, `${tag}b`]
    const slots = (await cluster.sendCommand(undefined, true, [
      'CLUSTER',
      'SLOTS',
    ])) as SlotRange[]
    const sourceRange = slots.find(
      ([min, max]) => min <= Number(slot) && Number(slot) <= max,
    )
    assert.ok(sourceRange)
    const sourceId = sourceRange[2][2]
    const targetRange = slots.find(range => range[2][2] !== sourceId)
    assert.ok(targetRange)
    const targetId = targetRange[2][2]
    // Keyless commands go to the owner of the key they are sent with.
    const sourceKey = keyIn(sourceRange, Number(slot))
    const targetKey = keyIn(targetRange, Number(slot))
    const onSource = (...args: string[]) =>
      cluster.sendCommand(sourceKey, false, args)
    const onTarget = (...args: string[]) =>
      cluster.sendCommand(targetKey, false, args)
    const countOn = async (node: typeof onSource) =>
      Number(await node('CLUSTER', 'COUNTKEYSINSLOT', slot))

    assert.strictEqual(await onSource('CLUSTER', 'MYID'), sourceId)
    assert.strictEqual(await onTarget('CLUSTER', 'MYID'), targetId)

    /** Move the slot's keys and ownership from `from` to `to`. */
    const reshard = async (
      from: { node: typeof onSource; id: string },
      to: { node: typeof onSource; id: string },
    ) => {
      assert.strictEqual(
        await to.node('CLUSTER', 'SETSLOT', slot, 'IMPORTING', from.id),
        'OK',
      )
      assert.strictEqual(
        await from.node('CLUSTER', 'SETSLOT', slot, 'MIGRATING', to.id),
        'OK',
      )
      for (const key of keys) {
        const value = await cluster.get(key)
        assert.strictEqual(await cluster.del(key), 1)
        // `from` no longer holds the key: ASK to `to`.
        assert.strictEqual(await cluster.set(key, value ?? ''), 'OK')
        assert.strictEqual(await cluster.get(key), value)
      }
      assert.strictEqual(await countOn(from.node), 0)
      assert.strictEqual(
        await to.node('CLUSTER', 'SETSLOT', slot, 'NODE', to.id),
        'OK',
      )
      assert.strictEqual(
        await from.node('CLUSTER', 'SETSLOT', slot, 'NODE', to.id),
        'OK',
      )
    }

    const source = { node: onSource, id: sourceId }
    const target = { node: onTarget, id: targetId }
    assert.strictEqual(await cluster.set(keys[0], 'one'), 'OK')
    assert.strictEqual(await cluster.set(keys[1], 'two'), 'OK')
    let open = false
    try {
      assert.strictEqual(
        await onTarget('CLUSTER', 'SETSLOT', slot, 'IMPORTING', sourceId),
        'OK',
      )
      assert.strictEqual(
        await onSource('CLUSTER', 'SETSLOT', slot, 'MIGRATING', targetId),
        'OK',
      )
      open = true

      // A key still on the source is served there; a new one is ASKed to
      // the target, and read back from it the same way.
      assert.strictEqual(await cluster.get(keys[0]), 'one')
      assert.strictEqual(await cluster.set(`${tag}new`, 'n'), 'OK')
      assert.strictEqual(await cluster.get(`${tag}new`), 'n')
      assert.strictEqual(await countOn(onTarget), 1)
      assert.strictEqual(await cluster.del(`${tag}new`), 1)
      assert.strictEqual(await countOn(onTarget), 0)

      // Undo the opening: `reshard` opens the slot itself.
      assert.strictEqual(
        await onTarget('CLUSTER', 'SETSLOT', slot, 'STABLE'),
        'OK',
      )
      assert.strictEqual(
        await onSource('CLUSTER', 'SETSLOT', slot, 'STABLE'),
        'OK',
      )
      open = false

      await reshard(source, target)
      assert.strictEqual(await countOn(onTarget), 2)
      assert.strictEqual(await cluster.get(keys[0]), 'one')
      assert.strictEqual(await cluster.get(keys[1]), 'two')

      await reshard(target, source)
      assert.strictEqual(await countOn(onSource), 2)
      assert.deepStrictEqual(await cluster.mGet(keys), ['one', 'two'])
    } finally {
      if (open) {
        await onTarget('CLUSTER', 'SETSLOT', slot, 'STABLE')
        await onSource('CLUSTER', 'SETSLOT', slot, 'STABLE')
      }
      await cluster.del(keys)
    }
  })
})
