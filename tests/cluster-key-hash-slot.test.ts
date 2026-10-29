import { describe, test } from 'node:test'
import assert from 'node:assert'
import clusterKeySlot from 'cluster-key-slot'
import {
  keyHashSlot,
  RedisClusterTopology,
} from '../src/state/cluster-topology'

/**
 * `CLUSTER KEYSLOT` replies captured from a real Redis 7.0.15 started with
 * `--cluster-enabled yes`. `keyHashSlot()` is the same code in Redis 6.2,
 * 7.x and 8.x and in Valkey 8.0 and 9.0.
 */
const REAL_SLOTS: ReadonlyArray<[key: string | Buffer, slot: number]> = [
  ['', 0],
  ['123456789', 12739],
  ['foo', 12182],
  ['{foo}', 12182],
  ['{foo}{bar}', 12182],
  ['a{b}c', 3300],
  ['{user1000}.following', 3443],
  // An empty first tag hashes the whole key; a later `{...}` is never the tag.
  ['{}{foo}', 2263],
  ['foo{}{bar}', 8363],
  ['foo{}bar{zap}', 15239],
  ['{}', 15257],
  ['{}foo', 9500],
  ['x{}', 2608],
  // An unterminated tag hashes the whole key.
  ['foo{bar', 15278],
  ['{', 4092],
  // A `}` before the first `{` is not a tag end.
  ['}', 12090],
  ['foo}bar{x', 11158],
  // The tag ends at the first `}`, and may itself contain `{`.
  ['{{foo}}', 13308],
  ['foo{{bar}}zap', 4015],
  // Bytes, not UTF-16 code units.
  ['ключ{тег}', 14548],
  ['тег', 14548],
  [Buffer.from([0xff, 0x00, 0x7b, 0x80, 0x7d]), 4488],
  [Buffer.from([0xff, 0x00, 0x7b, 0x7d, 0x7b, 0x80, 0x7d]), 13216],
]

describe('keyHashSlot (#88)', () => {
  for (const [key, slot] of REAL_SLOTS) {
    test(`${JSON.stringify(key.toString())} hashes to slot ${slot}`, () => {
      assert.strictEqual(keyHashSlot(Buffer.from(key)), slot)
    })
  }

  test('cluster-key-slot, which the clients route with, disagrees on {}{foo}', () => {
    // Pins why ioredis / node-redis get a MOVED for such keys, against this
    // server and against a real cluster alike.
    assert.strictEqual(clusterKeySlot('{}{foo}'), 13308)
    assert.strictEqual(keyHashSlot(Buffer.from('{}{foo}')), 2263)
  })
})

describe('RedisClusterTopology slot helpers', () => {
  const topology = new RedisClusterTopology()

  test('calculateSlot uses keyHashSlot', () => {
    assert.strictEqual(topology.calculateSlot(Buffer.from('{}{foo}')), 2263)
  })

  test('calculateSlotForKeys returns null for no keys', () => {
    assert.strictEqual(topology.calculateSlotForKeys([]), null)
  })

  test('calculateSlotForKeys returns the shared slot', () => {
    assert.strictEqual(
      topology.calculateSlotForKeys([
        Buffer.from('{foo}a'),
        Buffer.from('{foo}b'),
        Buffer.from('foo'),
      ]),
      12182,
    )
  })

  test('calculateSlotForKeys returns -1 across slots', () => {
    // `{}{foo}` and `{{foo}x` (tag `{foo`) share a slot only under the
    // cluster-key-slot package's rule.
    assert.strictEqual(
      topology.calculateSlotForKeys([
        Buffer.from('{}{foo}'),
        Buffer.from('{{foo}x'),
      ]),
      -1,
    )
    assert.strictEqual(
      topology.calculateSlotForKeys([
        Buffer.from('{foo}'),
        Buffer.from('{bar}'),
      ]),
      -1,
    )
  })
})
