import { describe, test } from 'node:test'
import assert from 'node:assert'
import { ClusterNetwork, computeSlotRange } from '../src/core/cluster/network'

describe('ClusterNetwork slot distribution', () => {
  test('computeSlotRange covers all slots without overlap', () => {
    const masters = 5
    const ranges = Array.from({ length: masters }, (_, i) =>
      computeSlotRange(i, masters),
    )

    assert.strictEqual(ranges[0][0], 0)
    assert.strictEqual(ranges[masters - 1][1], 16383)

    let totalSlots = 0
    for (let i = 0; i < ranges.length; i++) {
      const [start, end] = ranges[i]
      totalSlots += end - start + 1
      if (i > 0) {
        assert.strictEqual(start, ranges[i - 1][1] + 1)
      }
    }

    assert.strictEqual(totalSlots, 16384)
  })

  test('computeSlotRange rejects invalid inputs', () => {
    assert.throws(() => computeSlotRange(0, 0), /Invalid masters count/)
    assert.throws(() => computeSlotRange(-1, 3), /Invalid master index/)
    assert.throws(() => computeSlotRange(3, 3), /Invalid master index/)
  })
})

describe('ClusterNetwork validation', () => {
  test('getMaster rejects invalid node ids', () => {
    const logger = { info: () => {}, error: () => {} }
    const network = new ClusterNetwork(logger)

    assert.throws(() => network.getMaster('replica-bad-master-x'), /Invalid/)
  })
})
