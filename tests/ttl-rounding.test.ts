import { test, describe } from 'node:test'
import assert from 'node:assert'
import {
  hashFieldTtlSeconds,
  keyTtlSeconds,
  ttlMilliseconds,
} from '../src/commands/helpers'

// Pins the seconds-granularity rounding rules against an injected clock, so
// the boundaries are checked deterministically rather than by wall-clock
// integration tests (#432).
const NOW = 1_700_000_000_000

describe('TTL rounding helpers', () => {
  test('hashFieldTtlSeconds rounds remaining time up (HTTL)', () => {
    const cases: Array<[remainingMs: number, expected: number]> = [
      [1, 1],
      [999, 1],
      [1000, 1],
      [1001, 2],
      [1999, 2],
      [2000, 2],
      [2001, 3],
      [0, 0],
      [-1, 0],
      [-1500, 0],
    ]

    for (const [remainingMs, expected] of cases) {
      assert.strictEqual(
        hashFieldTtlSeconds(NOW + remainingMs, NOW),
        expected,
        `remaining ${remainingMs}ms`,
      )
    }
  })

  test('keyTtlSeconds rounds remaining time to the nearest second (TTL)', () => {
    const cases: Array<[remainingMs: number, expected: number]> = [
      [1, 0],
      [499, 0],
      [500, 1],
      [1499, 1],
      [1500, 2],
      [0, 0],
      [-1, 0],
      [-1500, 0],
    ]

    for (const [remainingMs, expected] of cases) {
      assert.strictEqual(
        keyTtlSeconds(NOW + remainingMs, NOW),
        expected,
        `remaining ${remainingMs}ms`,
      )
    }
  })

  test('ttlMilliseconds reports exact remaining time, clamped at 0', () => {
    assert.strictEqual(ttlMilliseconds(NOW + 1234, NOW), 1234)
    assert.strictEqual(ttlMilliseconds(NOW, NOW), 0)
    assert.strictEqual(ttlMilliseconds(NOW - 5, NOW), 0)
  })
})
