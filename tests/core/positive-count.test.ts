import { describe, test } from 'node:test'
import assert from 'node:assert'
import {
  parseClampedLongToken,
  parseCommandArgs,
  t,
} from '../../src/core/command-schema'
import { resolveCompatibilityProfile } from '../../src/core/compatibility'

const POSITIVE = { message: 'value is out of range, must be positive' }

function parseCount(raw: string): number {
  return parseCommandArgs(
    t.positiveCount(),
    [Buffer.from(raw)],
    'lpop',
    resolveCompatibilityProfile('redis-8.0'),
  )
}

describe('pop counts read like a C long (#610)', () => {
  test('parseClampedLongToken follows string2ll and clamps to safe integers', () => {
    assert.strictEqual(parseClampedLongToken('0'), 0)
    assert.strictEqual(parseClampedLongToken('-5'), -5)
    assert.strictEqual(
      parseClampedLongToken('9223372036854775807'),
      Number.MAX_SAFE_INTEGER,
    )
    assert.strictEqual(
      parseClampedLongToken('-9223372036854775808'),
      Number.MIN_SAFE_INTEGER,
    )
    for (const raw of [
      '9223372036854775808',
      '-9223372036854775809',
      '-0',
      '+1',
      '01',
      ' 1',
      '1.5',
      '0x1',
      '',
    ]) {
      assert.strictEqual(parseClampedLongToken(raw), undefined, raw)
    }
  })

  test('t.positiveCount answers the positive wording for every bad token', () => {
    assert.strictEqual(parseCount('3'), 3)
    assert.strictEqual(parseCount('0'), 0)
    assert.strictEqual(
      parseCount('9223372036854775807'),
      Number.MAX_SAFE_INTEGER,
    )
    for (const raw of ['x', '1.5', '-1', '-0', '9223372036854775808']) {
      assert.throws(() => parseCount(raw), POSITIVE, raw)
    }
  })
})
