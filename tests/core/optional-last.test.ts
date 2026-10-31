import { describe, test } from 'node:test'
import assert from 'node:assert'
import { parseCommandArgs, schemaArity, t } from '../../src/core/command-schema'
import { resolveCompatibilityProfile } from '../../src/core/compatibility'

const SCHEMA = t.object({
  key: t.key(),
  count: t.optionalLast(t.positiveCount()),
})

function parse(...args: string[]) {
  return parseCommandArgs(
    SCHEMA,
    args.map(arg => Buffer.from(arg)),
    'spop',
    resolveCompatibilityProfile('redis-8.0'),
  )
}

describe('t.optionalLast (#619)', () => {
  test('parses an absent or a single trailing argument', () => {
    assert.deepStrictEqual(parse('k'), {
      key: Buffer.from('k'),
      count: undefined,
    })
    assert.deepStrictEqual(parse('k', '3'), { key: Buffer.from('k'), count: 3 })
  })

  test('answers syntax error for any further token before parsing the argument', () => {
    for (const extra of [
      ['1', '2'],
      ['x', '2'],
      ['-1', '2'],
      ['1', '2', '3'],
    ]) {
      assert.throws(() => parse('k', ...extra), { message: 'syntax error' })
    }
  })

  test('keeps the bad-argument error and the minimum arity', () => {
    assert.throws(() => parse('k', 'x'), {
      message: 'value is out of range, must be positive',
    })
    assert.throws(() => parse(), {
      message: "wrong number of arguments for 'spop' command",
    })
    assert.strictEqual(schemaArity(SCHEMA), -2)
  })
})
