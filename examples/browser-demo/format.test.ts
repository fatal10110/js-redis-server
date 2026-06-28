import { test, describe } from 'node:test'
import assert from 'node:assert'
import { tokenize, formatReply } from './format'

describe('tokenize', () => {
  test('splits on whitespace', () => {
    assert.deepStrictEqual(tokenize('SET k v'), ['SET', 'k', 'v'])
  })

  test('keeps a double-quoted token whole', () => {
    assert.deepStrictEqual(tokenize('EVAL "a b" 1 k'), [
      'EVAL',
      'a b',
      '1',
      'k',
    ])
  })

  test('handles escaped quotes inside quotes', () => {
    assert.deepStrictEqual(tokenize('SET k "a\\"b"'), ['SET', 'k', 'a"b'])
  })

  test('preserves an empty quoted arg', () => {
    assert.deepStrictEqual(tokenize('SET k ""'), ['SET', 'k', ''])
  })
})

describe('formatReply', () => {
  test('integer', () => {
    assert.strictEqual(formatReply(2), '(integer) 2')
  })

  test('nil', () => {
    assert.strictEqual(formatReply(null), '(nil)')
  })

  test('string is quoted', () => {
    assert.strictEqual(formatReply('world'), '"world"')
  })

  test('empty array', () => {
    assert.strictEqual(formatReply([]), '(empty array)')
  })

  test('flat array is numbered', () => {
    assert.strictEqual(formatReply(['a', 'b']), '1) "a"\n2) "b"')
  })

  test('nested array indents continuation lines', () => {
    assert.strictEqual(
      formatReply(['message', ['x', 'y']]),
      '1) "message"\n2) 1) "x"\n   2) "y"',
    )
  })
})
