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

  test('keeps a single-quoted token whole', () => {
    assert.deepStrictEqual(tokenize("eval 'return 1' 0"), [
      'eval',
      'return 1',
      '0',
    ])
  })

  test('treats an escaped single quote as a literal inside single quotes', () => {
    assert.deepStrictEqual(tokenize("SET k 'a\\'b'"), ['SET', 'k', "a'b"])
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

  // Pinned against real `redis-cli --no-raw` (its sdscatrepr escaping).
  describe('Buffer reply', () => {
    test('printable ASCII is quoted as-is', () => {
      assert.strictEqual(
        formatReply(Buffer.from('hello world~')),
        '"hello world~"',
      )
    })

    test('non-ASCII bytes render as lowercase \\xNN', () => {
      assert.strictEqual(
        formatReply(Buffer.from('éé')),
        '"\\xc3\\xa9\\xc3\\xa9"',
      )
    })

    test('matches redis-cli for quotes, backslash, C escapes, NUL and DEL', () => {
      const bytes = Buffer.from('a\\b"c\n\r\t\x07\x08\u00e9\x00\x7f~ ', 'utf8')
      assert.strictEqual(
        formatReply(bytes),
        String.raw`"a\\b\"c\n\r\t\a\b\xc3\xa9\x00\x7f~ "`,
      )
    })

    test('empty buffer is an empty quoted string', () => {
      assert.strictEqual(formatReply(Buffer.alloc(0)), '""')
    })

    test('inside an array, not treated as a map', () => {
      assert.strictEqual(
        formatReply(['k', Buffer.from([0xff])]),
        '1) "k"\n2) "\\xff"',
      )
    })
  })
})
