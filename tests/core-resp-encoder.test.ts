import { describe, test } from 'node:test'
import assert from 'node:assert'
import {
  RedisResult,
  RedisValue,
  encodeRedisResult,
  encodeRedisValue,
} from '../src/internal'

describe('RESP encoder core', () => {
  test('encodes RESP2 scalar values', () => {
    assert.deepStrictEqual(
      encodeRedisValue(RedisValue.simpleString('OK')),
      Buffer.from('+OK\r\n'),
    )
    assert.deepStrictEqual(
      encodeRedisValue(RedisValue.bulkString(Buffer.from('value'))),
      Buffer.from('$5\r\nvalue\r\n'),
    )
    assert.deepStrictEqual(
      encodeRedisValue(RedisValue.bulkString(null)),
      Buffer.from('$-1\r\n'),
    )
    assert.deepStrictEqual(
      encodeRedisValue(RedisValue.integer(42)),
      Buffer.from(':42\r\n'),
    )
    assert.deepStrictEqual(
      encodeRedisValue(RedisValue.error('broken', 'ERR')),
      Buffer.from('-ERR broken\r\n'),
    )
  })

  test('formats RESP2 double edge values and sanitizes error lines', () => {
    assert.deepStrictEqual(
      encodeRedisValue(RedisValue.double(Infinity)),
      Buffer.from('$3\r\ninf\r\n'),
    )
    assert.deepStrictEqual(
      encodeRedisValue(RedisValue.double(-Infinity)),
      Buffer.from('$4\r\n-inf\r\n'),
    )
    assert.deepStrictEqual(
      encodeRedisValue(RedisValue.double(Number.NaN)),
      Buffer.from('$3\r\nnan\r\n'),
    )
    assert.deepStrictEqual(
      encodeRedisValue(RedisValue.error('bad\r\nframe', 'ERR')),
      Buffer.from('-ERR bad frame\r\n'),
    )
  })

  test('downgrades RESP3-only shapes to deterministic RESP2 arrays/bulk values', () => {
    assert.deepStrictEqual(
      encodeRedisValue(
        RedisValue.map([
          [RedisValue.bulkString(Buffer.from('a')), RedisValue.integer(1)],
          [RedisValue.bulkString(Buffer.from('b')), RedisValue.boolean(true)],
        ]),
      ),
      Buffer.from('*4\r\n$1\r\na\r\n:1\r\n$1\r\nb\r\n:1\r\n'),
    )

    assert.deepStrictEqual(
      encodeRedisValue(
        RedisValue.mapPairs([
          [RedisValue.bulkString(Buffer.from('a')), RedisValue.integer(1)],
          [RedisValue.bulkString(Buffer.from('b')), RedisValue.boolean(true)],
        ]),
      ),
      Buffer.from('*2\r\n*2\r\n$1\r\na\r\n:1\r\n*2\r\n$1\r\nb\r\n:1\r\n'),
    )

    assert.deepStrictEqual(
      encodeRedisValue(
        RedisValue.push('message', [RedisValue.bulkString(Buffer.from('x'))]),
      ),
      Buffer.from('*2\r\n$7\r\nmessage\r\n$1\r\nx\r\n'),
    )

    assert.deepStrictEqual(
      encodeRedisValue(RedisValue.bigNumber(9007199254740993n)),
      Buffer.from('$16\r\n9007199254740993\r\n'),
    )
  })

  test('encodes RedisResult values and RESP3-native shapes', () => {
    assert.deepStrictEqual(
      encodeRedisResult(RedisResult.ok()),
      Buffer.from('+OK\r\n'),
    )

    assert.deepStrictEqual(
      encodeRedisResult(RedisResult.ok(), { version: 3 }),
      Buffer.from('+OK\r\n'),
    )
    assert.deepStrictEqual(
      encodeRedisValue(RedisValue.null(), { version: 3 }),
      Buffer.from('_\r\n'),
    )
    assert.deepStrictEqual(
      encodeRedisValue(RedisValue.boolean(false), { version: 3 }),
      Buffer.from('#f\r\n'),
    )
    assert.deepStrictEqual(
      encodeRedisValue(
        RedisValue.map([
          [RedisValue.bulkString(Buffer.from('a')), RedisValue.integer(1)],
          [RedisValue.bulkString(Buffer.from('b')), RedisValue.null()],
        ]),
        { version: 3 },
      ),
      Buffer.from('%2\r\n$1\r\na\r\n:1\r\n$1\r\nb\r\n_\r\n'),
    )
    assert.deepStrictEqual(
      encodeRedisValue(
        RedisValue.mapPairs([
          [RedisValue.bulkString(Buffer.from('a')), RedisValue.integer(1)],
          [RedisValue.bulkString(Buffer.from('b')), RedisValue.null()],
        ]),
        { version: 3 },
      ),
      Buffer.from('%2\r\n$1\r\na\r\n:1\r\n$1\r\nb\r\n_\r\n'),
    )
    assert.deepStrictEqual(
      encodeRedisValue(
        RedisValue.push('message', [RedisValue.bulkString(Buffer.from('x'))]),
        { version: 3 },
      ),
      Buffer.from('>2\r\n$7\r\nmessage\r\n$1\r\nx\r\n'),
    )
  })

  test('writes pre-encoded RedisResult bytes verbatim', () => {
    const encoded = Buffer.from('*1\r\n_\r\n')
    const result = RedisResult.preEncoded(
      RedisValue.array([RedisValue.null()]),
      encoded,
    )
    encoded.write('$', 4)

    assert.deepStrictEqual(
      encodeRedisResult(result, { version: 2 }),
      Buffer.from('*1\r\n_\r\n'),
    )
  })

  test('encodes RESP3 verbatim strings with a 3-byte format prefix', () => {
    assert.deepStrictEqual(
      encodeRedisValue(RedisValue.verbatim('txt', Buffer.from('Some string')), {
        version: 3,
      }),
      Buffer.from('=15\r\ntxt:Some string\r\n'),
    )
    assert.deepStrictEqual(
      encodeRedisValue(RedisValue.verbatim('mkd', Buffer.from('# title')), {
        version: 3,
      }),
      Buffer.from('=11\r\nmkd:# title\r\n'),
    )
  })

  test('downgrades RESP3 verbatim strings to a plain RESP2 bulk string', () => {
    assert.deepStrictEqual(
      encodeRedisValue(RedisValue.verbatim('txt', Buffer.from('Some string'))),
      Buffer.from('$11\r\nSome string\r\n'),
    )
  })

  test('rejects RESP3 verbatim strings whose format is not exactly 3 bytes', () => {
    for (const badFormat of ['', 'tx', 'text', 'txt ']) {
      assert.throws(
        () =>
          encodeRedisValue(
            RedisValue.verbatim(badFormat, Buffer.from('payload')),
            { version: 3 },
          ),
        /format must be exactly 3 bytes/,
        `format "${badFormat}" should be rejected`,
      )
    }
  })
})
