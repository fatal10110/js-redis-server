import { describe, test } from 'node:test'
import assert from 'node:assert'
import {
  RedisResult,
  RedisValue,
  encodeRedisResult,
  encodeRedisValue,
} from '../src/internal'
import { formatRedisDouble } from '../src/core/resp-encoder'

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
    // `sdsmapchars(s, "\r\n", "  ", 2)` in real Redis is a 1:1 character map,
    // so a `\r\n` run becomes two spaces rather than being collapsed to one
    // (#388). Pinned against redis-server 7.0.15 and 8.0.6.
    assert.deepStrictEqual(
      encodeRedisValue(RedisValue.error('bad\r\nframe', 'ERR')),
      Buffer.from('-ERR bad  frame\r\n'),
    )
    assert.deepStrictEqual(
      encodeRedisValue(RedisValue.error('one\nline', 'ERR')),
      Buffer.from('-ERR one line\r\n'),
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

  test('flat-pairs is a flat array on RESP2 and nested pairs on RESP3', () => {
    const withScores = RedisValue.flatPairs([
      [RedisValue.bulkString(Buffer.from('one')), RedisValue.double(1)],
      [RedisValue.bulkString(Buffer.from('two')), RedisValue.double(2.5)],
    ])

    // RESP2: flat [member, score, ...] with scores as bulk strings.
    assert.deepStrictEqual(
      encodeRedisValue(withScores),
      Buffer.from('*4\r\n$3\r\none\r\n$1\r\n1\r\n$3\r\ntwo\r\n$3\r\n2.5\r\n'),
    )

    // RESP3: array of [member, double-score] pairs.
    assert.deepStrictEqual(
      encodeRedisValue(withScores, { version: 3 }),
      Buffer.from('*2\r\n*2\r\n$3\r\none\r\n,1\r\n*2\r\n$3\r\ntwo\r\n,2.5\r\n'),
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

describe('formatRedisDouble', () => {
  // Every expected value is `ZSCORE` output at RESP2 from real Redis 8.0.6 and
  // 7.2.1 (identical), after `ZADD z <input> m`. RESP3's `,` double carries
  // the same text — `d2string()` produces both.
  // A `Number('…')` input is the ZADD argument as sent, parsed the way Redis's
  // strtod parses it — a literal would lose precision before the test runs.
  const cases: [input: number, redis: string][] = [
    // Integer path: every digit, exactly, up to ±2^62 — JS's toString would
    // round the tail.
    [1, '1'],
    [1e18, '1000000000000000000'],
    [3e18, '3000000000000000000'],
    [2 ** 62, '4611686018427387904'],
    [-(2 ** 62), '-4611686018427387904'],
    [Number('1234567890123456789'), '1234567890123456768'],
    // fpconv past 2^62: plain while the trailing zeros number fewer than 8…
    [4611686018427388928, '4611686018427389000'],
    [12345678901234567000, '12345678901234567000'],
    [Number('1.2345678901234567e22'), '12345678901234568000000'],
    // …exponent form otherwise, whatever the magnitude.
    [5e18, '5e+18'],
    [-5e18, '-5e+18'],
    [1e19, '1e+19'],
    [1e20, '1e+20'],
    [Number('99999999999999999999'), '1e+20'],
    [9.99e20, '9.99e+20'],
    [1e21, '1e+21'],
    [1.7976931348623157e308, '1.7976931348623157e+308'],
    // Fractions: a plain decimal while the last digit sits fewer than 7 places
    // after the point, or the magnitude is below 10^4…
    [2.5, '2.5'],
    [0.1, '0.1'],
    [123.456, '123.456'],
    [123.4567891, '123.4567891'],
    [0.000001, '0.000001'],
    [0.00001, '0.00001'],
    [0.000123, '0.000123'],
    // …exponent form otherwise, even where JS would still print a decimal.
    [0.0000123, '1.23e-5'],
    [1.23456e-5, '1.23456e-5'],
    [0.0001234567, '1.234567e-4'],
    [1e-7, '1e-7'],
    [1.5e-7, '1.5e-7'],
    [1e-300, '1e-300'],
    [5e-324, '5e-324'],
    // Specials.
    [Infinity, 'inf'],
    [-Infinity, '-inf'],
    [Number.NaN, 'nan'],
    [0, '0'],
    [-0, '-0'],
  ]

  for (const [input, redis] of cases) {
    test(`${String(input)} → ${redis}`, () => {
      assert.strictEqual(formatRedisDouble(input), redis)
    })
  }

  test('reaches the wire at both protocols', () => {
    assert.deepStrictEqual(
      encodeRedisValue(RedisValue.double(1e20)),
      Buffer.from('$5\r\n1e+20\r\n'),
    )
    assert.deepStrictEqual(
      encodeRedisValue(RedisValue.double(1e20), { version: 3 }),
      Buffer.from(',1e+20\r\n'),
    )
  })
})
