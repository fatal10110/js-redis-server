import { describe, test } from 'node:test'
import assert from 'node:assert'
import {
  RedisResult,
  RedisValue,
  encodeRedisResult,
  encodeRedisValue,
} from '../src'

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
        RedisValue.push('message', [RedisValue.bulkString(Buffer.from('x'))]),
      ),
      Buffer.from('*2\r\n$7\r\nmessage\r\n$1\r\nx\r\n'),
    )

    assert.deepStrictEqual(
      encodeRedisValue(RedisValue.bigNumber(9007199254740993n)),
      Buffer.from('$16\r\n9007199254740993\r\n'),
    )
  })

  test('encodes RedisResult values and rejects RESP3 until implemented', () => {
    assert.deepStrictEqual(
      encodeRedisResult(RedisResult.ok()),
      Buffer.from('+OK\r\n'),
    )

    assert.throws(
      () => encodeRedisResult(RedisResult.ok(), { version: 3 }),
      /RESP3 encoding is not implemented yet/,
    )
  })
})
