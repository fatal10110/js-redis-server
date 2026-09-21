import { test, describe } from 'node:test'
import assert from 'node:assert'
import {
  decodeRedisValue,
  decodeRedisKey,
  redisErrorText,
  toRedisArgument,
  type DecodeRedisValueOptions,
} from '../../src/core/decode-redis-value'
import { RedisCommandError } from '../../src/core/redis-error'
import type { RedisValue } from '../../src/core/redis-value'

// The node-redis facade and InMemoryRedisClient share this decoder and differ
// only through its options. Those differences are deliberate, so they are
// asserted here: flipping either client onto the other's option value must
// fail a test rather than pass silently.

const NODE_REDIS: DecodeRedisValueOptions = {
  narrowBigInt: 'always',
  pushShape: 'items',
  error: text => new Error(text),
}

const IN_MEMORY: DecodeRedisValueOptions = {
  narrowBigInt: 'when-safe',
  pushShape: 'tagged',
  error: (text, code) => new RedisCommandError(text, code),
}

const bulk = (value: string): RedisValue => ({
  kind: 'bulk-string',
  value: Buffer.from(value),
})

describe('decodeRedisValue divergences', () => {
  test('pushShape: node-redis drops the type tag, in-memory keeps it', () => {
    const push: RedisValue = {
      kind: 'push',
      name: 'message',
      items: [bulk('news'), bulk('hello')],
    }

    assert.deepStrictEqual(decodeRedisValue(push, NODE_REDIS), [
      'news',
      'hello',
    ])
    assert.deepStrictEqual(decodeRedisValue(push, IN_MEMORY), [
      'message',
      'news',
      'hello',
    ])
  })

  test('narrowBigInt: node-redis always narrows, in-memory widens past 2^53', () => {
    const unsafe: RedisValue = {
      kind: 'integer',
      value: BigInt(Number.MAX_SAFE_INTEGER) + 2n,
    }

    // node-redis parses `:` with plain JS number arithmetic — precision loss
    // included — so it is never a bigint.
    const narrowed = decodeRedisValue(unsafe, NODE_REDIS)
    assert.strictEqual(typeof narrowed, 'number')
    assert.strictEqual(narrowed, 9007199254740992)

    // The in-memory client keeps the exact value instead.
    assert.strictEqual(
      decodeRedisValue(unsafe, IN_MEMORY),
      BigInt(Number.MAX_SAFE_INTEGER) + 2n,
    )
  })

  test('narrowBigInt: a safe bigint is a number under both options', () => {
    const safe: RedisValue = { kind: 'integer', value: 42n }
    assert.strictEqual(decodeRedisValue(safe, NODE_REDIS), 42)
    assert.strictEqual(decodeRedisValue(safe, IN_MEMORY), 42)
  })

  test('error: the option picks the thrown class, with the wire text', () => {
    const error: RedisValue = {
      kind: 'error',
      code: 'WRONGTYPE',
      message: 'Operation against a key holding the wrong kind of value',
    }
    const text =
      'WRONGTYPE Operation against a key holding the wrong kind of value'

    assert.throws(
      () => decodeRedisValue(error, NODE_REDIS),
      (err: unknown) => {
        assert.ok(err instanceof Error)
        assert.ok(!(err instanceof RedisCommandError))
        assert.strictEqual(err.message, text)
        return true
      },
    )

    assert.throws(
      () => decodeRedisValue(error, IN_MEMORY),
      (err: unknown) => {
        assert.ok(err instanceof RedisCommandError)
        assert.strictEqual(err.message, text)
        assert.strictEqual(err.code, 'WRONGTYPE')
        return true
      },
    )
  })

  test('returnBuffers keeps bulk-string and verbatim replies as Buffers', () => {
    const options = { ...IN_MEMORY, returnBuffers: true }
    assert.deepStrictEqual(
      decodeRedisValue(bulk('v'), options),
      Buffer.from('v'),
    )
    assert.strictEqual(decodeRedisValue(bulk('v'), IN_MEMORY), 'v')
    // A null bulk-string is still null, not an empty Buffer.
    assert.strictEqual(
      decodeRedisValue({ kind: 'bulk-string', value: null }, options),
      null,
    )
  })

  test('map keys stay utf8 strings even with returnBuffers', () => {
    const map: RedisValue = {
      kind: 'map',
      entries: [[bulk('field'), bulk('value')]],
    }
    assert.deepStrictEqual(
      decodeRedisValue(map, { ...IN_MEMORY, returnBuffers: true }),
      { field: Buffer.from('value') },
    )
  })
})

describe('decodeRedisKey', () => {
  test('stringifies scalar key kinds and falls back to empty', () => {
    assert.strictEqual(decodeRedisKey(bulk('f')), 'f')
    assert.strictEqual(
      decodeRedisKey({ kind: 'simple-string', value: 'OK' }),
      'OK',
    )
    assert.strictEqual(decodeRedisKey({ kind: 'integer', value: 7 }), '7')
    assert.strictEqual(decodeRedisKey({ kind: 'bulk-string', value: null }), '')
    assert.strictEqual(decodeRedisKey({ kind: 'null' }), '')
  })
})

describe('redisErrorText', () => {
  test('prefixes the code when there is one', () => {
    assert.strictEqual(
      redisErrorText({ code: 'MOVED', message: '1234 host:port' }),
      'MOVED 1234 host:port',
    )
    assert.strictEqual(redisErrorText({ message: 'bare' }), 'bare')
  })
})

describe('toRedisArgument', () => {
  test('passes Buffers through and stringifies strings and numbers', () => {
    const buffer = Buffer.from('b')
    assert.strictEqual(toRedisArgument(buffer), buffer)
    assert.deepStrictEqual(toRedisArgument('s'), Buffer.from('s'))
    // The `number` branch exists for InMemoryRedisClient only — the node-redis
    // facade keeps its own narrower coercion so it throws like the real client.
    assert.deepStrictEqual(toRedisArgument(5), Buffer.from('5'))
  })
})
