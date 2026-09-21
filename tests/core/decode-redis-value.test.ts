import { test, describe, before } from 'node:test'
import assert from 'node:assert'
import { ErrorReply } from 'redis'
import {
  createNodeRedisMock,
  NODE_REDIS_DECODE_OPTIONS,
} from '../../src/client-mocks/node-redis-mock'
import { IN_MEMORY_DECODE_OPTIONS } from '../../src/in-memory-client'
import {
  decodeRedisValue,
  decodeRedisKey,
  flatPairsShapeFor,
  redisErrorText,
  toRedisArgument,
} from '../../src/core/decode-redis-value'
import { RedisCommandError } from '../../src/core/redis-error'
import type { RedisValue } from '../../src/core/redis-value'

// The node-redis facade and InMemoryRedisClient share one decoder and differ
// only through its options. These import the option objects the clients
// *actually* pass — not local copies — so flipping either client onto the
// other's value fails here. That tripwire is the point of the file: the
// divergences are deliberate, and a silent re-convergence is what would
// otherwise go unnoticed.

const bulk = (value: string): RedisValue => ({
  kind: 'bulk-string',
  value: Buffer.from(value),
})

describe('decode option divergences between the two clients', () => {
  before(async () => {
    // Resolving node-redis' error classes is what createNodeRedisMock() does
    // before handing back a client, and NODE_REDIS_DECODE_OPTIONS.error falls
    // back to RedisCommandError until it has run.
    const client = await createNodeRedisMock()
    await client.quit()
  })

  test('pushShape: the facade drops the type tag, in-memory keeps it', () => {
    const push: RedisValue = {
      kind: 'push',
      name: 'message',
      items: [bulk('news'), bulk('hello')],
    }

    assert.strictEqual(NODE_REDIS_DECODE_OPTIONS.pushShape, 'items')
    assert.deepStrictEqual(decodeRedisValue(push, NODE_REDIS_DECODE_OPTIONS), [
      'news',
      'hello',
    ])

    assert.strictEqual(IN_MEMORY_DECODE_OPTIONS.pushShape, 'tagged')
    assert.deepStrictEqual(decodeRedisValue(push, IN_MEMORY_DECODE_OPTIONS), [
      'message',
      'news',
      'hello',
    ])
  })

  test('narrowBigInt: the facade always narrows, in-memory widens past 2^53', () => {
    const unsafe: RedisValue = {
      kind: 'integer',
      value: BigInt(Number.MAX_SAFE_INTEGER) + 2n,
    }

    // node-redis parses `:` with plain JS number arithmetic — precision loss
    // included — so it is never a bigint.
    assert.strictEqual(NODE_REDIS_DECODE_OPTIONS.narrowBigInt, 'always')
    const narrowed = decodeRedisValue(unsafe, NODE_REDIS_DECODE_OPTIONS)
    assert.strictEqual(typeof narrowed, 'number')
    assert.strictEqual(narrowed, 9007199254740992)

    // The in-memory client keeps the exact value instead.
    assert.strictEqual(IN_MEMORY_DECODE_OPTIONS.narrowBigInt, 'when-safe')
    assert.strictEqual(
      decodeRedisValue(unsafe, IN_MEMORY_DECODE_OPTIONS),
      BigInt(Number.MAX_SAFE_INTEGER) + 2n,
    )
  })

  test('narrowBigInt: a safe bigint is a number for both clients', () => {
    const safe: RedisValue = { kind: 'integer', value: 42n }
    assert.strictEqual(decodeRedisValue(safe, NODE_REDIS_DECODE_OPTIONS), 42)
    assert.strictEqual(decodeRedisValue(safe, IN_MEMORY_DECODE_OPTIONS), 42)
  })

  test('error: the facade throws node-redis ErrorReply, in-memory RedisCommandError', () => {
    const error: RedisValue = {
      kind: 'error',
      code: 'WRONGTYPE',
      message: 'Operation against a key holding the wrong kind of value',
    }
    const text =
      'WRONGTYPE Operation against a key holding the wrong kind of value'

    assert.throws(
      () => decodeRedisValue(error, NODE_REDIS_DECODE_OPTIONS),
      (err: unknown) => {
        // `instanceof ErrorReply` is node-redis' documented idiom.
        assert.ok(err instanceof ErrorReply)
        assert.ok(!(err instanceof RedisCommandError))
        assert.strictEqual(err.message, text)
        return true
      },
    )

    assert.throws(
      () => decodeRedisValue(error, IN_MEMORY_DECODE_OPTIONS),
      (err: unknown) => {
        assert.ok(err instanceof RedisCommandError)
        assert.ok(!(err instanceof ErrorReply))
        assert.strictEqual(err.message, text)
        assert.strictEqual(err.code, 'WRONGTYPE')
        return true
      },
    )
  })

  test('returnBuffers is the in-memory client alone, and off by default', () => {
    // The facade never sets it; the in-memory client layers it per connection.
    assert.strictEqual(NODE_REDIS_DECODE_OPTIONS.returnBuffers, undefined)
    assert.strictEqual(IN_MEMORY_DECODE_OPTIONS.returnBuffers, undefined)

    const options = { ...IN_MEMORY_DECODE_OPTIONS, returnBuffers: true }
    assert.deepStrictEqual(
      decodeRedisValue(bulk('v'), options),
      Buffer.from('v'),
    )
    assert.strictEqual(
      decodeRedisValue(bulk('v'), IN_MEMORY_DECODE_OPTIONS),
      'v',
    )
    // A null bulk-string is still null, not an empty Buffer.
    assert.strictEqual(
      decodeRedisValue({ kind: 'bulk-string', value: null }, options),
      null,
    )
  })

  test('flatPairsShape is RESP2-flat on both constants and per-protocol at decode time', () => {
    // Unlike the other knobs this one is not a per-client divergence: both
    // clients start from the RESP2 shape and override it from the session's
    // negotiated version (see flatPairsShapeFor). #385.
    assert.strictEqual(NODE_REDIS_DECODE_OPTIONS.flatPairsShape, 'flat')
    assert.strictEqual(IN_MEMORY_DECODE_OPTIONS.flatPairsShape, 'flat')

    assert.strictEqual(flatPairsShapeFor(2), 'flat')
    assert.strictEqual(flatPairsShapeFor(3), 'tuples')

    const withScores: RedisValue = {
      kind: 'flat-pairs',
      entries: [
        [bulk('a'), { kind: 'double', value: 1 }],
        [bulk('b'), { kind: 'double', value: 2 }],
      ],
    }

    // Real node-redis, sendCommand against Redis 8.0.6:
    //   RESP2 → ["a","1","b","2"]   RESP3 → [["a",1],["b",2]]
    assert.deepStrictEqual(
      decodeRedisValue(withScores, {
        ...NODE_REDIS_DECODE_OPTIONS,
        flatPairsShape: flatPairsShapeFor(2),
      }),
      ['a', 1, 'b', 2],
    )
    assert.deepStrictEqual(
      decodeRedisValue(withScores, {
        ...NODE_REDIS_DECODE_OPTIONS,
        flatPairsShape: flatPairsShapeFor(3),
      }),
      [
        ['a', 1],
        ['b', 2],
      ],
    )
  })

  test('map keys stay utf8 strings even with returnBuffers', () => {
    const map: RedisValue = {
      kind: 'map',
      entries: [[bulk('field'), bulk('value')]],
    }
    assert.deepStrictEqual(
      decodeRedisValue(map, {
        ...IN_MEMORY_DECODE_OPTIONS,
        returnBuffers: true,
      }),
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
