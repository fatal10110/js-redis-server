import { test, describe } from 'node:test'
import assert from 'node:assert'
import { ErrorReply, SimpleError } from 'redis'
import { NODE_REDIS_DECODE_OPTIONS } from '../../src/client-mocks/node-redis-mock'
import { IN_MEMORY_DECODE_OPTIONS } from '../../src/in-memory-client'
import {
  decodeRedisValue,
  decodeRedisKey,
  decodeRedisMapEntries,
  redisErrorText,
  toRedisArgument,
  type DecodeRedisValueOptions,
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

// `version` belongs to the connection, not the client, so it is not part of
// either constant. Every case below but the `version` one is
// protocol-independent; pin RESP2 so the options are complete.
const nodeRedisAtResp2: DecodeRedisValueOptions = {
  ...NODE_REDIS_DECODE_OPTIONS,
  version: 2,
}
const inMemoryAtResp2: DecodeRedisValueOptions = {
  ...IN_MEMORY_DECODE_OPTIONS,
  version: 2,
}

describe('decode option divergences between the two clients', () => {
  test('pushShape: the facade drops the type tag, in-memory keeps it', () => {
    const push: RedisValue = {
      kind: 'push',
      name: 'message',
      items: [bulk('news'), bulk('hello')],
    }

    assert.strictEqual(NODE_REDIS_DECODE_OPTIONS.pushShape, 'items')
    assert.deepStrictEqual(decodeRedisValue(push, nodeRedisAtResp2), [
      'news',
      'hello',
    ])

    assert.strictEqual(IN_MEMORY_DECODE_OPTIONS.pushShape, 'tagged')
    assert.deepStrictEqual(decodeRedisValue(push, inMemoryAtResp2), [
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
    const narrowed = decodeRedisValue(unsafe, nodeRedisAtResp2)
    assert.strictEqual(typeof narrowed, 'number')
    assert.strictEqual(narrowed, 9007199254740992)

    // The in-memory client keeps the exact value instead.
    assert.strictEqual(IN_MEMORY_DECODE_OPTIONS.narrowBigInt, 'when-safe')
    assert.strictEqual(
      decodeRedisValue(unsafe, inMemoryAtResp2),
      BigInt(Number.MAX_SAFE_INTEGER) + 2n,
    )
  })

  test('narrowBigInt: a safe bigint is a number for both clients', () => {
    const safe: RedisValue = { kind: 'integer', value: 42n }
    assert.strictEqual(decodeRedisValue(safe, nodeRedisAtResp2), 42)
    assert.strictEqual(decodeRedisValue(safe, inMemoryAtResp2), 42)
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
      () => decodeRedisValue(error, nodeRedisAtResp2),
      (err: unknown) => {
        // `instanceof ErrorReply` is node-redis' documented idiom.
        assert.ok(err instanceof ErrorReply)
        // ...and, as in real node-redis v6, concretely a SimpleError.
        assert.ok(err instanceof SimpleError)
        assert.ok(!(err instanceof RedisCommandError))
        assert.strictEqual(err.message, text)
        return true
      },
    )

    assert.throws(
      () => decodeRedisValue(error, inMemoryAtResp2),
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

    const options = { ...inMemoryAtResp2, returnBuffers: true }
    assert.deepStrictEqual(
      decodeRedisValue(bulk('v'), options),
      Buffer.from('v'),
    )
    assert.strictEqual(decodeRedisValue(bulk('v'), inMemoryAtResp2), 'v')
    // A null bulk-string is still null, not an empty Buffer.
    assert.strictEqual(
      decodeRedisValue({ kind: 'bulk-string', value: null }, options),
      null,
    )
  })

  // `version` is not a per-client divergence — it belongs to the connection,
  // which is why it is not part of either constant. Both clients pass their
  // session's negotiated version, so both must read the same reply the same way
  // at the same protocol. Every shape it decides is asserted for both. #385,
  // #414.
  const bothClients = [NODE_REDIS_DECODE_OPTIONS, IN_MEMORY_DECODE_OPTIONS]

  test('version: flat-pairs is flat on RESP2 and tuples on RESP3, for both clients', () => {
    const withScores: RedisValue = {
      kind: 'flat-pairs',
      entries: [
        [bulk('a'), { kind: 'double', value: 1 }],
        [bulk('b'), { kind: 'double', value: 2 }],
      ],
    }

    // Real node-redis, sendCommand against a real server:
    //   RESP2 → ["a","1","b","2"]   RESP3 → [["a",1],["b",2]]
    for (const client of bothClients) {
      assert.deepStrictEqual(
        decodeRedisValue(withScores, { ...client, version: 2 }),
        ['a', '1', 'b', '2'],
      )
      assert.deepStrictEqual(
        decodeRedisValue(withScores, { ...client, version: 3 }),
        [
          ['a', 1],
          ['b', 2],
        ],
      )
    }
  })

  test('version: a map is flat on RESP2 and an object on RESP3, for both clients', () => {
    const map: RedisValue = {
      kind: 'map',
      entries: [
        [bulk('f1'), bulk('v1')],
        [bulk('f2'), bulk('v2')],
      ],
    }

    // Real node-redis, `sendCommand(['HGETALL', 'h'])`:
    //   RESP2 → ["f1","v1","f2","v2"]   RESP3 → {f1:"v1",f2:"v2"}
    for (const client of bothClients) {
      assert.deepStrictEqual(decodeRedisValue(map, { ...client, version: 2 }), [
        'f1',
        'v1',
        'f2',
        'v2',
      ])
      assert.deepStrictEqual(decodeRedisValue(map, { ...client, version: 3 }), {
        f1: 'v1',
        f2: 'v2',
      })
    }
  })

  test('version: map-pairs is an array of pairs on RESP2, an object on RESP3', () => {
    // `map-pairs` differs from `map` only at RESP2, where the encoder writes it
    // as `[[k, v], …]` rather than flattening it — XREAD's shape.
    const streams: RedisValue = {
      kind: 'map-pairs',
      entries: [[bulk('s'), { kind: 'array', items: [bulk('e')] }]],
    }

    for (const client of bothClients) {
      assert.deepStrictEqual(
        decodeRedisValue(streams, { ...client, version: 2 }),
        [['s', ['e']]],
      )
      assert.deepStrictEqual(
        decodeRedisValue(streams, { ...client, version: 3 }),
        { s: ['e'] },
      )
    }
  })

  test('version: a double is the wire string on RESP2 and a number on RESP3', () => {
    // Real node-redis, `sendCommand(['ZSCORE', 'z', 'b'])`:
    //   RESP2 → "2.5"   RESP3 → 2.5
    // The RESP2 text comes from the same formatter the encoder uses, so the
    // Redis spellings of the specials survive the round trip.
    for (const client of bothClients) {
      const atResp2 = (value: number) =>
        decodeRedisValue({ kind: 'double', value }, { ...client, version: 2 })

      assert.strictEqual(atResp2(2.5), '2.5')
      assert.strictEqual(atResp2(Infinity), 'inf')
      assert.strictEqual(atResp2(-Infinity), '-inf')
      assert.strictEqual(atResp2(Number.NaN), 'nan')
      assert.strictEqual(
        decodeRedisValue(
          { kind: 'double', value: 2.5 },
          { ...client, version: 3 },
        ),
        2.5,
      )
    }
  })

  test('version: big-number is the digit string on RESP2, a bigint on RESP3', () => {
    // Real node-redis, `EVAL 'return {big_number="12345678901234567890"}' 0`:
    //   RESP2 → "12345678901234567890"   RESP3 → 12345678901234567890n
    // Reachable here through Lua's `redis.setresp(3)`, the same as `double`.
    const huge = 12345678901234567890n
    for (const client of bothClients) {
      assert.strictEqual(
        decodeRedisValue(
          { kind: 'big-number', value: huge },
          { ...client, version: 2 },
        ),
        '12345678901234567890',
      )
      assert.strictEqual(
        decodeRedisValue(
          { kind: 'big-number', value: huge },
          { ...client, version: 3 },
        ),
        huge,
      )
    }
  })

  test('version: a boolean is the 1/0 integer on RESP2, a boolean on RESP3', () => {
    // RESP2 has no boolean; `encodeRedisValue` writes `:1` / `:0`, so that is
    // the number a client reads back. Only RESP3 has `#t` / `#f`.
    for (const client of bothClients) {
      for (const [value, resp2] of [
        [true, 1],
        [false, 0],
      ] as const) {
        assert.strictEqual(
          decodeRedisValue(
            { kind: 'boolean', value },
            { ...client, version: 2 },
          ),
          resp2,
        )
        assert.strictEqual(
          decodeRedisValue(
            { kind: 'boolean', value },
            { ...client, version: 3 },
          ),
          value,
        )
      }
    }
  })

  test('map keys stay utf8 strings even with returnBuffers', () => {
    const map: RedisValue = {
      kind: 'map',
      entries: [[bulk('field'), bulk('value')]],
    }
    // At RESP3, where the map *is* an object. The RESP2 flat array is a plain
    // array of decoded items, so its keys follow returnBuffers like any other.
    assert.deepStrictEqual(
      decodeRedisValue(map, {
        ...inMemoryAtResp2,
        version: 3,
        returnBuffers: true,
      }),
      { field: Buffer.from('value') },
    )
  })

  test('decodeRedisMapEntries keeps a curated method on the object shape', () => {
    // node-redis' `hGetAll` / `configGet` transformReply builds the object
    // itself, so those curated methods are an object at RESP2 too — the one
    // place the protocol switch must not reach. #414.
    const entries: [RedisValue, RedisValue][] = [[bulk('f1'), bulk('v1')]]

    for (const client of bothClients) {
      for (const version of [2, 3] as const) {
        assert.deepStrictEqual(
          decodeRedisMapEntries(entries, { ...client, version }),
          { f1: 'v1' },
        )
      }
    }
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

  test('a double map key follows real node-redis at each protocol', () => {
    // The key and the value spellings deliberately differ at RESP3. Real
    // node-redis against Redis 8.0.6:
    //   EVAL "redis.setresp(3); return {map={[{double=1/0}]=1}}" 0
    //     RESP2 → ["inf",1]         RESP3 → {"Infinity":1}
    //   EVAL "redis.setresp(3); return {map={[{double=2.5}]=1}}" 0
    //     RESP2 → ["2.5",1]         RESP3 → {"2.5":1}
    //   …with {double=-1/0} and {double=0/0}:
    //     RESP2 → ["-inf",1] / ["nan",1]   RESP3 → {"-Infinity":1} / {"NaN":1}
    // At RESP2 the key is an array item spelled by Redis; at RESP3 node-redis
    // parses `,inf` to a number and keys the object with `String()`.
    const mapWith = (key: number): RedisValue => ({
      kind: 'map',
      entries: [
        [
          { kind: 'double', value: key },
          { kind: 'integer', value: 1 },
        ],
      ],
    })

    for (const client of [
      NODE_REDIS_DECODE_OPTIONS,
      IN_MEMORY_DECODE_OPTIONS,
    ]) {
      const at = (key: number, version: 2 | 3) =>
        decodeRedisValue(mapWith(key), { ...client, version })

      assert.deepStrictEqual(at(Infinity, 2), ['inf', 1])
      assert.deepStrictEqual(at(Infinity, 3), { Infinity: 1 })
      assert.deepStrictEqual(at(-Infinity, 3), { '-Infinity': 1 })
      assert.deepStrictEqual(at(Number.NaN, 3), { NaN: 1 })
      assert.deepStrictEqual(at(2.5, 2), ['2.5', 1])
      assert.deepStrictEqual(at(2.5, 3), { '2.5': 1 })
    }

    // decodeRedisKey on its own is the RESP3 half: JavaScript's spelling.
    assert.strictEqual(
      decodeRedisKey({ kind: 'double', value: Infinity }),
      'Infinity',
    )
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
