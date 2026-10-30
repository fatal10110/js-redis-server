import { test, describe } from 'node:test'
import assert from 'node:assert'
import { SimpleError } from 'redis'
import { NODE_REDIS_DECODE_OPTIONS } from '../../src/client-mocks/node-redis-mock'
import { IN_MEMORY_DECODE_OPTIONS } from '../../src/in-memory-client'
import {
  decodeRedisValue,
  decodeRedisMapEntries,
  type DecodeRedisValueOptions,
} from '../../src/core/decode-redis-value'
import type { RedisValue } from '../../src/core/redis-value'

const err: RedisValue = { kind: 'error', code: 'ERR', message: 'x' }
const one: RedisValue = { kind: 'integer', value: 1 }
const bulk = (value: string): RedisValue => ({
  kind: 'bulk-string',
  value: Buffer.from(value),
})

const throwing: DecodeRedisValueOptions = {
  narrowBigInt: 'always',
  pushShape: 'items',
  error: text => new Error(text),
  version: 3,
}
const returning = { ...throwing, nestedErrors: 'return' as const }

describe('decodeRedisValue nestedErrors', () => {
  test("default 'throw': a nested error fails the whole reply", () => {
    assert.throws(
      () => decodeRedisValue({ kind: 'array', items: [one, err] }, throwing),
      /^Error: ERR x$/,
    )
  })

  test("'return': decoded in place, at any depth and in every container", () => {
    const nested = decodeRedisValue(
      {
        kind: 'array',
        items: [one, err, { kind: 'set', items: [err] }],
      },
      returning,
    )
    assert.ok(Array.isArray(nested))
    assert.strictEqual(nested[0], 1)
    assert.ok(nested[1] instanceof Error)
    assert.strictEqual(nested[1].message, 'ERR x')
    const inner = nested[2]
    assert.ok(Array.isArray(inner) && inner[0] instanceof Error)

    const map: RedisValue = { kind: 'map', entries: [[bulk('a'), err]] }
    const asObject = decodeRedisValue(map, returning) as { a: unknown }
    assert.ok(asObject.a instanceof Error)
    const flat = decodeRedisValue(map, { ...returning, version: 2 })
    assert.ok(Array.isArray(flat) && flat[1] instanceof Error)
    const entries = decodeRedisMapEntries(map.entries, returning)
    assert.ok(entries.a instanceof Error)
  })

  test("'return' still throws a top-level error", () => {
    assert.throws(() => decodeRedisValue(err, returning), /^Error: ERR x$/)
  })

  test('the node-redis facade returns nested errors, the in-memory client throws', () => {
    const reply: RedisValue = { kind: 'array', items: [one, err] }
    const nodeRedis = decodeRedisValue(reply, {
      ...NODE_REDIS_DECODE_OPTIONS,
      version: 2,
    })
    assert.ok(Array.isArray(nodeRedis) && nodeRedis[1] instanceof SimpleError)
    assert.throws(() =>
      decodeRedisValue(reply, { ...IN_MEMORY_DECODE_OPTIONS, version: 2 }),
    )
  })
})
