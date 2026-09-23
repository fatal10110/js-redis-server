import { describe, test } from 'node:test'
import assert from 'node:assert'
import { redisValueToLuaReply } from '../src/core/lua-runtime'
import { RedisValue } from '../src/core/redis-value'

/**
 * The shape a `redis.call` reply takes in Lua, per the protocol the script
 * selected with `redis.setresp()` (#449). The integration suites cover the
 * kinds commands emit today (double, map, map-pairs, flat-pairs); this covers
 * the RESP3 branches no command reaches yet — set, boolean, big number and
 * verbatim string.
 */
describe('redisValueToLuaReply', () => {
  const b = (s: string) => Buffer.from(s)

  test('RESP3 set, boolean, big number and verbatim become typed values', () => {
    assert.deepStrictEqual(
      redisValueToLuaReply(RedisValue.set([RedisValue.bulkString(b('x'))]), 3),
      { set: [b('x')] },
    )
    assert.strictEqual(redisValueToLuaReply(RedisValue.boolean(true), 3), true)
    assert.strictEqual(
      redisValueToLuaReply(RedisValue.boolean(false), 3),
      false,
    )
    assert.deepStrictEqual(
      redisValueToLuaReply(RedisValue.bigNumber(12345678901234567890n), 3),
      { big_number: b('12345678901234567890') },
    )
    assert.deepStrictEqual(
      redisValueToLuaReply(RedisValue.verbatim('txt', b('hi')), 3),
      { verbatim_string: { format: b('txt'), string: b('hi') } },
    )
  })

  test('RESP2 flattens the same kinds', () => {
    assert.deepStrictEqual(
      redisValueToLuaReply(RedisValue.set([RedisValue.bulkString(b('x'))]), 2),
      [b('x')],
    )
    assert.strictEqual(redisValueToLuaReply(RedisValue.boolean(true), 2), 1)
    assert.strictEqual(redisValueToLuaReply(RedisValue.boolean(false), 2), 0)
    assert.deepStrictEqual(
      redisValueToLuaReply(RedisValue.bigNumber(12345678901234567890n), 2),
      b('12345678901234567890'),
    )
    assert.deepStrictEqual(
      redisValueToLuaReply(RedisValue.verbatim('txt', b('hi')), 2),
      b('hi'),
    )
  })

  test('RESP3 converts nested values recursively', () => {
    const value = RedisValue.array([
      RedisValue.double(2.5),
      RedisValue.map([[RedisValue.bulkString(b('f')), RedisValue.double(1)]]),
    ])
    assert.deepStrictEqual(redisValueToLuaReply(value, 3), [
      { double: 2.5 },
      { map: [[b('f'), { double: 1 }]] },
    ])
  })
})
