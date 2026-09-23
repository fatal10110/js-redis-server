import assert from 'node:assert'
import { after, before, describe, test } from 'node:test'
import { Redis } from 'ioredis'
import { TestRunner } from '../test-config'
import { randomKey } from '../utils'

/**
 * How a Lua script's typed replies reach a RESP2 client (#449). ioredis@5
 * speaks RESP2 only; the node-redis twin covers RESP3 as well.
 *
 * Ground truth, real Redis 8.0: `{double=…}`, `{big_number=…}`, `{map=…}` and
 * `{set=…}` tables convert whether or not the script called
 * `redis.setresp(3)`, and after `redis.setresp(3)` a `redis.call` reply
 * reaches the script as its RESP3 typed table (`{map=…}`, `{double=…}`).
 *
 * Lua semantics do not depend on cluster mode, so this runs on a standalone
 * server (`REDIS_STANDALONE_PORT` on the real backend).
 */
const testRunner = new TestRunner()
const RUN = randomKey()

// Known mock gaps in the bundled `lua-redis-wasm` engine, pinned against real
// Redis until they close.
// mock and socketless run the same in-process server, so they share its gaps.
const mockGap = (reason: string) =>
  testRunner.backend !== 'real' ? reason : false
const ENGINE_GAP = mockGap(
  'lua-redis-wasm drops typed tables without redis.setresp(3) (#449)',
)
const ENGINE_NULL_GAP = mockGap(
  'lua-redis-wasm decodes a RESP3 null as false, not nil (#449)',
)

describe(`Lua typed replies at RESP2 (ioredis, ${testRunner.getBackendName()})`, () => {
  const hashKey = `lua449:${RUN}:h`
  const zsetKey = `lua449:${RUN}:z`
  const missingKey = `lua449:${RUN}:missing`
  const streamKey = `lua449:${RUN}:st`
  let redis: Redis

  before(async () => {
    redis = await testRunner.setupIoredisStandalone()
    await redis.hset(hashKey, 'f', 'v')
    await redis.zadd(zsetKey, 2.5, 'b')
    await redis.xadd(streamKey, '1-1', 'a', '1')
  })

  after(async () => {
    await redis.del(hashKey, zsetKey, streamKey)
    await testRunner.cleanup()
  })

  function evalScript(script: string): Promise<unknown> {
    return redis.eval(script, 4, hashKey, zsetKey, missingKey, streamKey)
  }

  describe('typed tables convert without redis.setresp(3)', () => {
    test('{double=…} is a bulk string', { todo: ENGINE_GAP }, async () => {
      assert.strictEqual(await evalScript('return {double=2.5}'), '2.5')
    })

    test('{big_number=…} is a bulk string', { todo: ENGINE_GAP }, async () => {
      assert.strictEqual(
        await evalScript("return {big_number='12345678901234567890'}"),
        '12345678901234567890',
      )
    })

    test('{map=…} is a flat array', { todo: ENGINE_GAP }, async () => {
      assert.deepStrictEqual(await evalScript("return {map={a='1'}}"), [
        'a',
        '1',
      ])
    })

    test('{set=…} is an array', { todo: ENGINE_GAP }, async () => {
      assert.deepStrictEqual(await evalScript('return {set={a=true}}'), ['a'])
    })

    test(
      '{verbatim_string=…} is a bulk string',
      { todo: ENGINE_GAP },
      async () => {
        assert.strictEqual(
          await evalScript(
            "return {verbatim_string={format='txt', string='hi'}}",
          ),
          'hi',
        )
      },
    )
  })

  describe('redis.call replies after redis.setresp(3)', () => {
    test('HGETALL is a map the script indexes by field', async () => {
      assert.deepStrictEqual(
        await evalScript(
          "redis.setresp(3); return redis.call('HGETALL', KEYS[1])",
        ),
        ['f', 'v'],
      )
      assert.deepStrictEqual(
        await evalScript(
          "redis.setresp(3); local r = redis.call('HGETALL', KEYS[1]); return {type(r.map), r.map.f}",
        ),
        ['table', 'v'],
      )
    })

    test('XREAD is a map the script indexes by stream name', async () => {
      assert.deepStrictEqual(
        await evalScript(
          "redis.setresp(3); return redis.call('XREAD', 'STREAMS', KEYS[4], '0')",
        ),
        [streamKey, [['1-1', ['a', '1']]]],
      )
      assert.strictEqual(
        await evalScript(
          "redis.setresp(3); local r = redis.call('XREAD', 'STREAMS', KEYS[4], '0'); return r.map[KEYS[4]][1][1]",
        ),
        '1-1',
      )
    })

    test('ZSCORE is a {double=…} table', async () => {
      assert.strictEqual(
        await evalScript(
          "redis.setresp(3); return redis.call('ZSCORE', KEYS[2], 'b')",
        ),
        '2.5',
      )
      assert.deepStrictEqual(
        await evalScript(
          "redis.setresp(3); local r = redis.call('ZSCORE', KEYS[2], 'b'); return {type(r), tostring(r.double)}",
        ),
        ['table', '2.5'],
      )
    })

    test('ZRANGE WITHSCORES is member/score pairs', async () => {
      assert.deepStrictEqual(
        await evalScript(
          "redis.setresp(3); return redis.call('ZRANGE', KEYS[2], 0, -1, 'WITHSCORES')",
        ),
        [['b', '2.5']],
      )
    })

    test('a missing value is nil', { todo: ENGINE_NULL_GAP }, async () => {
      assert.strictEqual(
        await evalScript("redis.setresp(3); return redis.call('GET', KEYS[3])"),
        null,
      )
      assert.strictEqual(
        await evalScript(
          "redis.setresp(3); return type(redis.call('GET', KEYS[3]))",
        ),
        'nil',
      )
    })

    test(
      'a missing value ends an array reply',
      { todo: ENGINE_NULL_GAP },
      async () => {
        // A Lua nil ends the array Redis builds from a table.
        assert.deepStrictEqual(
          await evalScript(
            "redis.setresp(3); return redis.call('HMGET', KEYS[1], 'f', 'nope', 'f')",
          ),
          ['v'],
        )
      },
    )

    test('redis.setresp(2) switches back to RESP2 shapes', async () => {
      assert.deepStrictEqual(
        await evalScript(
          "redis.setresp(3); redis.setresp(2); local r = redis.call('HGETALL', KEYS[1]); return {type(r.map), r[1], r[2]}",
        ),
        ['nil', 'f', 'v'],
      )
    })
  })

  test('without redis.setresp(3), HGETALL is flat and ZSCORE a string', async () => {
    assert.deepStrictEqual(
      await evalScript("return redis.call('HGETALL', KEYS[1])"),
      ['f', 'v'],
    )
    assert.strictEqual(
      await evalScript("return redis.call('ZSCORE', KEYS[2], 'b')"),
      '2.5',
    )
  })
})
