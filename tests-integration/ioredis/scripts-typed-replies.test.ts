import assert from 'node:assert'
import { after, before, describe, test } from 'node:test'
import { Cluster } from 'ioredis'
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
 */
const testRunner = new TestRunner()
const RUN = randomKey()

// Known mock gaps in the bundled `lua-redis-wasm` engine, pinned against real
// Redis until they close.
const mockGap = (reason: string) =>
  testRunner.backend === 'mock' ? reason : false
const ENGINE_GAP = mockGap(
  'lua-redis-wasm drops typed tables without redis.setresp(3) (#449)',
)
const ENGINE_NULL_GAP = mockGap(
  'lua-redis-wasm decodes a RESP3 null as false, not nil (#449)',
)

describe(`Lua typed replies at RESP2 (ioredis, ${testRunner.getBackendName()})`, () => {
  const tag = `{lua449:${RUN}}`
  const hashKey = `${tag}:h`
  const zsetKey = `${tag}:z`
  const missingKey = `${tag}:missing`
  let redis: Cluster

  before(async () => {
    redis = await testRunner.setupIoredisCluster()
    await redis.hset(hashKey, 'f', 'v')
    await redis.zadd(zsetKey, 2.5, 'b')
  })

  after(async () => {
    await redis.del(hashKey, zsetKey)
    await testRunner.cleanup()
  })

  function evalScript(script: string): Promise<unknown> {
    return redis.eval(script, 3, hashKey, zsetKey, missingKey)
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
