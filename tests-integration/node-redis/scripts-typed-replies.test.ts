import assert from 'node:assert'
import { after, before, describe, test } from 'node:test'
import { createClient, RedisClientType, RedisClusterType } from 'redis'
import { TestRunner } from '../test-config'
import { findNodeRedisSlotOwner, randomKey } from '../utils'

/**
 * How a Lua script's typed replies reach a client, at each protocol (#449).
 *
 * Ground truth, `node-redis@6` against real Redis 8.0:
 *
 *  1. `{double=…}`, `{big_number=…}`, `{map=…}`, `{set=…}` and
 *     `{verbatim_string=…}` tables are converted whether or not the script
 *     called `redis.setresp(3)` — only a Lua *boolean* depends on it.
 *  2. After `redis.setresp(3)`, `redis.call` hands the script RESP3 replies:
 *     a map is a `{map=…}` table, a double a `{double=…}` table, a set a
 *     `{set=…}` table and a missing value `nil`. Returned as-is, they reach
 *     the client as the RESP3 type (downgraded by the client's own protocol).
 */
const testRunner = new TestRunner()
const RUN = randomKey()

// Known mock gaps, pinned against real Redis until they close. The first two
// live in the bundled `lua-redis-wasm` engine, not in this repo.
const mockGap = (reason: string) =>
  testRunner.backend === 'mock' ? reason : false
const ENGINE_GAP = mockGap(
  'lua-redis-wasm drops typed tables without redis.setresp(3) (#449)',
)
const ENGINE_NULL_GAP = mockGap(
  'lua-redis-wasm decodes a RESP3 null as false, not nil (#449)',
)
const SET_REPLY_GAP = mockGap('mock SMEMBERS replies an array, not a set')

describe(`Lua typed replies per protocol (node-redis, ${testRunner.getBackendName()})`, () => {
  const tag = `{lua449:${RUN}}`
  const hashKey = `${tag}:h`
  const zsetKey = `${tag}:z`
  const setKey = `${tag}:s`
  const missingKey = `${tag}:missing`
  let cluster: RedisClusterType
  const clients: Record<2 | 3, RedisClientType> = {} as never

  before(async () => {
    cluster = (await testRunner.setupNodeRedisCluster()) as RedisClusterType
    const { host, port } = findNodeRedisSlotOwner(cluster, tag)
    for (const RESP of [2, 3] as const) {
      const client = createClient({
        url: `redis://${host}:${port}`,
        RESP,
      }) as unknown as RedisClientType
      client.on('error', () => {})
      await client.connect()
      clients[RESP] = client
    }
    await cluster.hSet(hashKey, 'f', 'v')
    await cluster.zAdd(zsetKey, { score: 2.5, value: 'b' })
    await cluster.sAdd(setKey, 'x')
  })

  after(async () => {
    await cluster.del([hashKey, zsetKey, setKey])
    clients[2]?.destroy()
    clients[3]?.destroy()
    await testRunner.cleanup()
  })

  function evalAt(resp: 2 | 3, script: string): Promise<unknown> {
    return clients[resp].eval(script, {
      keys: [hashKey, zsetKey, setKey, missingKey],
    })
  }

  describe('typed tables convert without redis.setresp(3)', () => {
    test('{double=…}', { todo: ENGINE_GAP }, async () => {
      assert.strictEqual(await evalAt(2, 'return {double=2.5}'), '2.5')
      assert.strictEqual(await evalAt(3, 'return {double=2.5}'), 2.5)
    })

    test('{big_number=…}', { todo: ENGINE_GAP }, async () => {
      const script = "return {big_number='12345678901234567890'}"
      assert.strictEqual(await evalAt(2, script), '12345678901234567890')
      assert.strictEqual(await evalAt(3, script), 12345678901234567890n)
    })

    test('{map=…}', { todo: ENGINE_GAP }, async () => {
      // One entry keeps Lua's table iteration order deterministic.
      const script = "return {map={a='1'}}"
      assert.deepStrictEqual(await evalAt(2, script), ['a', '1'])
      assert.deepStrictEqual(await evalAt(3, script), { a: '1' })
    })

    test('{set=…}', { todo: ENGINE_GAP }, async () => {
      const script = 'return {set={a=true}}'
      assert.deepStrictEqual(await evalAt(2, script), ['a'])
      assert.deepStrictEqual(await evalAt(3, script), ['a'])
    })

    test('a boolean still needs setresp(3) to be a boolean', async () => {
      assert.strictEqual(await evalAt(2, 'return true'), 1)
      assert.strictEqual(await evalAt(2, 'return false'), null)
      assert.strictEqual(await evalAt(3, 'return true'), 1)
      assert.strictEqual(await evalAt(3, 'return false'), null)
    })
  })

  describe('redis.call replies after redis.setresp(3)', () => {
    test('HGETALL is a map', async () => {
      const script = "redis.setresp(3); return redis.call('HGETALL', KEYS[1])"
      assert.deepStrictEqual(await evalAt(2, script), ['f', 'v'])
      assert.deepStrictEqual(await evalAt(3, script), { f: 'v' })
    })

    test('the script sees a map as a {map=…} table', async () => {
      const script =
        "redis.setresp(3); local r = redis.call('HGETALL', KEYS[1]); return {type(r.map), r.map.f}"
      assert.deepStrictEqual(await evalAt(2, script), ['table', 'v'])
    })

    test('ZSCORE and ZINCRBY are doubles', async () => {
      const zscore =
        "redis.setresp(3); return redis.call('ZSCORE', KEYS[2], 'b')"
      assert.strictEqual(await evalAt(2, zscore), '2.5')
      assert.strictEqual(await evalAt(3, zscore), 2.5)
      const zincrby =
        "redis.setresp(3); return redis.call('ZINCRBY', KEYS[2], 0, 'b')"
      assert.strictEqual(await evalAt(2, zincrby), '2.5')
      assert.strictEqual(await evalAt(3, zincrby), 2.5)
    })

    test('the script sees a double as a {double=…} table', async () => {
      const script =
        "redis.setresp(3); local r = redis.call('ZSCORE', KEYS[2], 'b'); return {type(r), tostring(r.double)}"
      assert.deepStrictEqual(await evalAt(2, script), ['table', '2.5'])
    })

    test('ZRANGE WITHSCORES is member/double pairs', async () => {
      const script =
        "redis.setresp(3); return redis.call('ZRANGE', KEYS[2], 0, -1, 'WITHSCORES')"
      assert.deepStrictEqual(await evalAt(2, script), [['b', '2.5']])
      assert.deepStrictEqual(await evalAt(3, script), [['b', 2.5]])
    })

    test('SMEMBERS is a set', async () => {
      const script = "redis.setresp(3); return redis.call('SMEMBERS', KEYS[3])"
      assert.deepStrictEqual(await evalAt(2, script), ['x'])
      assert.deepStrictEqual(await evalAt(3, script), ['x'])
    })

    test(
      'the script sees a set as a {set=…} table',
      { todo: SET_REPLY_GAP },
      async () => {
        const script =
          "redis.setresp(3); local r = redis.call('SMEMBERS', KEYS[3]); return tostring(r.set.x)"
        assert.strictEqual(await evalAt(2, script), 'true')
      },
    )

    test('integer replies stay integers', async () => {
      const script =
        "redis.setresp(3); return redis.call('SMISMEMBER', KEYS[3], 'x', 'y')"
      assert.deepStrictEqual(await evalAt(2, script), [1, 0])
      assert.deepStrictEqual(await evalAt(3, script), [1, 0])
    })

    test('a missing value is nil', { todo: ENGINE_NULL_GAP }, async () => {
      const script = "redis.setresp(3); return redis.call('GET', KEYS[4])"
      assert.strictEqual(await evalAt(2, script), null)
      assert.strictEqual(await evalAt(3, script), null)
      const type = "redis.setresp(3); return type(redis.call('GET', KEYS[4]))"
      assert.strictEqual(await evalAt(2, type), 'nil')
    })
  })

  describe('redis.call replies without redis.setresp(3) stay RESP2', () => {
    test('HGETALL is a flat array and ZSCORE a string', async () => {
      const hgetall = "return redis.call('HGETALL', KEYS[1])"
      assert.deepStrictEqual(await evalAt(2, hgetall), ['f', 'v'])
      assert.deepStrictEqual(await evalAt(3, hgetall), ['f', 'v'])
      const zscore = "return redis.call('ZSCORE', KEYS[2], 'b')"
      assert.strictEqual(await evalAt(2, zscore), '2.5')
      assert.strictEqual(await evalAt(3, zscore), '2.5')
    })

    test('a missing value is false', async () => {
      const script = "return type(redis.call('GET', KEYS[4]))"
      assert.strictEqual(await evalAt(3, script), 'boolean')
    })
  })
})
