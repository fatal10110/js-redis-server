import assert from 'node:assert'
import { after, before, describe, test } from 'node:test'
import { createClient, RESP_TYPES } from 'redis'
import { TestRunner } from '../test-config'
import { randomKey } from '../utils'

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
 *     The protocol is per script: `redis.setresp(2)` switches back, and every
 *     EVAL starts at RESP2 again.
 *
 * Lua semantics do not depend on cluster mode, so this runs on a standalone
 * server (`REDIS_STANDALONE_PORT` on the real backend).
 */
const testRunner = new TestRunner()
const RUN = randomKey()

// Known mock gaps, pinned against real Redis until they close. The first two
// live in the bundled `lua-redis-wasm` engine; the third in this repo.
// mock and socketless run the same in-process server, so they share its gaps.
const mockGap = (reason: string) =>
  testRunner.backend !== 'real' ? reason : false
const ENGINE_GAP = mockGap(
  'lua-redis-wasm drops typed tables without redis.setresp(3) (#449)',
)
const ENGINE_NULL_GAP = mockGap(
  'lua-redis-wasm decodes a RESP3 null as false, not nil (#449)',
)
const SET_REPLY_GAP = mockGap('mock SMEMBERS replies an array, not a set')

function connectAt(port: number, RESP: 2 | 3) {
  const client = createClient({ url: `redis://127.0.0.1:${port}`, RESP })
  client.on('error', () => {})
  return client.connect()
}

type Client = Awaited<ReturnType<typeof connectAt>>

describe(`Lua typed replies per protocol (node-redis, ${testRunner.getBackendName()})`, () => {
  const hashKey = `lua449:${RUN}:h`
  const zsetKey = `lua449:${RUN}:z`
  const setKey = `lua449:${RUN}:s`
  const missingKey = `lua449:${RUN}:missing`
  const streamKey = `lua449:${RUN}:st`
  let clients: Record<2 | 3, Client> | undefined

  before(async () => {
    const port = await testRunner.setupRawStandalone()
    const [resp2, resp3] = await Promise.all([
      connectAt(port, 2),
      connectAt(port, 3),
    ])
    clients = { 2: resp2, 3: resp3 }
    await resp2.hSet(hashKey, 'f', 'v')
    await resp2.zAdd(zsetKey, { score: 2.5, value: 'b' })
    await resp2.sAdd(setKey, 'x')
    await resp2.xAdd(streamKey, '1-1', { a: '1' })
  })

  after(async () => {
    await clients?.[2].del([hashKey, zsetKey, setKey, streamKey])
    clients?.[2].destroy()
    clients?.[3].destroy()
    await testRunner.cleanup()
  })

  function client(resp: 2 | 3): Client {
    assert.ok(clients, 'clients are connected in before()')
    return clients[resp]
  }

  function evalAt(resp: 2 | 3, script: string): Promise<unknown> {
    return client(resp).eval(script, {
      keys: [hashKey, zsetKey, setKey, missingKey, streamKey],
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

    test('{verbatim_string=…}', { todo: ENGINE_GAP }, async () => {
      const script = "return {verbatim_string={format='txt', string='hi'}}"
      assert.strictEqual(await evalAt(2, script), 'hi')
      assert.strictEqual(await evalAt(3, script), 'hi')
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

    test('XREAD is a map of stream name to entries', async () => {
      const script =
        "redis.setresp(3); return redis.call('XREAD', 'STREAMS', KEYS[5], '0')"
      const entries = [['1-1', ['a', '1']]]
      assert.deepStrictEqual(await evalAt(2, script), [streamKey, entries])
      assert.deepStrictEqual(await evalAt(3, script), {
        [streamKey]: entries,
      })
      const seen =
        "redis.setresp(3); local r = redis.call('XREAD', 'STREAMS', KEYS[5], '0'); return r.map[KEYS[5]][1][1]"
      assert.strictEqual(await evalAt(2, seen), '1-1')
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

    test('SMEMBERS is a set', { todo: SET_REPLY_GAP }, async () => {
      // node-redis reads a RESP3 set as a plain array unless told otherwise;
      // mapping sets to `Set` is what tells `~` apart from `*`.
      const script = "redis.setresp(3); return redis.call('SMEMBERS', KEYS[3])"
      const reply = await client(3)
        .withTypeMapping({ [RESP_TYPES.SET]: Set })
        .eval(script, { keys: [hashKey, zsetKey, setKey] })
      assert.deepStrictEqual(reply, new Set(['x']))
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

    test(
      'a missing value ends an array reply',
      { todo: ENGINE_NULL_GAP },
      async () => {
        // A Lua nil ends the array Redis builds from a table.
        const script =
          "redis.setresp(3); return redis.call('HMGET', KEYS[1], 'f', 'nope', 'f')"
        assert.deepStrictEqual(await evalAt(2, script), ['v'])
        assert.deepStrictEqual(await evalAt(3, script), ['v'])
      },
    )
  })

  describe('the protocol belongs to one script run', () => {
    test('redis.setresp(2) switches redis.call back to RESP2 shapes', async () => {
      const script =
        "redis.setresp(3); redis.setresp(2); local r = redis.call('HGETALL', KEYS[1]); return {type(r.map), r[1], r[2]}"
      assert.deepStrictEqual(await evalAt(3, script), ['nil', 'f', 'v'])
    })

    test('every EVAL starts at RESP2 again', async () => {
      const probe = "return type(redis.call('HGETALL', KEYS[1]).map)"
      assert.strictEqual(await evalAt(3, 'redis.setresp(3); return 1'), 1)
      assert.strictEqual(await evalAt(3, probe), 'nil')
      await assert.rejects(() =>
        evalAt(3, "redis.setresp(3); return redis.call('NOSUCHCOMMAND')"),
      )
      assert.strictEqual(await evalAt(3, probe), 'nil')
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
