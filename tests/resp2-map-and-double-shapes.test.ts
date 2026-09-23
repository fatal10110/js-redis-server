import { test, describe, afterEach } from 'node:test'
import assert from 'node:assert'
import { createInMemoryClient } from '../src'
import type { InMemoryRedisClient } from '../src'
import {
  createNodeRedisMock,
  type NodeRedisMockClient,
} from '../src/client-mocks/node-redis-mock'

/**
 * Sibling of `flat-pairs-resp3.test.ts`, covering the other two decoder arms
 * whose shape is decided by the protocol rather than by the client: `map` /
 * `map-pairs`, and the `double` scalar. See #414.
 *
 * RESP2 has neither a map type nor a double type. A map goes on the wire as a
 * flat array (`map`) or as an array of two-element arrays (`map-pairs`), and a
 * double goes out as a bulk string — so a client reading a RESP2 connection
 * hands back exactly those, and only RESP3's `%` and `,` produce an object and
 * a JS number.
 *
 * Ground truth, `node-redis@6` against a real redis-server via `sendCommand`
 * (so no per-command `transformReply` is in the way):
 *
 *   command                      RESP: 2                      RESP: 3
 *   HGETALL h                    ["f1","v1","f2","v2"]        {f1:"v1",f2:"v2"}
 *   HGETALL missing              []                           {}
 *   CONFIG GET maxmemory         ["maxmemory","0"]            {maxmemory:"0"}
 *   XREAD … STREAMS s 0          [["s",[…]]]                  {s:[…]}
 *   ZSCORE z b                   "2.5"                        2.5
 *   ZINCRBY z 1.5 a              "2.5"                        2.5
 *   ZRANGE z 0 -1 WITHSCORES     ["a","1","b","2.5"]          [["a",1],["b",2.5]]
 *   ZPOPMIN z 1                  ["a","1"]                    [["a",1]]
 *
 * The *curated* node-redis methods are a separate matter: `hGetAll()` and
 * `configGet()` build the object themselves inside `transformReply`, from the
 * RESP2 flat array as readily as from the RESP3 map, so they return an object
 * on both protocols. Only the raw `sendCommand` path is protocol-dependent.
 */

describe('map and double shapes follow the negotiated RESP version', () => {
  describe('InMemoryRedisClient', () => {
    let client: InMemoryRedisClient

    afterEach(() => {
      client?.close()
    })

    async function seeded(resp: 2 | 3): Promise<InMemoryRedisClient> {
      client = await createInMemoryClient()
      if (resp === 3) {
        await client.command('HELLO', 3)
      }
      await client.command('HSET', 'h', 'f1', 'v1', 'f2', 'v2')
      await client.command('ZADD', 'z', 1, 'a', 2.5, 'b')
      await client.command('XADD', 's', '1-1', 'fa', 'va', 'fb', 'vb')
      return client
    }

    test('RESP2 reads a map reply as the flat array on the wire', async () => {
      const c = await seeded(2)

      assert.deepStrictEqual(await c.command('HGETALL', 'h'), [
        'f1',
        'v1',
        'f2',
        'v2',
      ])
      assert.deepStrictEqual(await c.command('HGETALL', 'missing'), [])
      assert.deepStrictEqual(await c.command('CONFIG', 'GET', 'maxmemory'), [
        'maxmemory',
        '0',
      ])
    })

    test('RESP2 reads a map-pairs reply as an array of pairs', async () => {
      const c = await seeded(2)

      // XREAD is `map-pairs`: on RESP2 it is `[[stream, entries], …]`, not one
      // flattened array, because that is how the encoder puts it on the wire.
      assert.deepStrictEqual(
        await c.command('XREAD', 'COUNT', 10, 'STREAMS', 's', '0'),
        [['s', [['1-1', ['fa', 'va', 'fb', 'vb']]]]],
      )
    })

    test('RESP3 reads map and map-pairs replies as objects', async () => {
      const c = await seeded(3)

      assert.deepStrictEqual(await c.command('HGETALL', 'h'), {
        f1: 'v1',
        f2: 'v2',
      })
      assert.deepStrictEqual(await c.command('HGETALL', 'missing'), {})
      assert.deepStrictEqual(await c.command('CONFIG', 'GET', 'maxmemory'), {
        maxmemory: '0',
      })
      assert.deepStrictEqual(
        await c.command('XREAD', 'COUNT', 10, 'STREAMS', 's', '0'),
        { s: [['1-1', ['fa', 'va', 'fb', 'vb']]] },
      )
    })

    test('RESP2 reads a sorted-set score as the bulk string on the wire', async () => {
      const c = await seeded(2)

      assert.strictEqual(await c.command('ZSCORE', 'z', 'b'), '2.5')
      assert.strictEqual(await c.command('ZSCORE', 'z', 'missing'), null)
      assert.deepStrictEqual(
        await c.command('ZRANGE', 'z', 0, -1, 'WITHSCORES'),
        ['a', '1', 'b', '2.5'],
      )
      assert.strictEqual(await c.command('ZINCRBY', 'z', 1.5, 'a'), '2.5')
      assert.deepStrictEqual(await c.command('ZPOPMIN', 'z', 1), ['a', '2.5'])
    })

    test('RESP3 reads a sorted-set score as a number', async () => {
      const c = await seeded(3)

      assert.strictEqual(await c.command('ZSCORE', 'z', 'b'), 2.5)
      assert.strictEqual(await c.command('ZSCORE', 'z', 'missing'), null)
      assert.deepStrictEqual(
        await c.command('ZRANGE', 'z', 0, -1, 'WITHSCORES'),
        [
          ['a', 1],
          ['b', 2.5],
        ],
      )
      assert.strictEqual(await c.command('ZINCRBY', 'z', 1.5, 'a'), 2.5)
      assert.deepStrictEqual(await c.command('ZPOPMIN', 'z', 1), [['a', 2.5]])
    })

    test('a RESP2 score keeps the wire spelling of inf and nan', async () => {
      const c = await seeded(2)
      await c.command('ZADD', 'inf', 'inf', 'up', '-inf', 'down')

      assert.strictEqual(await c.command('ZSCORE', 'inf', 'up'), 'inf')
      assert.strictEqual(await c.command('ZSCORE', 'inf', 'down'), '-inf')
    })

    test('HELLO itself answers in the shape of the version it lands on', async () => {
      // HELLO 3's own reply is already RESP3, so it is a map; HELLO 2's is the
      // RESP2 flat array. Both clients read the protocol off the session after
      // the command has run, so this falls out of the same switch.
      const c = await seeded(2)

      // Real node-redis, sendCommand at either protocol:
      //   HELLO 3 → {server:…, proto:3, …}
      //   HELLO 2 → ["server",…,"proto",2,…]
      const upgraded = await c.command('HELLO', 3)
      assert.ok(
        upgraded !== null &&
          typeof upgraded === 'object' &&
          !Array.isArray(upgraded) &&
          !Buffer.isBuffer(upgraded),
        `expected HELLO 3 to answer with a map, got ${JSON.stringify(upgraded)}`,
      )
      assert.strictEqual((upgraded as { proto: number }).proto, 3)

      const downgraded = await c.command('HELLO', 2)
      assert.ok(
        Array.isArray(downgraded),
        `expected HELLO 2 to answer with a flat array, got ${JSON.stringify(downgraded)}`,
      )
      // The flat array is the map's entries in order, so `proto` is a field
      // name followed by its value — not a key on an object.
      const protoAt = downgraded.indexOf('proto')
      assert.ok(protoAt >= 0 && protoAt % 2 === 0, 'proto is a field name')
      assert.strictEqual(downgraded[protoAt + 1], 2)
    })
  })

  describe('node-redis facade', () => {
    const openClients: NodeRedisMockClient[] = []

    afterEach(async () => {
      while (openClients.length > 0) {
        await openClients.pop()?.quit()
      }
    })

    async function seeded(resp: 2 | 3): Promise<NodeRedisMockClient> {
      const client = (await createNodeRedisMock()) as NodeRedisMockClient
      openClients.push(client)
      if (resp === 3) {
        await client.sendCommand(['HELLO', '3'])
      }
      await client.sendCommand(['HSET', 'h', 'f1', 'v1', 'f2', 'v2'])
      await client.sendCommand(['ZADD', 'z', '1', 'a', '2.5', 'b'])
      await client.sendCommand(['XADD', 's', '1-1', 'fa', 'va', 'fb', 'vb'])
      return client
    }

    test('RESP2 sendCommand sees the flat array, RESP3 the object', async () => {
      const resp2 = await seeded(2)
      assert.deepStrictEqual(await resp2.sendCommand(['HGETALL', 'h']), [
        'f1',
        'v1',
        'f2',
        'v2',
      ])
      assert.deepStrictEqual(
        await resp2.sendCommand(['HGETALL', 'missing']),
        [],
      )
      assert.deepStrictEqual(
        await resp2.sendCommand(['CONFIG', 'GET', 'maxmemory']),
        ['maxmemory', '0'],
      )
      assert.deepStrictEqual(
        await resp2.sendCommand(['XREAD', 'COUNT', '10', 'STREAMS', 's', '0']),
        [['s', [['1-1', ['fa', 'va', 'fb', 'vb']]]]],
      )

      const resp3 = await seeded(3)
      assert.deepStrictEqual(await resp3.sendCommand(['HGETALL', 'h']), {
        f1: 'v1',
        f2: 'v2',
      })
      assert.deepStrictEqual(
        await resp3.sendCommand(['HGETALL', 'missing']),
        {},
      )
      assert.deepStrictEqual(
        await resp3.sendCommand(['CONFIG', 'GET', 'maxmemory']),
        { maxmemory: '0' },
      )
      assert.deepStrictEqual(
        await resp3.sendCommand(['XREAD', 'COUNT', '10', 'STREAMS', 's', '0']),
        { s: [['1-1', ['fa', 'va', 'fb', 'vb']]] },
      )
    })

    test('curated hGetAll() stays an object on both protocols', async () => {
      // node-redis' own `hGetAll` transformReply builds the object itself, so
      // the curated method is protocol-independent where `sendCommand` is not.
      // Letting the protocol switch reach it would break the curated API.
      const resp2 = await seeded(2)
      assert.deepStrictEqual(await resp2.hGetAll('h'), { f1: 'v1', f2: 'v2' })
      assert.deepStrictEqual(await resp2.hGetAll('missing'), {})

      const resp3 = await seeded(3)
      assert.deepStrictEqual(await resp3.hGetAll('h'), { f1: 'v1', f2: 'v2' })
      assert.deepStrictEqual(await resp3.hGetAll('missing'), {})
    })

    test('curated hGetAll() still throws on a wrong-type key', async () => {
      // The object shape is pinned for a *map* reply only. An error must still
      // reach the ordinary decoder and throw — real node-redis' `hGetAll()`
      // rejects with WRONGTYPE at both protocols. Coercing every non-map reply
      // to `{}` would swallow it, and the `{}` would be indistinguishable from
      // the missing-key case asserted above.
      for (const resp of [2, 3] as const) {
        const c = await seeded(resp)
        await c.sendCommand(['SET', 'str', 'v'])

        await assert.rejects(
          () => c.hGetAll('str'),
          (err: Error) => {
            assert.match(err.message, /^WRONGTYPE /)
            return true
          },
          `RESP${resp}: hGetAll on a string key must throw, not return {}`,
        )
      }
    })

    test('RESP2 sendCommand sees a score as a string, RESP3 as a number', async () => {
      const resp2 = await seeded(2)
      assert.strictEqual(await resp2.sendCommand(['ZSCORE', 'z', 'b']), '2.5')
      assert.strictEqual(
        await resp2.sendCommand(['ZSCORE', 'z', 'missing']),
        null,
      )
      assert.deepStrictEqual(
        await resp2.sendCommand(['ZRANGE', 'z', '0', '-1', 'WITHSCORES']),
        ['a', '1', 'b', '2.5'],
      )
      assert.strictEqual(
        await resp2.sendCommand(['ZINCRBY', 'z', '1.5', 'a']),
        '2.5',
      )

      const resp3 = await seeded(3)
      assert.strictEqual(await resp3.sendCommand(['ZSCORE', 'z', 'b']), 2.5)
      assert.strictEqual(
        await resp3.sendCommand(['ZSCORE', 'z', 'missing']),
        null,
      )
      assert.deepStrictEqual(
        await resp3.sendCommand(['ZRANGE', 'z', '0', '-1', 'WITHSCORES']),
        [
          ['a', 1],
          ['b', 2.5],
        ],
      )
      assert.strictEqual(
        await resp3.sendCommand(['ZINCRBY', 'z', '1.5', 'a']),
        2.5,
      )
    })

    test('a HELLO mid-connection moves both shapes, MULTI replies included', async () => {
      const c = await seeded(2)

      const replies = await c
        .multi()
        .addCommand(['HGETALL', 'h'])
        .addCommand(['HELLO', '3'])
        .addCommand(['HGETALL', 'h'])
        .addCommand(['ZSCORE', 'z', 'b'])
        .exec()

      assert.deepStrictEqual(replies[0], ['f1', 'v1', 'f2', 'v2'])
      assert.deepStrictEqual(replies[2], { f1: 'v1', f2: 'v2' })
      assert.strictEqual(replies[3], 2.5)
    })

    test('curated hGetAll() fails loudly on a reply that is not a map', async () => {
      // After a raw MULTI the reply is `+QUEUED`. It must not resolve to a
      // string typed as an object.
      const c = await seeded(2)
      await c.sendCommand(['MULTI'])
      await assert.rejects(() => c.hGetAll('h'), /expected a map reply/)
      await c.sendCommand(['DISCARD'])
    })
  })
})

/**
 * The two Lua reply kinds the pair and map tests above cannot reach: a boolean
 * and a big number, both of which a script can return after
 * `redis.setresp(3)`. RESP2 has neither type, so the server writes `:1` / `:0`
 * and the digits as a bulk string.
 *
 * Ground truth, `node-redis@6` against real Redis 8.0.6 via `sendCommand`:
 *
 *   script                                                RESP: 2                  RESP: 3
 *   redis.setresp(3); return true                         1                        true
 *   redis.setresp(3); return false                        0                        false
 *   redis.setresp(3); return {big_number="1234…7890"}     "12345678901234567890"   12345678901234567890n
 *
 * (Real Redis converts `{big_number=…}` without the `setresp(3)` too; the
 * mock's Lua engine does not yet, which is tracked separately, so every script
 * here opts in explicitly.)
 */
describe('Lua boolean and big-number replies follow the negotiated RESP version', () => {
  const TRUE = 'redis.setresp(3); return true'
  const FALSE = 'redis.setresp(3); return false'
  const BIG = 'redis.setresp(3); return {big_number="12345678901234567890"}'

  describe('InMemoryRedisClient', () => {
    let client: InMemoryRedisClient

    afterEach(() => {
      client?.close()
    })

    test('RESP2 reads them as the integer and the digit string', async () => {
      client = await createInMemoryClient()
      assert.strictEqual(await client.command('EVAL', TRUE, 0), 1)
      assert.strictEqual(await client.command('EVAL', FALSE, 0), 0)
      assert.strictEqual(
        await client.command('EVAL', BIG, 0),
        '12345678901234567890',
      )
    })

    test('RESP3 reads them as a boolean and a bigint', async () => {
      client = await createInMemoryClient()
      await client.command('HELLO', 3)
      assert.strictEqual(await client.command('EVAL', TRUE, 0), true)
      assert.strictEqual(await client.command('EVAL', FALSE, 0), false)
      assert.strictEqual(
        await client.command('EVAL', BIG, 0),
        12345678901234567890n,
      )
    })
  })

  describe('node-redis facade', () => {
    const openClients: NodeRedisMockClient[] = []

    afterEach(async () => {
      while (openClients.length > 0) {
        await openClients.pop()?.quit()
      }
    })

    async function connect(resp: 2 | 3): Promise<NodeRedisMockClient> {
      const client = (await createNodeRedisMock()) as NodeRedisMockClient
      openClients.push(client)
      if (resp === 3) {
        await client.sendCommand(['HELLO', '3'])
      }
      return client
    }

    // Real node-redis' `eval()` has no transformReply, so it follows the
    // protocol exactly like `sendCommand` — both are asserted.
    test('RESP2 reads them as the integer and the digit string', async () => {
      const c = await connect(2)
      assert.strictEqual(await c.sendCommand(['EVAL', TRUE, '0']), 1)
      assert.strictEqual(await c.sendCommand(['EVAL', FALSE, '0']), 0)
      assert.strictEqual(
        await c.sendCommand(['EVAL', BIG, '0']),
        '12345678901234567890',
      )
      assert.strictEqual(await c.eval(TRUE), 1)
      assert.strictEqual(await c.eval(BIG), '12345678901234567890')
    })

    test('RESP3 reads them as a boolean and a bigint', async () => {
      const c = await connect(3)
      assert.strictEqual(await c.sendCommand(['EVAL', TRUE, '0']), true)
      assert.strictEqual(await c.sendCommand(['EVAL', FALSE, '0']), false)
      assert.strictEqual(
        await c.sendCommand(['EVAL', BIG, '0']),
        12345678901234567890n,
      )
      assert.strictEqual(await c.eval(TRUE), true)
      assert.strictEqual(await c.eval(BIG), 12345678901234567890n)
    })

    test('a MULTI replays them in the shape of each reply’s own protocol', async () => {
      const c = await connect(2)
      const replies = await c
        .multi()
        .addCommand(['EVAL', TRUE, '0'])
        .addCommand(['HELLO', '3'])
        .addCommand(['EVAL', TRUE, '0'])
        .addCommand(['EVAL', BIG, '0'])
        .exec()

      assert.strictEqual(replies[0], 1)
      assert.strictEqual(replies[2], true)
      assert.strictEqual(replies[3], 12345678901234567890n)
    })
  })
})
