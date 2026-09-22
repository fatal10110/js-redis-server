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

      const upgraded = await c.command('HELLO', 3)
      assert.ok(
        upgraded !== null && !Array.isArray(upgraded),
        `expected HELLO 3 to answer with a map, got ${JSON.stringify(upgraded)}`,
      )

      const downgraded = await c.command('HELLO', 2)
      assert.ok(
        Array.isArray(downgraded),
        `expected HELLO 2 to answer with a flat array, got ${JSON.stringify(downgraded)}`,
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
  })
})
