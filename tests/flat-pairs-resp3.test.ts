import { test, describe, afterEach } from 'node:test'
import assert from 'node:assert'
import { createInMemoryClient } from '../src'
import type { InMemoryRedisClient } from '../src'
import {
  createNodeRedisMock,
  type NodeRedisMockClient,
} from '../src/client-mocks/node-redis-mock'

/**
 * `flat-pairs` replies (WITHSCORES / WITHVALUES) are flat `[k, v, k, v, …]` on
 * the RESP2 wire and `[[k, v], …]` tuples on RESP3. Both socketless clients
 * bypass the encoder and decode a `RedisValue` straight to JS, so the shape has
 * to follow the RESP version the session negotiated — otherwise a consumer
 * written against a RESP3 connection (`for (const [field, value] of reply)`)
 * reads characters out of a flat array. See #385.
 *
 * Ground truth, `node-redis@6` against real Redis 8.0.6 via `sendCommand` (so
 * no per-command `transformReply` in the way):
 *
 *   RESP: 2                            RESP: 3 (node-redis 6's default)
 *   ZRANGE z 0 -1 WITHSCORES
 *     ["a","1","b","2"]                  [["a",1],["b",2]]
 *   ZRANDMEMBER z -3 WITHSCORES
 *     ["b","2","a","1","a","1"]          [["b",2],["b",2],["a",1]]
 *   ZPOPMIN z 2
 *     ["a","1","b","2"]                  [["a",1],["b",2]]
 *   ZUNION 1 z WITHSCORES
 *     ["a","1","b","2"]                  [["a",1],["b",2]]
 *   HRANDFIELD h -3 WITHVALUES
 *     ["f2","v2","f1","v1","f1","v1"]    [["f2","v2"],["f2","v2"],["f1","v1"]]
 *
 * ioredis is RESP2-only — its `protocol: 3` option never reaches the wire
 * (`HELLO` still reports `proto 2` against Redis 8.0.6) — so it always sees the
 * flat shape and needs no protocol switch.
 *
 * Two shapes named in #385 are *not* `flat-pairs` and are unchanged, confirmed
 * against the same real server: XRANGE / XREAD entry fields stay a flat array
 * in both RESP2 and RESP3 (`[["1-1",["fa","va","fb","vb"]]]`), and CONFIG GET
 * is a map reply.
 *
 * Note the RESP2 scores below are numbers where real node-redis at RESP2 hands
 * back strings: a sorted-set score is a `double` RedisValue, which these
 * socketless clients decode numerically whatever the protocol. That scalar
 * divergence is separate from the pair *shape* under test here.
 */
describe('flat-pairs shape follows the negotiated RESP version', () => {
  describe('InMemoryRedisClient', () => {
    let client: InMemoryRedisClient

    afterEach(() => {
      client?.close()
    })

    // `one` holds a single member/field so the negative-count RANDMEMBER
    // replies (which repeat members) are deterministic.
    async function seeded(resp: 2 | 3): Promise<InMemoryRedisClient> {
      client = await createInMemoryClient()
      if (resp === 3) {
        await client.command('HELLO', 3)
      }
      await client.command('ZADD', 'z', 1, 'a', 2, 'b')
      await client.command('ZADD', 'zone', 1, 'a')
      await client.command('HSET', 'hone', 'f1', 'v1')
      return client
    }

    test('RESP2 keeps every flat-pairs reply flat', async () => {
      const c = await seeded(2)

      assert.deepStrictEqual(
        await c.command('ZRANGE', 'z', 0, -1, 'WITHSCORES'),
        ['a', 1, 'b', 2],
      )
      assert.deepStrictEqual(
        await c.command('ZRANDMEMBER', 'zone', -2, 'WITHSCORES'),
        ['a', 1, 'a', 1],
      )
      assert.deepStrictEqual(await c.command('ZUNION', 1, 'z', 'WITHSCORES'), [
        'a',
        1,
        'b',
        2,
      ])
      assert.deepStrictEqual(
        await c.command('HRANDFIELD', 'hone', -2, 'WITHVALUES'),
        ['f1', 'v1', 'f1', 'v1'],
      )
      assert.deepStrictEqual(await c.command('ZPOPMIN', 'z', 2), [
        'a',
        1,
        'b',
        2,
      ])
    })

    test('RESP3 decodes every flat-pairs reply into [k, v] tuples', async () => {
      const c = await seeded(3)

      assert.deepStrictEqual(
        await c.command('ZRANGE', 'z', 0, -1, 'WITHSCORES'),
        [
          ['a', 1],
          ['b', 2],
        ],
      )
      assert.deepStrictEqual(
        await c.command('ZRANDMEMBER', 'zone', -2, 'WITHSCORES'),
        [
          ['a', 1],
          ['a', 1],
        ],
      )
      assert.deepStrictEqual(await c.command('ZUNION', 1, 'z', 'WITHSCORES'), [
        ['a', 1],
        ['b', 2],
      ])
      assert.deepStrictEqual(
        await c.command('HRANDFIELD', 'hone', -2, 'WITHVALUES'),
        [
          ['f1', 'v1'],
          ['f1', 'v1'],
        ],
      )
      assert.deepStrictEqual(await c.command('ZPOPMIN', 'z', 2), [
        ['a', 1],
        ['b', 2],
      ])
    })

    test('an empty flat-pairs reply is an empty array on both protocols', async () => {
      const c = await seeded(3)
      assert.deepStrictEqual(
        await c.command('ZRANGE', 'missing', 0, -1, 'WITHSCORES'),
        [],
      )
      assert.deepStrictEqual(
        await c.command('HRANDFIELD', 'missing', 2, 'WITHVALUES'),
        [],
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
      await client.sendCommand(['ZADD', 'z', '1', 'a', '2', 'b'])
      await client.sendCommand(['ZADD', 'zone', '1', 'a'])
      await client.sendCommand(['HSET', 'hone', 'f1', 'v1'])
      return client
    }

    test('RESP2 keeps every flat-pairs reply flat', async () => {
      const c = await seeded(2)

      assert.deepStrictEqual(
        await c.sendCommand(['ZRANGE', 'z', '0', '-1', 'WITHSCORES']),
        ['a', 1, 'b', 2],
      )
      assert.deepStrictEqual(
        await c.sendCommand(['ZRANDMEMBER', 'zone', '-2', 'WITHSCORES']),
        ['a', 1, 'a', 1],
      )
      assert.deepStrictEqual(
        await c.sendCommand(['ZUNION', '1', 'z', 'WITHSCORES']),
        ['a', 1, 'b', 2],
      )
      assert.deepStrictEqual(
        await c.sendCommand(['HRANDFIELD', 'hone', '-2', 'WITHVALUES']),
        ['f1', 'v1', 'f1', 'v1'],
      )
      assert.deepStrictEqual(await c.sendCommand(['ZPOPMIN', 'z', '2']), [
        'a',
        1,
        'b',
        2,
      ])
    })

    test('RESP3 decodes every flat-pairs reply into [k, v] tuples', async () => {
      const c = await seeded(3)

      assert.deepStrictEqual(
        await c.sendCommand(['ZRANGE', 'z', '0', '-1', 'WITHSCORES']),
        [
          ['a', 1],
          ['b', 2],
        ],
      )
      assert.deepStrictEqual(
        await c.sendCommand(['ZRANDMEMBER', 'zone', '-2', 'WITHSCORES']),
        [
          ['a', 1],
          ['a', 1],
        ],
      )
      assert.deepStrictEqual(
        await c.sendCommand(['ZUNION', '1', 'z', 'WITHSCORES']),
        [
          ['a', 1],
          ['b', 2],
        ],
      )
      assert.deepStrictEqual(
        await c.sendCommand(['HRANDFIELD', 'hone', '-2', 'WITHVALUES']),
        [
          ['f1', 'v1'],
          ['f1', 'v1'],
        ],
      )
      assert.deepStrictEqual(await c.sendCommand(['ZPOPMIN', 'z', '2']), [
        ['a', 1],
        ['b', 2],
      ])
    })

    test('RESP3 tuples survive a MULTI/EXEC replay', async () => {
      const c = await seeded(3)
      const replies = await c
        .multi()
        .addCommand(['ZRANGE', 'z', '0', '-1', 'WITHSCORES'])
        .exec()

      assert.deepStrictEqual(replies, [
        [
          ['a', 1],
          ['b', 2],
        ],
      ])
    })
  })
})
