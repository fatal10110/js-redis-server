import { test, describe, afterEach } from 'node:test'
import assert from 'node:assert'
import { MultiErrorReply } from 'redis'
import { createInMemoryClient } from '../src'
import type { InMemoryRedisClient } from '../src'
import {
  createNodeRedisMock,
  type NodeRedisMockClient,
  type NodeRedisMockCluster,
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
/** Run a MULTI whose queue contains failing commands, and read the aggregate. */
async function execExpectingErrors(
  multi: ReturnType<NodeRedisMockClient['multi']>,
): Promise<{ replies: unknown[]; errorIndexes: number[] }> {
  try {
    const replies = await multi.exec()
    assert.fail(`expected a MultiErrorReply, got ${JSON.stringify(replies)}`)
  } catch (err) {
    assert.ok(
      err instanceof MultiErrorReply,
      `expected a MultiErrorReply, got ${err}`,
    )
    return { replies: err.replies, errorIndexes: err.errorIndexes }
  }
}

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

    test('a HELLO inside MULTI only reshapes the replies from itself onward', async () => {
      // Real node-redis@6 against Redis 8.0.6, RESP2 connection running
      // MULTI; ZRANGE …; HELLO 3; ZRANGE …; EXEC:
      //   item0 ["a","1","b","2"]   item1 <RESP3 map>   item2 [["a",1],["b",2]]
      // — i.e. the switch takes effect at the HELLO's own reply, and the items
      // queued before it keep the old shape.
      const c = await seeded(2)
      const replies = await c
        .multi()
        .addCommand(['ZRANGE', 'z', '0', '-1', 'WITHSCORES'])
        .addCommand(['HELLO', '3'])
        .addCommand(['ZRANGE', 'z', '0', '-1', 'WITHSCORES'])
        .exec()

      assert.deepStrictEqual(replies[0], ['a', 1, 'b', 2])
      assert.deepStrictEqual(replies[2], [
        ['a', 1],
        ['b', 2],
      ])
      // And the connection is left on RESP3 afterwards.
      assert.deepStrictEqual(
        await c.sendCommand(['ZRANGE', 'z', '0', '-1', 'WITHSCORES']),
        [
          ['a', 1],
          ['b', 2],
        ],
      )
    })

    // A queued HELLO that *fails* leaves the protocol where it was, so the
    // replay must not apply it. Both orderings matter: a failed switch late in
    // the queue must not reshape the items before it, and one at the head must
    // not reshape the items after it.
    test('a failed HELLO inside MULTI does not move the shape', async () => {
      const c = await seeded(2)
      const failed = await execExpectingErrors(
        c
          .multi()
          .addCommand(['ZRANGE', 'z', '0', '-1', 'WITHSCORES'])
          .addCommand(['HELLO', '3'])
          .addCommand(['ZRANGE', 'z', '0', '-1', 'WITHSCORES'])
          // WRONGPASS — the protocol stays on 3, so the replay's prediction of
          // 2 for the tail is wrong and must not be applied.
          .addCommand(['HELLO', '2', 'AUTH', 'u', 'p']),
      )
      assert.deepStrictEqual(failed.errorIndexes, [3])
      assert.deepStrictEqual(failed.replies[0], ['a', 1, 'b', 2])
      assert.deepStrictEqual(failed.replies[2], [
        ['a', 1],
        ['b', 2],
      ])

      // …and one that fails at the head leaves everything after it on RESP2.
      const c2 = await seeded(2)
      const headFailure = await execExpectingErrors(
        c2
          .multi()
          .addCommand(['HELLO', '3', 'AUTH', 'u', 'p'])
          .addCommand(['ZRANGE', 'z', '0', '-1', 'WITHSCORES']),
      )
      assert.deepStrictEqual(headFailure.errorIndexes, [0])
      assert.deepStrictEqual(headFailure.replies[1], ['a', 1, 'b', 2])
    })

    test('HELLO 2 and RESET put the connection back on the flat shape', async () => {
      const c = await seeded(3)
      assert.deepStrictEqual(
        await c.sendCommand(['ZRANGE', 'z', '0', '-1', 'WITHSCORES']),
        [
          ['a', 1],
          ['b', 2],
        ],
      )

      await c.sendCommand(['HELLO', '2'])
      assert.deepStrictEqual(
        await c.sendCommand(['ZRANGE', 'z', '0', '-1', 'WITHSCORES']),
        ['a', 1, 'b', 2],
      )

      await c.sendCommand(['HELLO', '3'])
      await c.sendCommand(['RESET'])
      assert.deepStrictEqual(
        await c.sendCommand(['ZRANGE', 'z', '0', '-1', 'WITHSCORES']),
        ['a', 1, 'b', 2],
      )
    })
  })

  // The cluster facade is the one client where the protocol is not simply its
  // own session's: `HELLO` is keyless, so it reaches `masters[0]` only. Real
  // node-redis hands its RESP setting to every node client when it builds the
  // slot map, so no key may come back in the other protocol's shape.
  describe('node-redis cluster facade', () => {
    const openClusters: NodeRedisMockCluster[] = []
    // Six keys is enough to land on every master of a 3-master cluster.
    const keys = ['k0', 'k1', 'k2', 'k3', 'k4', 'k5']

    afterEach(async () => {
      while (openClusters.length > 0) {
        await openClusters.pop()?.quit()
      }
    })

    async function seededCluster(): Promise<NodeRedisMockCluster> {
      const cluster = (await createNodeRedisMock({
        cluster: { masters: 3 },
      })) as NodeRedisMockCluster
      openClusters.push(cluster)
      for (const key of keys) {
        await cluster.sendCommand(['ZADD', key, '1', 'a', '2', 'b'])
      }
      return cluster
    }

    const zrange = (cluster: NodeRedisMockCluster, key: string) =>
      cluster.sendCommand(['ZRANGE', key, '0', '-1', 'WITHSCORES'])

    const tuples = [
      ['a', 1],
      ['b', 2],
    ]
    const flat = ['a', 1, 'b', 2]

    test('HELLO 3 reaches every node, not just the one it routed to', async () => {
      const cluster = await seededCluster()
      await cluster.sendCommand(['HELLO', '3'])

      for (const key of keys) {
        assert.deepStrictEqual(
          await zrange(cluster, key),
          tuples,
          `key ${key} came back in the wrong shape`,
        )
      }
    })

    test('concurrent commands on different nodes do not cross-contaminate', async () => {
      const cluster = await seededCluster()
      await cluster.sendCommand(['HELLO', '3'])

      // k0 and k2 hash to different masters. Whichever finishes last must not
      // decide the other's shape.
      assert.deepStrictEqual(
        await Promise.all([zrange(cluster, 'k2'), zrange(cluster, 'k0')]),
        [tuples, tuples],
      )
      assert.deepStrictEqual(
        await Promise.all([zrange(cluster, 'k0'), zrange(cluster, 'k2')]),
        [tuples, tuples],
      )
    })

    test('a RESP2 cluster client is flat on every node', async () => {
      const cluster = await seededCluster()
      for (const key of keys) {
        assert.deepStrictEqual(await zrange(cluster, key), flat)
      }
    })

    test('HELLO 2 downgrades every node back to the flat shape', async () => {
      const cluster = await seededCluster()
      await cluster.sendCommand(['HELLO', '3'])
      // Touch a non-masters[0] key so more than one node session is live and
      // has actually been switched to RESP3 before the downgrade.
      assert.deepStrictEqual(await zrange(cluster, 'k0'), tuples)

      await cluster.sendCommand(['HELLO', '2'])
      for (const key of keys) {
        assert.deepStrictEqual(await zrange(cluster, key), flat)
      }
    })

    // The HELLO must be *inside* the concurrent batch: a command that started
    // before the switch landed sees a session still on the old protocol, and
    // must not write that back over the version HELLO negotiated. Awaiting the
    // HELLO first, as the test above does, cannot reach this.
    test('a HELLO raced against a command on another node is not lost', async () => {
      for (const helloFirst of [true, false]) {
        const cluster = await seededCluster()
        const hello = () => cluster.sendCommand(['HELLO', '3'])
        // k0 is not on masters[0], so its session has not synced yet.
        const read = () => zrange(cluster, 'k0')

        await Promise.all(helloFirst ? [hello(), read()] : [read(), hello()])

        for (const key of keys) {
          assert.deepStrictEqual(
            await zrange(cluster, key),
            tuples,
            `HELLO 3 was lost (hello ${helloFirst ? 'first' : 'second'}, key ${key})`,
          )
        }
      }
    })

    test('a HELLO 2 raced against a command on another node is not lost', async () => {
      const cluster = await seededCluster()
      await cluster.sendCommand(['HELLO', '3'])
      assert.deepStrictEqual(await zrange(cluster, 'k0'), tuples)

      await Promise.all([
        cluster.sendCommand(['HELLO', '2']),
        zrange(cluster, 'k0'),
      ])

      for (const key of keys) {
        assert.deepStrictEqual(await zrange(cluster, key), flat)
      }
    })
  })
})
