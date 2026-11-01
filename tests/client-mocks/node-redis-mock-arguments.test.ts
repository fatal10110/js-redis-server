import { test, describe, afterEach } from 'node:test'
import assert from 'node:assert'
import {
  createNodeRedisMock,
  type NodeRedisMockClient,
} from '../../src/client-mocks/node-redis-mock'

// #490: node-redis' array / object argument forms on the facade's curated
// methods. Each reply was observed through real node-redis 6.0.0 against
// Redis 8.0.0 and 7.2.4, at RESP2 and RESP3 (identical on both).

for (const RESP of [2, 3] as const) {
  describe(`node-redis mock: argument forms (RESP${RESP})`, () => {
    let client: NodeRedisMockClient | undefined

    afterEach(async () => {
      await client?.quit()
      client = undefined
    })

    async function makeClient(): Promise<NodeRedisMockClient> {
      client = (await createNodeRedisMock({ RESP })) as NodeRedisMockClient
      return client
    }

    test('variadic methods take an array: sAdd, del, exists, lPush, rPush, watch', async () => {
      const c = await makeClient()
      assert.strictEqual(await c.sAdd('s', ['a', 'b', 'c']), 3)
      assert.strictEqual(await c.sAdd('s', 'd'), 1)
      assert.deepStrictEqual((await c.sMembers('s')).sort(), [
        'a',
        'b',
        'c',
        'd',
      ])
      assert.strictEqual(await c.rPush('l', ['1', '2']), 2)
      assert.strictEqual(await c.lPush('l', ['0', '-1']), 4)
      assert.deepStrictEqual(await c.lRange('l', 0, -1), ['-1', '0', '1', '2'])
      assert.strictEqual(await c.exists(['l', 's', 'missing']), 2)
      assert.strictEqual(await c.exists('l'), 1)
      assert.strictEqual(await c.watch(['l', 's']), 'OK')
      assert.strictEqual(await c.unwatch(), 'OK')
      assert.strictEqual(await c.del(['l', 's', 'missing']), 2)
      // The facade's rest-argument form still works.
      assert.strictEqual(await c.sAdd('s2', 'x', 'y'), 2)
      assert.strictEqual(await c.del('s2'), 1)
    })

    test('hSet takes an object, a Map, tuples and a flat list; numbers become strings', async () => {
      const c = await makeClient()
      assert.strictEqual(await c.hSet('h', { f1: 'v1', f2: 2 }), 2)
      assert.strictEqual(
        await c.hSet(
          'h',
          new Map<string | number, string | number>([
            ['f3', 'v3'],
            [4, 5],
          ]),
        ),
        2,
      )
      assert.strictEqual(
        await c.hSet('h', [
          ['f5', 'v5'],
          ['f6', 'v6'],
        ]),
        2,
      )
      assert.strictEqual(await c.hSet('h', ['f7', 'v7', 'f8', 8]), 2)
      assert.strictEqual(await c.hSet('h', 'f9', 9), 1)
      assert.deepStrictEqual(await c.hGetAll('h'), {
        '4': '5',
        f1: 'v1',
        f2: '2',
        f3: 'v3',
        f5: 'v5',
        f6: 'v6',
        f7: 'v7',
        f8: '8',
        f9: '9',
      })
    })

    test('set takes the options object', async () => {
      const c = await makeClient()
      assert.strictEqual(
        await c.set('a', 'v', { expiration: { type: 'EX', value: 100 } }),
        'OK',
      )
      assert.strictEqual(await c.ttl('a'), 100)
      assert.strictEqual(
        await c.set('a', 'w', { expiration: { type: 'KEEPTTL' } }),
        'OK',
      )
      assert.strictEqual(await c.set('a', 'x', { expiration: 'KEEPTTL' }), 'OK')
      assert.strictEqual(await c.ttl('a'), 100)
      assert.strictEqual(await c.set('b', 1, { PX: 50000, NX: true }), 'OK')
      assert.strictEqual(await c.set('b', 2, { NX: true }), null)
      assert.strictEqual(await c.set('b', 3, { XX: true, GET: true }), '1')
      assert.strictEqual(await c.set('b', 4, { condition: 'XX' }), 'OK')
      assert.strictEqual(await c.get('b'), '4')
      // Raw trailing arguments are still appended as given.
      assert.strictEqual(await c.set('c', 'v', 'EX', '50'), 'OK')
      assert.strictEqual(await c.ttl('c'), 50)
    })

    test('mSet takes tuples, a flat list or an object; an empty list throws', async () => {
      const c = await makeClient()
      assert.strictEqual(await c.mSet({ m1: 'a', m2: 'b' }), 'OK')
      assert.strictEqual(await c.mSet(['m3', 'c', 'm4', 'd']), 'OK')
      assert.strictEqual(await c.mSet([['m5', 'e']]), 'OK')
      assert.deepStrictEqual(
        await Promise.all(['m1', 'm2', 'm3', 'm4', 'm5'].map(k => c.get(k))),
        ['a', 'b', 'c', 'd', 'e'],
      )
      await assert.rejects(() => c.mSet([]), {
        name: 'Error',
        message: 'empty toSet Argument',
      })
    })

    test('expire takes a mode', async () => {
      const c = await makeClient()
      await c.set('k', 'v')
      assert.strictEqual(await c.expire('k', 100, 'NX'), 1)
      assert.strictEqual(await c.expire('k', 200, 'NX'), 0)
      assert.strictEqual(await c.expire('k', 300, 'GT'), 1)
      assert.strictEqual(await c.ttl('k'), 300)
    })

    test('zAdd takes options and sends an infinite score as +inf', async () => {
      const c = await makeClient()
      assert.strictEqual(
        await c.zAdd(
          'z',
          [
            { score: 1, value: 'a' },
            { score: Infinity, value: 'b' },
          ],
          { CH: true },
        ),
        2,
      )
      assert.strictEqual(
        await c.zAdd('z', { score: 5, value: 'a' }, { NX: true }),
        0,
      )
      assert.strictEqual(
        await c.zAdd(
          'z',
          { score: 5, value: 'a' },
          { condition: 'XX', comparison: 'GT', CH: true },
        ),
        1,
      )
      assert.deepStrictEqual(await c.zRangeWithScores('z', 0, -1), [
        { value: 'a', score: 5 },
        { value: 'b', score: Infinity },
      ])
    })

    test('zUnionStore takes one key, weighted keys and AGGREGATE', async () => {
      const c = await makeClient()
      await c.zAdd('z', [
        { score: 5, value: 'a' },
        { score: Infinity, value: 'b' },
      ])
      await c.zAdd('z2', [
        { score: 10, value: 'a' },
        { score: 1, value: 'c' },
      ])
      assert.strictEqual(await c.zUnionStore('u1', 'z'), 2)
      assert.strictEqual(
        await c.zUnionStore(
          'u2',
          [
            { key: 'z', weight: 2 },
            { key: 'z2', weight: 1 },
          ],
          { AGGREGATE: 'MAX' },
        ),
        3,
      )
      assert.deepStrictEqual(await c.zRangeWithScores('u2', 0, -1), [
        { value: 'c', score: 1 },
        { value: 'a', score: 10 },
        { value: 'b', score: Infinity },
      ])
      assert.strictEqual(await c.zUnionStore('u3', { key: 'z2', weight: 3 }), 2)
      assert.deepStrictEqual(await c.zRangeWithScores('u3', 0, -1), [
        { value: 'c', score: 3 },
        { value: 'a', score: 30 },
      ])
    })

    test('multi() takes the same forms', async () => {
      const c = await makeClient()
      assert.deepStrictEqual(
        await c
          .multi()
          .set('x', 1, { EX: 100 })
          .hSet('hh', { a: '1', b: 2 })
          .del(['x', 'nope'])
          .exec(),
        ['OK', 2, 1],
      )
    })

    test('subscribe / pSubscribe take arrays; unsubscribe removes one listener', async () => {
      const c = await makeClient()
      const sub = c.duplicate()
      try {
        const got: string[][] = []
        const l1 = (message: string, channel: string) =>
          got.push(['l1', message, channel])
        const l2 = (message: string, channel: string) =>
          got.push(['l2', message, channel])
        const numSub = () => c.sendCommand(['PUBSUB', 'NUMSUB', 'c1', 'c2'])

        await sub.subscribe(['c1', 'c2'], l1)
        await sub.subscribe('c1', l2)
        await sub.pSubscribe(['p*', 'q*'], l1)
        assert.deepStrictEqual(await numSub(), ['c1', 1, 'c2', 1])
        assert.strictEqual(await c.sendCommand(['PUBSUB', 'NUMPAT']), 2)
        await c.publish('c1', 'm1')
        await c.publish('c2', 'm2')
        await c.publish('px', 'm3')
        for (let i = 0; i < 200 && got.length < 4; i++) {
          await new Promise(resolve => setTimeout(resolve, 5))
        }

        // c1 keeps l1: no UNSUBSCRIBE.
        await sub.unsubscribe('c1', l2)
        assert.deepStrictEqual(await numSub(), ['c1', 1, 'c2', 1])
        // Now neither channel has a listener left.
        await sub.unsubscribe(['c1', 'c2'], l1)
        assert.deepStrictEqual(await numSub(), ['c1', 0, 'c2', 0])
        await sub.pUnsubscribe(['p*'])
        await c.publish('c1', 'm4')
        await c.publish('px', 'm5')
        await c.publish('qx', 'm6')
        for (let i = 0; i < 200 && got.length < 5; i++) {
          await new Promise(resolve => setTimeout(resolve, 5))
        }
        assert.deepStrictEqual(got, [
          ['l1', 'm1', 'c1'],
          ['l2', 'm1', 'c1'],
          ['l1', 'm2', 'c2'],
          ['l1', 'm3', 'px'],
          ['l1', 'm6', 'qx'],
        ])
      } finally {
        await sub.quit()
      }
    })
  })
}
