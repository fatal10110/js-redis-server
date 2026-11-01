import { test, describe, afterEach } from 'node:test'
import assert from 'node:assert'
import { createRequire } from 'node:module'
import {
  createNodeRedisMock,
  type NodeRedisMockClient,
  type NodeRedisMockCluster,
} from '../../src/client-mocks/node-redis-mock'
import { NODE_REDIS_COMMAND_NAMES } from '../../src/client-mocks/node-redis-command-names'

// #490: the facade's node-redis command methods come from node-redis' own
// command definitions (`@redis/client` COMMANDS: parseCommand +
// transformReply). Every expected reply below was observed through real
// node-redis 6.0.0 against Redis 8.0.0, at RESP2 and RESP3, by running the
// same calls on both clients.

test('the typed command names are the installed node-redis camelCase COMMANDS', () => {
  const fromRedis = createRequire(require.resolve('redis'))
  const commands = (
    fromRedis('@redis/client/dist/lib/commands') as {
      default: Record<string, unknown>
    }
  ).default
  const camelCase = Object.keys(commands)
    .filter(name => name !== name.toUpperCase())
    .sort()
  assert.deepStrictEqual([...NODE_REDIS_COMMAND_NAMES].sort(), camelCase)
})

type Case = [name: string, run: (c: NodeRedisMockClient) => Promise<unknown>]

// Run in order on one client: later cases read what earlier ones wrote.
function cases(RESP: 2 | 3): { case: Case; expected: unknown }[] {
  const table: [...Case, unknown][] = [
    [
      'zAdd',
      c =>
        c.zAdd('z', [
          { score: 1.5, value: 'a' },
          { score: 2, value: 'b' },
          { score: Infinity, value: 'c' },
        ]),
      3,
    ],
    ['zScore', c => c.zScore('z', 'a'), 1.5],
    ['zScore inf', c => c.zScore('z', 'c'), Infinity],
    ['zScore missing', c => c.zScore('z', 'nope'), null],
    ['zRankWithScore', c => c.zRankWithScore('z', 'b'), { rank: 1, score: 2 }],
    [
      'zRangeByScoreWithScores',
      c => c.zRangeByScoreWithScores('z', '-inf', '+inf'),
      [
        { value: 'a', score: 1.5 },
        { value: 'b', score: 2 },
        { value: 'c', score: Infinity },
      ],
    ],
    [
      'zmPop',
      c => c.zmPop('z', 'MIN', { COUNT: 1 }),
      { key: 'z', members: [{ value: 'a', score: 1.5 }] },
    ],
    ['zIncrBy', c => c.zIncrBy('z', 0.25, 'b'), 2.25],
    ['zmScore', c => c.zmScore('z', ['b', 'x']), [2.25, null]],
    [
      'ZSCORE (upper-case alias)',
      c => (c as unknown as Record<string, typeof c.zScore>).ZSCORE('z', 'b'),
      2.25,
    ],
    ['hSet', c => c.hSet('h', { f: '1', g: '2' }), 2],
    ['hIncrByFloat', c => c.hIncrByFloat('h', 'f', 1.5), '2.5'],
    ['hmGet', c => c.hmGet('h', ['f', 'x']), ['2.5', null]],
    [
      'hScan',
      c => c.hScan('h', '0'),
      {
        cursor: '0',
        entries: [
          { field: 'f', value: '2.5' },
          { field: 'g', value: '2' },
        ],
      },
    ],
    ['hExpire', c => c.hExpire('h', ['f', 'zz'], 100), [1, -2]],
    ['hTTL', c => c.hTTL('h', ['f', 'g', 'zz']), [100, -1, -2]],
    ['hGetDel', c => c.hGetDel('h', ['g']), ['2']],
    ['xAdd', c => c.xAdd('s', '1-1', { a: '1', b: '2' }), '1-1'],
    ['xAdd again', c => c.xAdd('s', '2-1', { a: '3' }), '2-1'],
    [
      'xRange',
      c => c.xRange('s', '-', '+'),
      [
        { id: '1-1', message: { a: '1', b: '2' } },
        { id: '2-1', message: { a: '3' } },
      ],
    ],
    [
      'xRevRange',
      c => c.xRevRange('s', '+', '-', { COUNT: 1 }),
      [{ id: '2-1', message: { a: '3' } }],
    ],
    [
      'xRead',
      c => c.xRead({ key: 's', id: '0' }, { COUNT: 1 }),
      [{ name: 's', messages: [{ id: '1-1', message: { a: '1', b: '2' } }] }],
    ],
    ['xGroupCreate', c => c.xGroupCreate('s', 'g', '0'), 'OK'],
    [
      'xReadGroup',
      c => c.xReadGroup('g', 'c1', { key: 's', id: '>' }),
      [
        {
          name: 's',
          messages: [
            { id: '1-1', message: { a: '1', b: '2' } },
            { id: '2-1', message: { a: '3' } },
          ],
        },
      ],
    ],
    [
      'xPending',
      c => c.xPending('s', 'g'),
      {
        pending: 2,
        firstId: '1-1',
        lastId: '2-1',
        consumers: [{ name: 'c1', deliveriesCounter: 2 }],
      },
    ],
    ['xLen', c => c.xLen('s'), 2],
    [
      'xInfoGroups',
      c => c.xInfoGroups('s'),
      [
        {
          name: 'g',
          consumers: 1,
          pending: 2,
          'last-delivered-id': '2-1',
          'entries-read': 2,
          lag: 0,
        },
      ],
    ],
    [
      'xAutoClaim',
      c => c.xAutoClaim('s', 'g', 'c2', 0, '0-0'),
      {
        nextId: '0-0',
        messages: [
          { id: '1-1', message: { a: '1', b: '2' } },
          { id: '2-1', message: { a: '3' } },
        ],
        deletedMessages: [],
      },
    ],
    ['sAdd', c => c.sAdd('set', ['a', 'b']), 2],
    ['sIsMember', c => c.sIsMember('set', 'a'), 1],
    ['smIsMember', c => c.smIsMember('set', ['a', 'x']), [1, 0]],
    ['sInterCard', c => c.sInterCard(['set']), 2],
    ['sort', c => c.sort('set', { ALPHA: true }), ['a', 'b']],
    ['rPush', c => c.rPush('l', ['a', 'b', 'c']), 3],
    ['lmPop', c => c.lmPop(['l'], 'LEFT', { COUNT: 2 }), ['l', ['a', 'b']]],
    ['blPop', c => c.blPop('l', 1), { key: 'l', element: 'c' }],
    ['blPop timeout', c => c.blPop('none', 0.01), null],
    ['lPos', c => c.lPos('l', 'zz'), null],
    [
      'geoAdd',
      c =>
        c.geoAdd('g', [
          { longitude: 13.361389, latitude: 38.115556, member: 'Palermo' },
          { longitude: 15.087269, latitude: 37.502669, member: 'Catania' },
        ]),
      2,
    ],
    [
      'geoPos',
      c => c.geoPos('g', ['Palermo', 'missing']),
      [
        RESP === 2
          ? { longitude: '13.361389338970184', latitude: '38.1155563954963' }
          : { longitude: 13.361389338970184, latitude: 38.1155563954963 },
        null,
      ],
    ],
    ['geoDist', c => c.geoDist('g', 'Palermo', 'Catania', 'km'), 166.2742],
    ['geoHash', c => c.geoHash('g', 'Palermo'), ['sqc8b49rny0']],
    ['set', c => c.set('k', '10'), 'OK'],
    ['incrByFloat', c => c.incrByFloat('k', 0.5), '10.5'],
    ['getEx', c => c.getEx('k', { type: 'PX', value: 100000 }), '10.5'],
    ['expireTime', c => c.expireTime('nokey'), -2],
    ['type', c => c.type('z'), 'zset'],
    ['copy', c => c.copy('k', 'k2'), 1],
    ['mGet', c => c.mGet(['k', 'nokey']), ['10.5', null]],
    ['mSetNX', c => c.mSetNX(['k3', 'v']), 1],
    [
      'configGet',
      c => c.configGet('maxmemory-policy'),
      { 'maxmemory-policy': 'noeviction' },
    ],
    ['commandGetKeys', c => c.commandGetKeys(['SET', 'a', 'b']), ['a']],
    ['ping', c => c.ping(), 'PONG'],
    ['echo', c => c.echo('hi'), 'hi'],
    ['clientSetName', c => c.clientSetName('n1'), 'OK'],
    ['clientGetName', c => c.clientGetName(), 'n1'],
  ]
  return table.map(([name, run, expected]) => ({
    case: [name, run],
    expected,
  }))
}

for (const RESP of [2, 3] as const) {
  describe(`node-redis mock: node-redis' own command methods (RESP${RESP})`, () => {
    let client: NodeRedisMockClient | undefined
    let cluster: NodeRedisMockCluster | undefined

    afterEach(async () => {
      await client?.quit()
      await cluster?.quit()
      client = undefined
      cluster = undefined
    })

    async function makeClient(): Promise<NodeRedisMockClient> {
      client = (await createNodeRedisMock({ RESP })) as NodeRedisMockClient
      return client
    }

    test('arguments and reply shapes match real node-redis', async () => {
      const c = await makeClient()
      for (const {
        case: [name, run],
        expected,
      } of cases(RESP)) {
        assert.deepStrictEqual(await run(c), expected, name)
      }
    })

    test('errors reject as node-redis: server errors, its own argument checks', async () => {
      const c = await makeClient()
      await c.zAdd('z', { score: 1, value: 'a' })
      await assert.rejects(c.hIncrBy('z', 'x', 1), (err: Error) => {
        assert.strictEqual(err.constructor.name, 'SimpleError')
        assert.strictEqual(
          err.message,
          'WRONGTYPE Operation against a key holding the wrong kind of value',
        )
        return true
      })
      await assert.rejects(c.configSet('nosuch', 'x'), {
        message:
          "ERR Unknown option or number of arguments for CONFIG SET - 'nosuch'",
      })
      // node-redis 5+ takes a scan cursor as a string; its encoder rejects a
      // number, so the facade does too.
      await assert.rejects(c.hScan('h', 0), {
        name: 'TypeError',
        message:
          '"arguments[2]" must be of type "string | Buffer", got number instead.',
      })
    })

    test('multi() queues them and runs each reply through its transform', async () => {
      const c = await makeClient()
      await c.zAdd('z', { score: 2.25, value: 'b' })
      await c.hSet('h', 'f', '2.5')
      await c.xAdd('s', '1-1', { a: '1' })
      assert.deepStrictEqual(
        await c
          .multi()
          .zScore('z', 'b')
          .hGetAll('h')
          .xRange('s', '-', '+')
          .incr('n')
          .exec(),
        [2.25, { f: '2.5' }, [{ id: '1-1', message: { a: '1' } }], 1],
      )
      // A failed command keeps node-redis' MultiErrorReply; the others are
      // still transformed.
      await assert.rejects(
        c.multi().incr('z').zScore('z', 'b').exec(),
        (err: Error & { replies: unknown[]; errorIndexes: number[] }) => {
          assert.strictEqual(err.constructor.name, 'MultiErrorReply')
          assert.deepStrictEqual(err.errorIndexes, [0])
          assert.strictEqual(err.replies[1], 2.25)
          return true
        },
      )
    })

    test('select() resolves undefined and survives a reconnect; duplicate() keeps the configured database', async () => {
      const c = await makeClient()
      await c.set('db0', 'x')
      assert.strictEqual(await c.select(1), undefined)
      assert.strictEqual(await c.get('db0'), null)
      await c.set('db1', 'y')
      await c.quit()
      await c.connect()
      assert.strictEqual(await c.get('db1'), 'y')
      const dup = c.duplicate()
      try {
        assert.strictEqual(await dup.get('db0'), 'x')
      } finally {
        await dup.quit()
      }
    })

    test('multi().select() moves the selected database once EXEC ran', async () => {
      const c = await makeClient()
      assert.deepStrictEqual(await c.multi().select(2).set('k', 'v').exec(), [
        'OK',
        'OK',
      ])
      await c.quit()
      await c.connect()
      assert.strictEqual(await c.get('k'), 'v')
    })

    test('close() resolves undefined, on the client and the cluster', async () => {
      const c = await makeClient()
      assert.strictEqual(await c.close(), undefined)
      assert.strictEqual(c.isOpen, false)
      client = undefined
      const cl = (await createNodeRedisMock({
        cluster: { masters: 3 },
        RESP,
      })) as NodeRedisMockCluster
      assert.strictEqual(await cl.close(), undefined)
      assert.strictEqual(cl.isOpen, false)
    })

    test('the cluster routes them by key, and multi() runs on the first key slot', async () => {
      cluster = (await createNodeRedisMock({
        cluster: { masters: 3 },
        RESP,
      })) as NodeRedisMockCluster
      const c = cluster
      await c.zAdd('{t}z', { score: 1.5, value: 'a' })
      assert.strictEqual(await c.zScore('{t}z', 'a'), 1.5)
      assert.deepStrictEqual(await c.mGet(['{t}z', '{t}none']), [null, null])
      await assert.rejects(c.mGet(['a', 'b']), {
        message: "CROSSSLOT Keys in request don't hash to the same slot",
      })
      assert.strictEqual(await c.ping(), 'PONG')
      assert.deepStrictEqual(
        await c.multi().incr('{t}n').zScore('{t}z', 'a').exec(),
        [1, 1.5],
      )
      // `multi(routing)` picks the node of a keyless transaction.
      assert.deepStrictEqual(await c.multi('{t}').ping().exec(), ['PONG'])
    })

    test('a cluster multi() holds its node: a concurrent command on that node runs after EXEC', async () => {
      cluster = (await createNodeRedisMock({
        cluster: { masters: 3 },
        RESP,
      })) as NodeRedisMockCluster
      const c = cluster
      await c.set('{t}a', 'x')
      // Real node-redis writes MULTI..EXEC as one batch on the node's
      // connection, so a command issued alongside never lands inside it.
      const [tx, got, after] = await Promise.all([
        c.multi().set('{t}k', '1').get('{t}k').exec(),
        c.get('{t}a'),
        c.multi().incr('{t}n').exec(),
      ])
      assert.deepStrictEqual(tx, ['OK', '1'])
      assert.strictEqual(got, 'x')
      assert.deepStrictEqual(after, [1])
    })
  })
}
