import { test, describe, before, after } from 'node:test'
import assert from 'node:assert'
import { setTimeout as delay } from 'node:timers/promises'
import { Cluster, type ChainableCommander } from 'ioredis'
import { TestRunner } from '../test-config'
import { errorWithMessage, randomKey } from '../utils'

const testRunner = new TestRunner()

// The number of clients parked in blocking commands across the cluster: INFO
// `blocked_clients`, summed over the masters.
async function blockedClients(cluster: Cluster): Promise<number> {
  const infos = await Promise.all(
    cluster.nodes('master').map(node => node.info('clients')),
  )
  return infos.reduce((sum, info) => sum + blockedCount(info), 0)
}

function blockedCount(info: string): number {
  const match = /^blocked_clients:(\d+)\r?$/m.exec(info)
  assert.ok(match, `no blocked_clients in ${JSON.stringify(info)}`)
  return Number(match[1])
}

// Wait until `count` waiters are parked, so the order they blocked in is well
// defined (polled rather than slept for, so a slow run cannot reorder them).
async function waitForBlocked(cluster: Cluster, count: number): Promise<void> {
  const deadline = Date.now() + 5000
  let blocked = await blockedClients(cluster)
  while (blocked !== count) {
    assert.ok(
      Date.now() < deadline,
      `expected ${count} blocked clients, still ${blocked}`,
    )
    await delay(5)
    blocked = await blockedClients(cluster)
  }
}

// After a write that must not serve the waiters: they are still parked. The
// server settles the wakes a write causes before it reads the next command,
// so one read suffices.
async function expectBlocked(cluster: Cluster, count: number): Promise<void> {
  assert.strictEqual(await blockedClients(cluster), count, 'still blocked')
}

type FifoCase = {
  name: string
  // Prepare the key(s) before anyone blocks (e.g. XGROUP CREATE).
  setup?: (client: Cluster, key: string) => Promise<unknown>
  // Block on `key` from the waiter with index `i`.
  block: (client: Cluster, key: string, i: number) => Promise<unknown>
  // Feed the `i`-th value (one command per value).
  feed: (client: Cluster, key: string, i: number) => Promise<unknown>
  // The reply the waiter served `i`-th must get.
  expected: (key: string, i: number) => unknown
}

const WAITERS = 3

// Every value is fed with its own command. Real Redis serves the waiter that
// blocked first on each one (FIFO), so waiter i gets value i.
const cases: FifoCase[] = [
  {
    name: 'BLPOP',
    block: (c, key) => c.blpop(key, 5),
    feed: (c, key, i) => c.rpush(key, `v${i}`),
    expected: (key, i) => [key, `v${i}`],
  },
  {
    name: 'BRPOP',
    block: (c, key) => c.brpop(key, 5),
    feed: (c, key, i) => c.rpush(key, `v${i}`),
    expected: (key, i) => [key, `v${i}`],
  },
  {
    name: 'BLMOVE',
    block: (c, key) => c.blmove(key, `${key}:dst`, 'LEFT', 'RIGHT', '5'),
    feed: (c, key, i) => c.rpush(key, `v${i}`),
    expected: (_key, i) => `v${i}`,
  },
  {
    name: 'BZPOPMIN',
    block: (c, key) => c.bzpopmin(key, '5'),
    feed: (c, key, i) => c.zadd(key, i, `m${i}`),
    expected: (key, i) => [key, `m${i}`, String(i)],
  },
  {
    name: 'BZMPOP',
    block: (c, key) => c.bzmpop('5', '1', key, 'MIN'),
    feed: (c, key, i) => c.zadd(key, i, `m${i}`),
    expected: (key, i) => [key, [[`m${i}`, String(i)]]],
  },
  {
    // XREAD BLOCK hands the same entry to every waiter, so its service order is
    // not observable on the wire; XREADGROUP BLOCK consumes the entry, so the
    // consumer that blocked first must be the one that gets it.
    name: 'XREADGROUP BLOCK',
    setup: (c, key) => c.xgroup('CREATE', key, 'g', '$', 'MKSTREAM'),
    block: (c, key, i) =>
      c.xreadgroup(
        'GROUP',
        'g',
        `c${i}`,
        'COUNT',
        1,
        'BLOCK',
        5000,
        'STREAMS',
        key,
        '>',
      ),
    feed: (c, key, i) => c.xadd(key, `${i + 1}-0`, 'f', `v${i}`),
    expected: (key, i) => [[key, [[`${i + 1}-0`, ['f', `v${i}`]]]]],
  },
]

describe(`Blocking waiters are served FIFO (${testRunner.getBackendName()})`, () => {
  let feeder: Cluster
  const waiters: Cluster[] = []

  before(async () => {
    // One connection per waiter: a blocked command ties up its connection.
    feeder = await testRunner.setupIoredisCluster()
    for (let i = 0; i < WAITERS; i++) {
      waiters.push(await testRunner.setupIoredisCluster())
    }
    // ioredis opens node connections lazily; a waiter paying a TCP connect on
    // its first command could reach the server after a later waiter.
    await Promise.all(
      [feeder, ...waiters].flatMap(c => c.nodes('master').map(n => n.ping())),
    )
  })

  after(async () => {
    await testRunner.cleanup()
  })

  for (const c of cases) {
    test(`${c.name}: ${WAITERS} waiters on one key are served in the order they blocked`, async () => {
      const key = `{${randomKey()}}`
      await c.setup?.(feeder, key)

      const replies: Promise<unknown>[] = []
      for (let i = 0; i < WAITERS; i++) {
        replies.push(c.block(waiters[i], key, i))
        await waitForBlocked(feeder, i + 1)
      }

      for (let i = 0; i < WAITERS; i++) {
        await c.feed(feeder, key, i)
      }

      const got = await Promise.all(replies)
      assert.deepStrictEqual(
        got,
        Array.from({ length: WAITERS }, (_, i) => c.expected(key, i)),
      )
    })
  }

  test('XREAD BLOCK: every waiter on one key is served the new entry', async () => {
    const key = `{${randomKey()}}`
    const replies: Promise<unknown>[] = []
    for (let i = 0; i < WAITERS; i++) {
      replies.push(
        waiters[i].xread('COUNT', 1, 'BLOCK', 5000, 'STREAMS', key, '$'),
      )
      await waitForBlocked(feeder, i + 1)
    }

    await feeder.xadd(key, '1-0', 'f', 'v')

    const expected = [[key, [['1-0', ['f', 'v']]]]]
    assert.deepStrictEqual(
      await Promise.all(replies),
      Array.from({ length: WAITERS }, () => expected),
    )
  })

  test('BLPOP: a single multi-value push serves the waiters in the order they blocked', async () => {
    const key = `{${randomKey()}}`
    const replies: Promise<unknown>[] = []
    for (let i = 0; i < WAITERS; i++) {
      replies.push(waiters[i].blpop(key, 5))
      await waitForBlocked(feeder, i + 1)
    }

    await feeder.rpush(key, 'v0', 'v1', 'v2')

    assert.deepStrictEqual(await Promise.all(replies), [
      [key, 'v0'],
      [key, 'v1'],
      [key, 'v2'],
    ])
  })

  // A wake that finds nothing ready must re-park the waiter where it was, not
  // behind the waiters that blocked after it.
  const keepPlaceCases: Array<{
    name: string
    // Block on `a` (or `a` + `b` when the command takes several keys).
    blockTwo: (c: Cluster, a: string, b: string) => Promise<unknown>
    blockOne: (c: Cluster, a: string) => Promise<unknown>
    create: (m: ChainableCommander, key: string) => ChainableCommander
    feed: (c: Cluster, key: string) => Promise<unknown>
    expected: (key: string) => unknown
  }> = [
    {
      name: 'BLPOP',
      blockTwo: (c, a, b) => c.blpop(a, b, 1),
      blockOne: (c, a) => c.blpop(a, 1),
      create: (m, key) => m.rpush(key, 'x'),
      feed: (c, key) => c.rpush(key, 'v'),
      expected: key => [key, 'v'],
    },
    {
      name: 'BLMPOP',
      blockTwo: (c, a, b) => c.blmpop(1, 2, a, b, 'LEFT'),
      blockOne: (c, a) => c.blmpop(1, 1, a, 'LEFT'),
      create: (m, key) => m.rpush(key, 'x'),
      feed: (c, key) => c.rpush(key, 'v'),
      expected: key => [key, ['v']],
    },
    {
      name: 'BZPOPMIN',
      blockTwo: (c, a, b) => c.bzpopmin(a, b, 1),
      blockOne: (c, a) => c.bzpopmin(a, 1),
      create: (m, key) => m.zadd(key, 0, 'x'),
      feed: (c, key) => c.zadd(key, 1, 'm'),
      expected: key => [key, 'm', '1'],
    },
    {
      name: 'BZMPOP',
      blockTwo: (c, a, b) => c.bzmpop(1, 2, a, b, 'MIN'),
      blockOne: (c, a) => c.bzmpop(1, 1, a, 'MIN'),
      create: (m, key) => m.zadd(key, 0, 'x'),
      feed: (c, key) => c.zadd(key, 1, 'm'),
      expected: key => [key, [['m', '1']]],
    },
  ]

  for (const c of keepPlaceCases) {
    test(`${c.name}: a waiter woken to find nothing keeps its place in line`, async () => {
      const base = randomKey()
      const k1 = `{${base}}:k1`
      const k2 = `{${base}}:k2`

      // B blocks first (on k1 and k2), C second (on k1 only).
      const b = c.blockTwo(waiters[0], k1, k2)
      await waitForBlocked(feeder, 1)
      const cReply = c.blockOne(waiters[1], k1)
      await waitForBlocked(feeder, 2)

      // Wakes B for k2, which is gone again by the time B looks.
      await c.create(feeder.multi(), k2).del(k2).exec()
      await waitForBlocked(feeder, 2)

      await c.feed(feeder, k1)
      assert.deepStrictEqual(await b, c.expected(k1), 'B blocked first')
      assert.strictEqual(await cReply, null, 'C times out')
    })
  }

  // A write that leaves the key holding another type does not serve the
  // waiter: it stays blocked (no WRONGTYPE) until a value of its type arrives.
  const wrongTypeCases: Array<{
    name: string
    block: (c: Cluster, key: string) => Promise<unknown>
    wrongType: (c: Cluster, key: string) => Promise<unknown>
    feed: (c: Cluster, key: string) => Promise<unknown>
    expected: (key: string) => unknown
  }> = [
    {
      name: 'BLPOP',
      block: (c, key) => c.blpop(key, 2),
      wrongType: (c, key) => c.set(key, 'foo'),
      feed: (c, key) => c.rpush(key, 'v'),
      expected: key => [key, 'v'],
    },
    {
      name: 'BLMOVE',
      block: (c, key) => c.blmove(key, `${key}:dst`, 'LEFT', 'RIGHT', 2),
      wrongType: (c, key) => c.set(key, 'foo'),
      feed: (c, key) => c.rpush(key, 'v'),
      expected: () => 'v',
    },
    {
      name: 'BLMPOP',
      block: (c, key) => c.blmpop(2, 1, key, 'LEFT'),
      wrongType: (c, key) => c.set(key, 'foo'),
      feed: (c, key) => c.rpush(key, 'v'),
      expected: key => [key, ['v']],
    },
    {
      name: 'BZPOPMIN',
      block: (c, key) => c.bzpopmin(key, 2),
      wrongType: (c, key) => c.rpush(key, 'a'),
      feed: (c, key) => c.zadd(key, 1, 'm'),
      expected: key => [key, 'm', '1'],
    },
    {
      name: 'BZMPOP',
      block: (c, key) => c.bzmpop(2, 1, key, 'MIN'),
      wrongType: (c, key) => c.rpush(key, 'a'),
      feed: (c, key) => c.zadd(key, 1, 'm'),
      expected: key => [key, [['m', '1']]],
    },
    {
      name: 'XREAD BLOCK',
      block: (c, key) => c.xread('BLOCK', 2000, 'STREAMS', key, '$'),
      wrongType: (c, key) => c.rpush(key, 'a'),
      feed: (c, key) => c.xadd(key, '1-0', 'f', 'v'),
      expected: key => [[key, [['1-0', ['f', 'v']]]]],
    },
  ]

  for (const c of wrongTypeCases) {
    test(`${c.name}: a write of another type does not wake the waiter`, async () => {
      const key = `{${randomKey()}}`
      let settled = false
      const reply = c.block(waiters[0], key).finally(() => {
        settled = true
      })
      await waitForBlocked(feeder, 1)

      await c.wrongType(feeder, key)
      await expectBlocked(feeder, 1)
      assert.strictEqual(settled, false, 'still blocked, no WRONGTYPE reply')

      await feeder.del(key)
      await c.feed(feeder, key)
      assert.deepStrictEqual(await reply, c.expected(key))
    })
  }

  test('BLPOP k1 k2: a type change on k2 keeps the client blocked for k1', async () => {
    const base = randomKey()
    const k1 = `{${base}}:k1`
    const k2 = `{${base}}:k2`

    const reply = waiters[0].blpop(k1, k2, 2)
    await waitForBlocked(feeder, 1)
    await feeder.set(k2, 'foo')
    await expectBlocked(feeder, 1)
    await feeder.rpush(k1, 'v')

    assert.deepStrictEqual(await reply, [k1, 'v'])
  })

  // Unlike the pop commands, real Redis unblocks XREADGROUP when its stream is
  // overwritten with another type, and the re-run replies WRONGTYPE.
  for (const [how, overwrite] of [
    ['SET', (key: string) => feeder.set(key, 'foo')],
    [
      'MULTI; DEL; SET; EXEC',
      (key: string) => feeder.multi().del(key).set(key, 'foo').exec(),
    ],
  ] as const) {
    test(`XREADGROUP BLOCK: overwriting the stream (${how}) unblocks it with WRONGTYPE`, async () => {
      const key = `{${randomKey()}}`
      await feeder.xgroup('CREATE', key, 'g', '$', 'MKSTREAM')
      const reply = waiters[0].xreadgroup(
        'GROUP',
        'g',
        'c',
        'BLOCK',
        2000,
        'STREAMS',
        key,
        '>',
      )
      await waitForBlocked(feeder, 1)

      // Attach the assertion first: real Redis may reply before the
      // overwrite's own reply arrives.
      const started = Date.now()
      const rejected = assert.rejects(
        reply,
        errorWithMessage(
          'WRONGTYPE Operation against a key holding the wrong kind of value',
        ),
      )
      await overwrite(key)
      await rejected
      assert.ok(Date.now() - started < 1000, 'unblocked, not timed out')
    })
  }

  // Real Redis unblocks XREADGROUP with NOGROUP once its stream or its group
  // is gone (the re-run finds neither), and keeps it blocked while the group
  // survives the change.
  const nogroupCases: Array<[string, (key: string) => Promise<unknown>]> = [
    ['DEL', key => feeder.del(key)],
    ['UNLINK', key => feeder.unlink(key)],
    ['XGROUP DESTROY', key => feeder.xgroup('DESTROY', key, 'g')],
    ['RENAME', key => feeder.rename(key, `${key}:moved`)],
    [
      'MULTI; DEL; XADD; EXEC',
      key => feeder.multi().del(key).xadd(key, '*', 'f', 'v').exec(),
    ],
    ['PEXPIRE (active expiry)', key => feeder.pexpire(key, 100)],
  ]

  for (const [how, act] of nogroupCases) {
    test(`XREADGROUP BLOCK: ${how} unblocks it with NOGROUP`, async () => {
      const key = `{${randomKey()}}`
      await feeder.xgroup('CREATE', key, 'g', '$', 'MKSTREAM')
      const reply = waiters[0].xreadgroup(
        'GROUP',
        'g',
        'c',
        'BLOCK',
        2000,
        'STREAMS',
        key,
        '>',
      )
      await waitForBlocked(feeder, 1)

      const started = Date.now()
      const rejected = assert.rejects(
        reply,
        errorWithMessage(
          `NOGROUP No such key '${key}' or consumer group 'g' in XREADGROUP with GROUP option`,
        ),
      )
      await act(key)
      await rejected
      assert.ok(Date.now() - started < 1000, 'unblocked, not timed out')
    })
  }

  test('XREADGROUP BLOCK on two streams: deleting the second unblocks it with NOGROUP', async () => {
    const base = randomKey()
    const k1 = `{${base}}:1`
    const k2 = `{${base}}:2`
    await feeder.xgroup('CREATE', k1, 'g', '$', 'MKSTREAM')
    await feeder.xgroup('CREATE', k2, 'g', '$', 'MKSTREAM')
    const reply = waiters[0].xreadgroup(
      'GROUP',
      'g',
      'c',
      'BLOCK',
      2000,
      'STREAMS',
      k1,
      k2,
      '>',
      '>',
    )
    await waitForBlocked(feeder, 1)

    const rejected = assert.rejects(
      reply,
      errorWithMessage(
        `NOGROUP No such key '${k2}' or consumer group 'g' in XREADGROUP with GROUP option`,
      ),
    )
    await feeder.del(k2)
    await rejected
  })

  const groupSurvivesCases: Array<[string, (key: string) => Promise<unknown>]> =
    [
      [
        'XGROUP DESTROY of another group',
        async key => {
          await feeder.xgroup('CREATE', key, 'other', '$')
          await feeder.xgroup('DESTROY', key, 'other')
        },
      ],
      [
        'MULTI; XGROUP DESTROY; XGROUP CREATE; EXEC',
        key =>
          feeder
            .multi()
            .xgroup('DESTROY', key, 'g')
            .xgroup('CREATE', key, 'g', '$')
            .exec(),
      ],
    ]

  for (const [how, act] of groupSurvivesCases) {
    test(`XREADGROUP BLOCK: ${how} keeps it blocked`, async () => {
      const key = `{${randomKey()}}`
      await feeder.xgroup('CREATE', key, 'g', '$', 'MKSTREAM')
      let settled = false
      const reply = waiters[0]
        .xreadgroup('GROUP', 'g', 'c', 'BLOCK', 2000, 'STREAMS', key, '>')
        .finally(() => {
          settled = true
        })
      await waitForBlocked(feeder, 1)

      await act(key)
      await expectBlocked(feeder, 1)
      assert.strictEqual(settled, false, 'still blocked')

      await feeder.xadd(key, '1-0', 'f', 'v')
      assert.deepStrictEqual(await reply, [[key, [['1-0', ['f', 'v']]]]])
    })
  }
})
