import { test, describe, before, after } from 'node:test'
import assert from 'node:assert'
import { Cluster, type ChainableCommander } from 'ioredis'
import { TestRunner } from '../test-config'
import { randomKey } from '../utils'

const testRunner = new TestRunner()

// Give a blocking command time to park before the next client blocks, so the
// order the waiters blocked in is well defined.
function waitForPark(ms = 80): Promise<void> {
  return new Promise(resolve => setTimeout(resolve, ms))
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
        await waitForPark()
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
      await waitForPark()
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
      await waitForPark()
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
      await waitForPark()
      const cReply = c.blockOne(waiters[1], k1)
      await waitForPark()

      // Wakes B for k2, which is gone again by the time B looks.
      await c.create(feeder.multi(), k2).del(k2).exec()
      await waitForPark()

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
      await waitForPark()

      await c.wrongType(feeder, key)
      await waitForPark()
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
    await waitForPark()
    await feeder.set(k2, 'foo')
    await waitForPark()
    await feeder.rpush(k1, 'v')

    assert.deepStrictEqual(await reply, [k1, 'v'])
  })
})
