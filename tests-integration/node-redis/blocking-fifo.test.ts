import { test, describe, before, after } from 'node:test'
import assert from 'node:assert'
import { RedisClusterType } from 'redis'
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
  setup?: (client: RedisClusterType, key: string) => Promise<unknown>
  // Block on `key` from the waiter with index `i`.
  block: (client: RedisClusterType, key: string, i: number) => Promise<unknown>
  // Feed the `i`-th value (one command per value).
  feed: (client: RedisClusterType, key: string, i: number) => Promise<unknown>
  // The reply the waiter served `i`-th must get.
  expected: (key: string, i: number) => unknown
}

const WAITERS = 3

// Every value is fed with its own command. Real Redis serves the waiter that
// blocked first on each one (FIFO), so waiter i gets value i.
const cases: FifoCase[] = [
  {
    name: 'BLPOP',
    block: (c, key) => c.blPop(key, 5),
    feed: (c, key, i) => c.rPush(key, `v${i}`),
    expected: (key, i) => ({ key, element: `v${i}` }),
  },
  {
    name: 'BRPOP',
    block: (c, key) => c.brPop(key, 5),
    feed: (c, key, i) => c.rPush(key, `v${i}`),
    expected: (key, i) => ({ key, element: `v${i}` }),
  },
  {
    name: 'BLMOVE',
    block: (c, key) => c.blMove(key, `${key}:dst`, 'LEFT', 'RIGHT', 5),
    feed: (c, key, i) => c.rPush(key, `v${i}`),
    expected: (_key, i) => `v${i}`,
  },
  {
    name: 'BZPOPMIN',
    block: (c, key) => c.bzPopMin(key, 5),
    feed: (c, key, i) => c.zAdd(key, { score: i, value: `m${i}` }),
    expected: (key, i) => ({ key, value: `m${i}`, score: i }),
  },
  {
    name: 'BZMPOP',
    block: (c, key) => c.bzmPop(5, key, 'MIN'),
    feed: (c, key, i) => c.zAdd(key, { score: i, value: `m${i}` }),
    expected: (key, i) => ({ key, members: [{ value: `m${i}`, score: i }] }),
  },
  {
    // XREAD BLOCK hands the same entry to every waiter, so its service order is
    // not observable on the wire; XREADGROUP BLOCK consumes the entry, so the
    // consumer that blocked first must be the one that gets it.
    name: 'XREADGROUP BLOCK',
    setup: (c, key) => c.xGroupCreate(key, 'g', '$', { MKSTREAM: true }),
    block: (c, key, i) =>
      c.xReadGroup('g', `c${i}`, { key, id: '>' }, { COUNT: 1, BLOCK: 5000 }),
    feed: (c, key, i) => c.xAdd(key, `${i + 1}-0`, { f: `v${i}` }),
    expected: (key, i) => [
      {
        name: key,
        messages: [{ id: `${i + 1}-0`, message: { f: `v${i}` } }],
      },
    ],
  },
]

describe(`Blocking waiters are served FIFO (node-redis, ${testRunner.getBackendName()})`, () => {
  let feeder: RedisClusterType
  const waiters: RedisClusterType[] = []

  before(async () => {
    // One connection per waiter: a blocked command ties up its connection.
    feeder = (await testRunner.setupNodeRedisCluster()) as RedisClusterType
    for (let i = 0; i < WAITERS; i++) {
      waiters.push(
        (await testRunner.setupNodeRedisCluster()) as RedisClusterType,
      )
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
        waiters[i].xRead({ key, id: '$' }, { COUNT: 1, BLOCK: 5000 }),
      )
      await waitForPark()
    }

    await feeder.xAdd(key, '1-0', { f: 'v' })

    const expected = [
      { name: key, messages: [{ id: '1-0', message: { f: 'v' } }] },
    ]
    assert.deepStrictEqual(
      await Promise.all(replies),
      Array.from({ length: WAITERS }, () => expected),
    )
  })

  test('BLPOP: a single multi-value push serves the waiters in the order they blocked', async () => {
    const key = `{${randomKey()}}`
    const replies: Promise<unknown>[] = []
    for (let i = 0; i < WAITERS; i++) {
      replies.push(waiters[i].blPop(key, 5))
      await waitForPark()
    }

    await feeder.rPush(key, ['v0', 'v1', 'v2'])

    assert.deepStrictEqual(await Promise.all(replies), [
      { key, element: 'v0' },
      { key, element: 'v1' },
      { key, element: 'v2' },
    ])
  })
})
