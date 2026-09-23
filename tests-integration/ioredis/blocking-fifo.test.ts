import { test, describe, before, after } from 'node:test'
import assert from 'node:assert'
import { Cluster } from 'ioredis'
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
})
