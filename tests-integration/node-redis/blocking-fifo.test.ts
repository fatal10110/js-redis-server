import { test, describe, before, after } from 'node:test'
import assert from 'node:assert'
import { RedisClusterType } from 'redis'
import { TestRunner } from '../test-config'
import { errorWithMessage, randomKey } from '../utils'

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

  // A wake that finds nothing ready must re-park the waiter where it was, not
  // behind the waiters that blocked after it.
  const keepPlaceCases: Array<{
    name: string
    blockTwo: (c: RedisClusterType, a: string, b: string) => Promise<unknown>
    blockOne: (c: RedisClusterType, a: string) => Promise<unknown>
    // MULTI that creates `key` and deletes it again.
    createAndDelete: (c: RedisClusterType, key: string) => Promise<unknown>
    feed: (c: RedisClusterType, key: string) => Promise<unknown>
    expected: (key: string) => unknown
  }> = [
    {
      name: 'BLPOP',
      blockTwo: (c, a, b) => c.blPop([a, b], 1),
      blockOne: (c, a) => c.blPop(a, 1),
      createAndDelete: (c, key) => c.multi().rPush(key, 'x').del(key).exec(),
      feed: (c, key) => c.rPush(key, 'v'),
      expected: key => ({ key, element: 'v' }),
    },
    {
      name: 'BLMPOP',
      blockTwo: (c, a, b) => c.blmPop(1, [a, b], 'LEFT'),
      blockOne: (c, a) => c.blmPop(1, a, 'LEFT'),
      createAndDelete: (c, key) => c.multi().rPush(key, 'x').del(key).exec(),
      feed: (c, key) => c.rPush(key, 'v'),
      expected: key => [key, ['v']],
    },
    {
      name: 'BZPOPMIN',
      blockTwo: (c, a, b) => c.bzPopMin([a, b], 1),
      blockOne: (c, a) => c.bzPopMin(a, 1),
      createAndDelete: (c, key) =>
        c.multi().zAdd(key, { score: 0, value: 'x' }).del(key).exec(),
      feed: (c, key) => c.zAdd(key, { score: 1, value: 'm' }),
      expected: key => ({ key, value: 'm', score: 1 }),
    },
    {
      name: 'BZMPOP',
      blockTwo: (c, a, b) => c.bzmPop(1, [a, b], 'MIN'),
      blockOne: (c, a) => c.bzmPop(1, a, 'MIN'),
      createAndDelete: (c, key) =>
        c.multi().zAdd(key, { score: 0, value: 'x' }).del(key).exec(),
      feed: (c, key) => c.zAdd(key, { score: 1, value: 'm' }),
      expected: key => ({ key, members: [{ value: 'm', score: 1 }] }),
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
      await c.createAndDelete(feeder, k2)
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
    block: (c: RedisClusterType, key: string) => Promise<unknown>
    wrongType: (c: RedisClusterType, key: string) => Promise<unknown>
    feed: (c: RedisClusterType, key: string) => Promise<unknown>
    expected: (key: string) => unknown
  }> = [
    {
      name: 'BLPOP',
      block: (c, key) => c.blPop(key, 2),
      wrongType: (c, key) => c.set(key, 'foo'),
      feed: (c, key) => c.rPush(key, 'v'),
      expected: key => ({ key, element: 'v' }),
    },
    {
      name: 'BLMOVE',
      block: (c, key) => c.blMove(key, `${key}:dst`, 'LEFT', 'RIGHT', 2),
      wrongType: (c, key) => c.set(key, 'foo'),
      feed: (c, key) => c.rPush(key, 'v'),
      expected: () => 'v',
    },
    {
      name: 'BLMPOP',
      block: (c, key) => c.blmPop(2, key, 'LEFT'),
      wrongType: (c, key) => c.set(key, 'foo'),
      feed: (c, key) => c.rPush(key, 'v'),
      expected: key => [key, ['v']],
    },
    {
      name: 'BZPOPMIN',
      block: (c, key) => c.bzPopMin(key, 2),
      wrongType: (c, key) => c.rPush(key, 'a'),
      feed: (c, key) => c.zAdd(key, { score: 1, value: 'm' }),
      expected: key => ({ key, value: 'm', score: 1 }),
    },
    {
      name: 'BZMPOP',
      block: (c, key) => c.bzmPop(2, key, 'MIN'),
      wrongType: (c, key) => c.rPush(key, 'a'),
      feed: (c, key) => c.zAdd(key, { score: 1, value: 'm' }),
      expected: key => ({ key, members: [{ value: 'm', score: 1 }] }),
    },
    {
      name: 'XREAD BLOCK',
      block: (c, key) => c.xRead({ key, id: '$' }, { BLOCK: 2000 }),
      wrongType: (c, key) => c.rPush(key, 'a'),
      feed: (c, key) => c.xAdd(key, '1-0', { f: 'v' }),
      expected: key => [
        { name: key, messages: [{ id: '1-0', message: { f: 'v' } }] },
      ],
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

    const reply = waiters[0].blPop([k1, k2], 2)
    await waitForPark()
    await feeder.set(k2, 'foo')
    await waitForPark()
    await feeder.rPush(k1, 'v')

    assert.deepStrictEqual(await reply, { key: k1, element: 'v' })
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
      await feeder.xGroupCreate(key, 'g', '$', { MKSTREAM: true })
      const reply = waiters[0].xReadGroup(
        'g',
        'c',
        { key, id: '>' },
        { BLOCK: 2000 },
      )
      await waitForPark()

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
})
