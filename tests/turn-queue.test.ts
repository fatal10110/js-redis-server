import { describe, test } from 'node:test'
import assert from 'node:assert'
import {
  ClientSession,
  RedisResult,
  RedisValue,
  SerialTurnQueue,
  type RedisTurnHandle,
} from '../src/internal'
import { createRedisSessionHarness as createHarness } from './core-session-test-helpers'

function buf(...tokens: string[]): Buffer[] {
  return tokens.map(t => Buffer.from(t))
}

function arrayResult(items: string[]): RedisResult {
  return RedisResult.create(
    RedisValue.array(items.map(s => RedisValue.bulkString(Buffer.from(s)))),
  )
}

// Drain all pending microtasks so blocking commands reach their parked state.
function yieldToEventLoop(): Promise<void> {
  return new Promise(resolve => setImmediate(resolve))
}

function deferred(): { promise: Promise<void>; resolve: () => void } {
  let resolve!: () => void
  const promise = new Promise<void>(r => {
    resolve = r
  })
  return { promise, resolve }
}

/** Record `name` when `turn` is granted, then hand the turn straight back. */
function recordGrant(
  order: string[],
  name: string,
  turn: Promise<RedisTurnHandle>,
): Promise<void> {
  return turn.then(handle => {
    order.push(name)
    handle.release()
  })
}

describe('SerialTurnQueue', () => {
  test('resumed turns are granted FIFO, ahead of turns already queued', async () => {
    const queue = new SerialTurnQueue()
    const waits = [deferred(), deferred(), deferred()]
    const resumed: Promise<RedisTurnHandle>[] = []
    for (const wait of waits) {
      const turn = await queue.waitTurn()
      resumed.push(turn.suspend(wait.promise))
    }

    // A writer holds the turn; another command is already queued behind it.
    const writer = await queue.waitTurn()
    const order: string[] = []
    const granted = [
      recordGrant(order, 'queued', queue.waitTurn()),
      ...resumed.map((turn, i) => recordGrant(order, `parked${i}`, turn)),
    ]

    for (const wait of waits) wait.resolve()
    await yieldToEventLoop()
    writer.release()
    await Promise.all(granted)

    assert.deepStrictEqual(order, ['parked0', 'parked1', 'parked2', 'queued'])
  })

  test('bindResume takes the place in line synchronously', async () => {
    const queue = new SerialTurnQueue()
    const waits = [deferred(), deferred(), deferred()]
    const resumes: Array<() => void> = []
    const resumed: Promise<RedisTurnHandle>[] = []
    for (const wait of waits) {
      const turn = await queue.waitTurn()
      resumed.push(
        turn.suspend(wait.promise, resume => {
          resumes.push(resume)
        }),
      )
    }

    const writer = await queue.waitTurn()
    const order: string[] = []
    const granted = [
      recordGrant(order, 'queued', queue.waitTurn()),
      ...resumed.map((turn, i) => recordGrant(order, `parked${i}`, turn)),
    ]

    // Woken in reverse order, and the writer releases before any wait
    // promise has settled: the synchronous resume alone decides the order.
    for (const i of [2, 0, 1]) {
      resumes[i]()
      waits[i].resolve()
    }
    writer.release()
    await Promise.all(granted)

    assert.deepStrictEqual(order, ['parked2', 'parked0', 'parked1', 'queued'])
  })

  test('a wait that fails after a synchronous resume does not wedge the queue', async () => {
    const queue = new SerialTurnQueue()
    const turn = await queue.waitTurn()
    let fail!: (err: Error) => void
    const wait = new Promise<void>((_, reject) => {
      fail = reject
    })
    let resume!: () => void
    const suspended = turn.suspend(wait, r => {
      resume = r
    })

    const writer = await queue.waitTurn()
    resume()
    fail(new Error('aborted'))
    writer.release()

    await assert.rejects(suspended, /aborted/)
    const next = await queue.waitTurn()
    next.release()
  })
})

describe('blocked sessions are served in the order they blocked', () => {
  test('three BLPOP sessions on one key are served FIFO by one push', async () => {
    const { server, executor, session: pusher } = createHarness()
    const waiters = [0, 1, 2].map(() => new ClientSession({ server, executor }))

    const replies: Promise<RedisResult>[] = []
    for (const waiter of waiters) {
      replies.push(waiter.execute('blpop', buf('q', '0')))
      await yieldToEventLoop()
    }

    await pusher.execute('rpush', buf('q', 'v0', 'v1', 'v2'))
    assert.deepStrictEqual(await Promise.all(replies), [
      arrayResult(['q', 'v0']),
      arrayResult(['q', 'v1']),
      arrayResult(['q', 'v2']),
    ])
  })

  test('three BLPOP sessions on one key are served FIFO by separate pushes', async () => {
    const { server, executor, session: pusher } = createHarness()
    const waiters = [0, 1, 2].map(() => new ClientSession({ server, executor }))

    const replies: Promise<RedisResult>[] = []
    for (const waiter of waiters) {
      replies.push(waiter.execute('blpop', buf('q', '0')))
      await yieldToEventLoop()
    }

    for (const value of ['v0', 'v1', 'v2']) {
      await pusher.execute('rpush', buf('q', value))
    }
    assert.deepStrictEqual(await Promise.all(replies), [
      arrayResult(['q', 'v0']),
      arrayResult(['q', 'v1']),
      arrayResult(['q', 'v2']),
    ])
  })

  test('a woken waiter is served before a command queued behind the push', async () => {
    const { server, executor, session: pusher } = createHarness()
    const waiter = new ClientSession({ server, executor })
    const thief = new ClientSession({ server, executor })

    const reply = waiter.execute('blpop', buf('q', '0'))
    await yieldToEventLoop()

    // Issued back to back: LPOP queues for the turn while RPUSH holds it.
    const pushed = pusher.execute('rpush', buf('q', 'v'))
    const stolen = thief.execute('lpop', buf('q'))

    await pushed
    assert.deepStrictEqual(await reply, arrayResult(['q', 'v']))
    assert.deepStrictEqual(
      await stolen,
      await thief.execute('lpop', buf('missing')),
      'LPOP finds the list already served',
    )
  })
})
