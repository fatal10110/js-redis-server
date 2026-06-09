import { describe, test } from 'node:test'
import assert from 'node:assert'
import { ClientSession, RedisResult, RedisValue } from '../src'
import { createRedisSessionHarness as createHarness } from './core-session-test-helpers'

function buf(...tokens: string[]): Buffer[] {
  return tokens.map(t => Buffer.from(t))
}

function arrayResult(items: (string | null)[]): RedisResult {
  return RedisResult.create(
    RedisValue.array(
      items.map(s => RedisValue.bulkString(s === null ? null : Buffer.from(s))),
    ),
  )
}

function nullArrayResult(): RedisResult {
  return RedisResult.create(RedisValue.nullArray())
}

// Drain all pending microtasks so blocking commands reach their parked state.
function yieldToEventLoop(): Promise<void> {
  return new Promise(resolve => setImmediate(resolve))
}

describe('blocking list commands (unit)', () => {
  test('BLPOP returns immediately when list is non-empty', async () => {
    const { session } = createHarness()
    await session.execute('lpush', buf('k', 'v1'))
    const result = await session.execute('blpop', buf('k', '0'))
    assert.deepStrictEqual(result, arrayResult(['k', 'v1']))
  })

  test('BLPOP checks keys in order and pops first non-empty', async () => {
    const { session } = createHarness()
    await session.execute('lpush', buf('b', 'second'))
    const result = await session.execute('blpop', buf('a', 'b', '0'))
    assert.deepStrictEqual(result, arrayResult(['b', 'second']))
  })

  test('BLPOP timeout returns nil array', async () => {
    const { session } = createHarness()
    const result = await session.execute('blpop', buf('empty', '0.05'))
    assert.deepStrictEqual(result, nullArrayResult())
  })

  test('BLPOP blocks then returns when LPUSH arrives from another session', async () => {
    const { server, executor } = createHarness()
    const session1 = new ClientSession({ server, executor })
    const session2 = new ClientSession({ server, executor })

    const blockPromise = session1.execute('blpop', buf('mylist', '5'))
    await yieldToEventLoop()

    await session2.execute('lpush', buf('mylist', 'hello'))
    const result = await blockPromise
    assert.deepStrictEqual(result, arrayResult(['mylist', 'hello']))
  })

  test('BLPOP multiple keys: unblocks on first key that receives data', async () => {
    const { server, executor } = createHarness()
    const session1 = new ClientSession({ server, executor })
    const session2 = new ClientSession({ server, executor })

    const blockPromise = session1.execute('blpop', buf('a', 'b', 'c', '5'))
    await yieldToEventLoop()

    await session2.execute('lpush', buf('b', 'found'))
    const result = await blockPromise
    assert.deepStrictEqual(result, arrayResult(['b', 'found']))
  })

  test('BRPOP returns immediately when list is non-empty (pops from right)', async () => {
    const { session } = createHarness()
    await session.execute('rpush', buf('k', 'first', 'last'))
    const result = await session.execute('brpop', buf('k', '0'))
    assert.deepStrictEqual(result, arrayResult(['k', 'last']))
  })

  test('BRPOP blocks then returns when RPUSH arrives from another session', async () => {
    const { server, executor } = createHarness()
    const session1 = new ClientSession({ server, executor })
    const session2 = new ClientSession({ server, executor })

    const blockPromise = session1.execute('brpop', buf('mylist', '5'))
    await yieldToEventLoop()

    await session2.execute('rpush', buf('mylist', 'world'))
    const result = await blockPromise
    assert.deepStrictEqual(result, arrayResult(['mylist', 'world']))
  })

  test('BRPOP timeout returns nil array', async () => {
    const { session } = createHarness()
    const result = await session.execute('brpop', buf('empty', '0.05'))
    assert.deepStrictEqual(result, nullArrayResult())
  })
})

describe('XREAD BLOCK (unit)', () => {
  test('XREAD BLOCK returns immediately when entries already exist after id', async () => {
    const { session } = createHarness()
    await session.execute('xadd', buf('s', '1-1', 'f', 'v'))
    const result = await session.execute(
      'xread',
      buf('BLOCK', '0', 'STREAMS', 's', '0-0'),
    )
    assert.ok(result instanceof RedisResult)
    assert.strictEqual(result.value.kind, 'array')
  })

  test('XREAD BLOCK with $ on non-empty stream blocks on new entries only', async () => {
    const { server, executor } = createHarness()
    const session1 = new ClientSession({ server, executor })
    const session2 = new ClientSession({ server, executor })

    await session1.execute('xadd', buf('s', '1-1', 'f', 'v'))

    const blockPromise = session1.execute(
      'xread',
      buf('BLOCK', '5000', 'STREAMS', 's', '$'),
    )
    await yieldToEventLoop()

    await session2.execute('xadd', buf('s', '2-1', 'f', 'v2'))
    const result = await blockPromise

    assert.ok(result instanceof RedisResult)
    assert.strictEqual(result.value.kind, 'array')
  })

  test('XREAD BLOCK timeout returns nil', async () => {
    const { session } = createHarness()
    const result = await session.execute(
      'xread',
      buf('BLOCK', '50', 'STREAMS', 's', '$'),
    )
    assert.deepStrictEqual(
      result,
      RedisResult.create(RedisValue.bulkString(null)),
    )
  })

  test('XREAD non-blocking still works (no BLOCK option)', async () => {
    const { session } = createHarness()
    await session.execute('xadd', buf('s', '1-1', 'f', 'v'))
    const result = await session.execute('xread', buf('STREAMS', 's', '0-0'))
    assert.ok(result instanceof RedisResult)
    assert.strictEqual(result.value.kind, 'array')
  })

  test('XREAD BLOCK with COUNT limits results', async () => {
    const { server, executor } = createHarness()
    const session1 = new ClientSession({ server, executor })
    const session2 = new ClientSession({ server, executor })

    const blockPromise = session1.execute(
      'xread',
      buf('BLOCK', '5000', 'COUNT', '1', 'STREAMS', 's', '$'),
    )
    await yieldToEventLoop()

    await session2.execute('xadd', buf('s', '1-1', 'f', 'v1'))
    await session2.execute('xadd', buf('s', '2-1', 'f', 'v2'))
    const result = await blockPromise

    assert.ok(result instanceof RedisResult)
    assert.strictEqual(result.value.kind, 'array')
    const streamResults = (
      result.value as { kind: 'array'; items: RedisValue[] }
    ).items
    const entries = (streamResults[0] as { kind: 'array'; items: RedisValue[] })
      .items[1]
    assert.strictEqual(
      (entries as { kind: 'array'; items: RedisValue[] }).items.length,
      1,
    )
  })

  test('XREAD COUNT is per-stream: each stream returns up to COUNT entries independently', async () => {
    const { session } = createHarness()
    await session.execute('xadd', buf('a', '1-1', 'f', 'v'))
    await session.execute('xadd', buf('a', '2-1', 'f', 'v'))
    await session.execute('xadd', buf('a', '3-1', 'f', 'v'))
    await session.execute('xadd', buf('b', '1-1', 'f', 'v'))
    await session.execute('xadd', buf('b', '2-1', 'f', 'v'))
    await session.execute('xadd', buf('b', '3-1', 'f', 'v'))

    const result = await session.execute(
      'xread',
      buf('COUNT', '2', 'STREAMS', 'a', 'b', '0-0', '0-0'),
    )

    assert.ok(result instanceof RedisResult)
    const streams = (result.value as { kind: 'array'; items: RedisValue[] })
      .items
    assert.strictEqual(streams.length, 2, 'both streams returned')

    for (const streamResult of streams) {
      const entries = (streamResult as { kind: 'array'; items: RedisValue[] })
        .items[1]
      assert.strictEqual(
        (entries as { kind: 'array'; items: RedisValue[] }).items.length,
        2,
        'COUNT=2 is applied per-stream',
      )
    }
  })
})

describe('blocking commands inside MULTI/EXEC (unit)', () => {
  test('BLPOP inside MULTI/EXEC returns nil array immediately (non-blocking)', async () => {
    const { session } = createHarness()
    await session.execute('multi', buf())
    await session.execute('blpop', buf('nokey', '5'))
    const result = await session.execute('exec', buf())
    // EXEC array has one entry: nil array from non-blocking BLPOP
    assert.strictEqual(result.value.kind, 'array')
    const execItems = (result.value as { kind: 'array'; items: RedisResult[] })
      .items
    assert.strictEqual(execItems.length, 1)
    assert.deepStrictEqual(
      execItems[0],
      RedisValue.nullArray(),
      'BLPOP in MULTI returns nil array without blocking',
    )
  })

  test('XREAD BLOCK inside MULTI/EXEC returns nil immediately (non-blocking)', async () => {
    const { session } = createHarness()
    await session.execute('multi', buf())
    await session.execute(
      'xread',
      buf('BLOCK', '5000', 'STREAMS', 'nostream', '$'),
    )
    const result = await session.execute('exec', buf())
    assert.strictEqual(result.value.kind, 'array')
    const execItems = (result.value as { kind: 'array'; items: RedisResult[] })
      .items
    assert.strictEqual(execItems.length, 1)
    assert.deepStrictEqual(
      execItems[0],
      RedisValue.bulkString(null),
      'XREAD BLOCK in MULTI returns null without blocking',
    )
  })
})

describe('BLPOP thundering herd (unit)', () => {
  test('single push delivers to exactly one of two concurrent waiters', async () => {
    const { server, executor } = createHarness()
    const s1 = new ClientSession({ server, executor })
    const s2 = new ClientSession({ server, executor })
    const s3 = new ClientSession({ server, executor })

    // Two waiters with short timeout so the loser doesn't hang the suite
    const block1 = s1.execute('blpop', buf('k', '0.3'))
    const block2 = s2.execute('blpop', buf('k', '0.3'))
    await yieldToEventLoop()

    // Single push — only one waiter should win
    await s3.execute('lpush', buf('k', 'prize'))

    const [r1, r2] = (await Promise.all([block1, block2])) as RedisResult[]

    const expected = arrayResult(['k', 'prize'])
    const isWinner = (r: RedisResult) => {
      try {
        assert.deepStrictEqual(r, expected)
        return true
      } catch {
        return false
      }
    }

    const winners = [r1, r2].filter(isWinner)
    assert.strictEqual(winners.length, 1, 'exactly one waiter gets the value')
    assert.deepStrictEqual(
      [r1, r2].filter(r => !isWinner(r))[0],
      nullArrayResult(),
      'the other waiter times out',
    )
  })
})
