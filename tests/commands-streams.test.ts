import { describe, test } from 'node:test'
import assert from 'node:assert'
import { RedisResult, RedisValue } from '../src/internal'
import { createRedisSessionHarness as createSession } from './core-session-test-helpers'

function buf(...tokens: string[]): Buffer[] {
  return tokens.map(t => Buffer.from(t))
}

function intResult(n: number): RedisResult {
  return RedisResult.create(RedisValue.integer(n))
}

function bulkResult(s: string | null): RedisResult {
  return RedisResult.create(
    RedisValue.bulkString(s === null ? null : Buffer.from(s)),
  )
}

function xreadReplyEntries(result: RedisResult, streamIndex = 0): RedisValue[] {
  assert.strictEqual(result.value.kind, 'map-pairs')
  const [, entries] = result.value.entries[streamIndex]
  assert.strictEqual(entries.kind, 'array')
  return entries.items
}

describe('stream commands (unit)', () => {
  // XTRIM MAXLEN

  test('XTRIM MAXLEN exact removes oldest entries', async () => {
    const { session } = createSession()
    await session.execute('xadd', buf('s', '1-1', 'f', 'v'))
    await session.execute('xadd', buf('s', '2-1', 'f', 'v'))
    await session.execute('xadd', buf('s', '3-1', 'f', 'v'))

    assert.deepStrictEqual(
      await session.execute('xtrim', buf('s', 'MAXLEN', '2')),
      intResult(1),
    )
    assert.deepStrictEqual(
      await session.execute('xlen', buf('s')),
      intResult(2),
    )
  })

  test('XTRIM MAXLEN ~ (approximate) leaves one extra entry when possible', async () => {
    const { session } = createSession()
    await session.execute('xadd', buf('s', '1-1', 'f', 'v'))
    await session.execute('xadd', buf('s', '2-1', 'f', 'v'))
    await session.execute('xadd', buf('s', '3-1', 'f', 'v'))
    await session.execute('xadd', buf('s', '4-1', 'f', 'v'))

    assert.deepStrictEqual(
      await session.execute('xtrim', buf('s', 'MAXLEN', '~', '2')),
      intResult(1),
    )
    assert.deepStrictEqual(
      await session.execute('xlen', buf('s')),
      intResult(3),
    )
  })

  test('XTRIM MINID exact removes entries below threshold', async () => {
    const { session } = createSession()
    await session.execute('xadd', buf('s', '1-0', 'f', 'v'))
    await session.execute('xadd', buf('s', '2-0', 'f', 'v'))
    await session.execute('xadd', buf('s', '3-0', 'f', 'v'))

    assert.deepStrictEqual(
      await session.execute('xtrim', buf('s', 'MINID', '2-0')),
      intResult(1),
    )
    assert.deepStrictEqual(
      await session.execute('xlen', buf('s')),
      intResult(2),
    )
  })

  test('XTRIM MINID ~ (approximate) leaves one eligible entry when possible', async () => {
    const { session } = createSession()
    await session.execute('xadd', buf('s', '1-0', 'f', 'v'))
    await session.execute('xadd', buf('s', '2-0', 'f', 'v'))
    await session.execute('xadd', buf('s', '3-0', 'f', 'v'))

    assert.deepStrictEqual(
      await session.execute('xtrim', buf('s', 'MINID', '~', '3-0')),
      intResult(1),
    )
    assert.deepStrictEqual(
      await session.execute('xlen', buf('s')),
      intResult(2),
    )
  })

  test('XTRIM no-op when within limit returns 0', async () => {
    const { session } = createSession()
    await session.execute('xadd', buf('s', '1-1', 'f', 'v'))
    await session.execute('xadd', buf('s', '2-1', 'f', 'v'))

    assert.deepStrictEqual(
      await session.execute('xtrim', buf('s', 'MAXLEN', '5')),
      intResult(0),
    )
    assert.deepStrictEqual(
      await session.execute('xlen', buf('s')),
      intResult(2),
    )
  })

  test('XTRIM on missing key returns 0 without creating stream', async () => {
    const { session } = createSession()
    assert.deepStrictEqual(
      await session.execute('xtrim', buf('s', 'MAXLEN', '5')),
      intResult(0),
    )
    // Key must not exist.
    assert.deepStrictEqual(
      await session.execute('exists', buf('s')),
      intResult(0),
    )
  })

  // XADD NOMKSTREAM

  test('XADD NOMKSTREAM returns null when key does not exist', async () => {
    const { session } = createSession()
    assert.deepStrictEqual(
      await session.execute('xadd', buf('s', 'NOMKSTREAM', '1-1', 'f', 'v')),
      bulkResult(null),
    )
    assert.deepStrictEqual(
      await session.execute('exists', buf('s')),
      intResult(0),
    )
  })

  test('XADD NOMKSTREAM appends when stream exists', async () => {
    const { session } = createSession()
    await session.execute('xadd', buf('s', '1-1', 'f', 'v'))

    assert.deepStrictEqual(
      await session.execute('xadd', buf('s', 'NOMKSTREAM', '2-1', 'g', 'w')),
      bulkResult('2-1'),
    )
    assert.deepStrictEqual(
      await session.execute('xlen', buf('s')),
      intResult(2),
    )
  })

  // XADD MAXLEN

  test('XADD MAXLEN trims oldest entries after append', async () => {
    const { session } = createSession()
    await session.execute('xadd', buf('s', '1-1', 'f', 'v'))
    await session.execute('xadd', buf('s', '2-1', 'f', 'v'))
    await session.execute('xadd', buf('s', '3-1', 'f', 'v'))
    await session.execute('xadd', buf('s', 'MAXLEN', '2', '4-1', 'f', 'v'))

    assert.deepStrictEqual(
      await session.execute('xlen', buf('s')),
      intResult(2),
    )
    // oldest entries pruned; newest retained
    const range = await session.execute('xrange', buf('s', '-', '+'))
    const ids = (range.value as { items: RedisValue[] }).items.map(item =>
      (
        (item as { items: RedisValue[] }).items[0] as { value: Buffer }
      ).value.toString(),
    )
    assert.deepStrictEqual(ids, ['3-1', '4-1'])
  })

  test('XADD MAXLEN ~ (approximate) leaves one extra entry when possible', async () => {
    const { session } = createSession()
    await session.execute('xadd', buf('s', '1-1', 'f', 'v'))
    await session.execute('xadd', buf('s', '2-1', 'f', 'v'))
    await session.execute('xadd', buf('s', '3-1', 'f', 'v'))
    await session.execute('xadd', buf('s', 'MAXLEN', '~', '2', '4-1', 'f', 'v'))

    assert.deepStrictEqual(
      await session.execute('xlen', buf('s')),
      intResult(3),
    )
  })

  // XREAD

  test('XREAD returns entries strictly after given id', async () => {
    const { session } = createSession()
    await session.execute('xadd', buf('s', '1-1', 'f', '1'))
    await session.execute('xadd', buf('s', '2-1', 'f', '2'))
    await session.execute('xadd', buf('s', '3-1', 'f', '3'))

    const result = await session.execute('xread', buf('STREAMS', 's', '1-1'))
    assert.ok(result.value !== null)
    // Should return entries 2-1 and 3-1 only.
    const streamEntries = xreadReplyEntries(result)
    assert.strictEqual(streamEntries.length, 2)
  })

  test('XREAD COUNT limits returned entries', async () => {
    const { session } = createSession()
    await session.execute('xadd', buf('s', '1-1', 'f', '1'))
    await session.execute('xadd', buf('s', '2-1', 'f', '2'))
    await session.execute('xadd', buf('s', '3-1', 'f', '3'))

    const result = await session.execute(
      'xread',
      buf('COUNT', '1', 'STREAMS', 's', '0-0'),
    )
    assert.ok(result.value !== null)
    const streamEntries = xreadReplyEntries(result)
    assert.strictEqual(streamEntries.length, 1)
  })

  test('XREAD returns null when no new entries', async () => {
    const { session } = createSession()
    await session.execute('xadd', buf('s', '1-1', 'f', '1'))

    assert.deepStrictEqual(
      await session.execute('xread', buf('STREAMS', 's', '1-1')),
      bulkResult(null),
    )
  })

  test('XREAD with $ returns null on same call', async () => {
    const { session } = createSession()
    await session.execute('xadd', buf('s', '1-1', 'f', '1'))

    assert.deepStrictEqual(
      await session.execute('xread', buf('STREAMS', 's', '$')),
      bulkResult(null),
    )
  })

  test('XREAD on missing key returns null', async () => {
    const { session } = createSession()
    assert.deepStrictEqual(
      await session.execute('xread', buf('STREAMS', 's', '0-0')),
      bulkResult(null),
    )
  })

  test('XREAD across two streams returns both', async () => {
    const { session } = createSession()
    await session.execute('xadd', buf('s1', '1-1', 'a', '1'))
    await session.execute('xadd', buf('s2', '2-1', 'b', '2'))

    const result = await session.execute(
      'xread',
      buf('STREAMS', 's1', 's2', '0-0', '0-0'),
    )
    assert.ok(result.value !== null)
    assert.strictEqual(result.value.kind, 'map-pairs')
    assert.strictEqual(result.value.entries.length, 2)
  })

  // Consumer groups (#498)

  async function groupWithEntries(n: number) {
    const harness = createSession()
    for (let i = 1; i <= n; i++) {
      await harness.session.execute('xadd', buf('s', `${i}-1`, 'f', 'v'))
    }
    await harness.session.execute('xgroup', buf('CREATE', 's', 'g', '0'))
    return harness
  }

  function group(server: ReturnType<typeof createSession>['server']) {
    const stream = server.getDatabase(0).getStream(Buffer.from('s'))
    assert.ok(stream)
    const found = stream.groups.get(Buffer.from('g').toString('hex'))
    assert.ok(found)
    return found
  }

  test('XCLAIM LASTID only ever moves the group forward', async () => {
    const { session, server } = await groupWithEntries(3)
    await session.execute(
      'xreadgroup',
      buf('GROUP', 'g', 'c1', 'COUNT', '2', 'STREAMS', 's', '>'),
    )

    await session.execute(
      'xclaim',
      buf('s', 'g', 'c2', '0', '1-1', 'LASTID', '0-1', 'JUSTID'),
    )
    assert.deepStrictEqual(group(server).lastDeliveredId, { ms: 2n, seq: 1n })

    await session.execute('xclaim', buf('s', 'g', 'c2', '0', 'LASTID', '7-0'))
    assert.deepStrictEqual(group(server).lastDeliveredId, { ms: 7n, seq: 0n })
  })

  test('XCLAIM FORCE adds an entry as one delivery and skips the min-idle check', async () => {
    const { session, server } = await groupWithEntries(2)

    const reply = await session.execute(
      'xclaim',
      buf('s', 'g', 'c', '100000', '1-1', '2-1', 'FORCE', 'JUSTID'),
    )
    assert.strictEqual(reply.value.kind, 'array')
    assert.strictEqual(reply.value.items.length, 2)
    const counts = Array.from(
      group(server).pending.values(),
      p => p.deliveryCount,
    )
    assert.deepStrictEqual(counts, [1, 1])
  })

  test('XAUTOCLAIM examines at most COUNT * 10 pending entries', async () => {
    const { session } = await groupWithEntries(25)
    await session.execute(
      'xreadgroup',
      buf('GROUP', 'g', 'c1', 'STREAMS', 's', '>'),
    )

    const reply = await session.execute(
      'xautoclaim',
      buf('s', 'g', 'c2', '999999', '0', 'COUNT', '1'),
    )
    assert.strictEqual(reply.value.kind, 'array')
    // Ten entries examined, none idle long enough: the cursor is the 11th.
    assert.deepStrictEqual(
      reply.value.items[0],
      RedisValue.bulkString(Buffer.from('11-1')),
    )
    assert.deepStrictEqual(reply.value.items[1], RedisValue.array([]))
  })
})
