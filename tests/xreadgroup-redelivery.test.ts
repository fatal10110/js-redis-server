import { describe, test } from 'node:test'
import assert from 'node:assert'
import { setTimeout as sleep } from 'node:timers/promises'
import { createRedisSessionHarness as createSession } from './core-session-test-helpers'

/**
 * XREADGROUP history reads (any id but `>`) re-deliver the consumer's pending
 * entries: real Redis bumps each NACK's delivery count and resets its
 * delivery time (t_stream.c streamReplyWithRangeFromConsumerPEL), except for
 * an entry deleted from the stream, whose NACK is left as it was (#624).
 */
function buf(...tokens: string[]): Buffer[] {
  return tokens.map(t => Buffer.from(t))
}

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

/** [id, delivery count, deliveredAt] per PEL entry, by id. */
function pel(server: ReturnType<typeof createSession>['server']) {
  return [...group(server).pending.values()]
    .map((p): [string, number, number] => [
      `${p.id.ms}-${p.id.seq}`,
      p.deliveryCount,
      p.deliveredAt,
    ])
    .sort((a, b) => a[0].localeCompare(b[0]))
}

describe('XREADGROUP history read re-delivery (#624)', () => {
  test('bumps the delivery count and resets the delivery time', async () => {
    const { session, server } = await groupWithEntries(2)
    await session.execute(
      'xreadgroup',
      buf('GROUP', 'g', 'c', 'STREAMS', 's', '>'),
    )
    const deliveredAt = pel(server)[0][2]
    await sleep(5)

    const before = Date.now()
    await session.execute(
      'xreadgroup',
      buf('GROUP', 'g', 'c', 'STREAMS', 's', '0'),
    )
    const rows = pel(server)
    assert.deepStrictEqual(
      rows.map(([id, count]) => [id, count]),
      [
        ['1-1', 2],
        ['2-1', 2],
      ],
    )
    for (const [id, , at] of rows) {
      assert.ok(at >= before && at > deliveredAt, `${id} delivery time kept`)
    }
    // One command time for every re-delivered entry.
    assert.strictEqual(rows[0][2], rows[1][2])

    await session.execute(
      'xreadgroup',
      buf('GROUP', 'g', 'c', 'STREAMS', 's', '0'),
    )
    assert.deepStrictEqual(
      pel(server).map(([id, count]) => [id, count]),
      [
        ['1-1', 3],
        ['2-1', 3],
      ],
    )
  })

  test('leaves the NACK of a deleted entry unchanged', async () => {
    const { session, server } = await groupWithEntries(2)
    await session.execute(
      'xreadgroup',
      buf('GROUP', 'g', 'c', 'STREAMS', 's', '>'),
    )
    await session.execute('xdel', buf('s', '2-1'))
    const [[, , liveAt], deleted] = pel(server)
    await sleep(5)

    await session.execute(
      'xreadgroup',
      buf('GROUP', 'g', 'c', 'STREAMS', 's', '0'),
    )
    const rows = pel(server)
    assert.deepStrictEqual(rows[0].slice(0, 2), ['1-1', 2])
    assert.ok(rows[0][2] > liveAt)
    assert.deepStrictEqual(rows[1], deleted)
    assert.strictEqual(deleted[1], 1)
  })

  test('COUNT, the start id and other consumers bound the bump', async () => {
    const { session, server } = await groupWithEntries(4)
    await session.execute(
      'xreadgroup',
      buf('GROUP', 'g', 'c1', 'COUNT', '3', 'STREAMS', 's', '>'),
    )
    await session.execute(
      'xreadgroup',
      buf('GROUP', 'g', 'c2', 'STREAMS', 's', '>'),
    )

    // Entries after 1-1 only, at most COUNT 1 of them: just 2-1.
    await session.execute(
      'xreadgroup',
      buf('GROUP', 'g', 'c1', 'COUNT', '1', 'STREAMS', 's', '1-1'),
    )
    // A consumer with nothing pending re-delivers nothing.
    await session.execute(
      'xreadgroup',
      buf('GROUP', 'g', 'c3', 'STREAMS', 's', '0'),
    )
    assert.deepStrictEqual(
      pel(server).map(([id, count]) => [id, count]),
      [
        ['1-1', 1],
        ['2-1', 2],
        ['3-1', 1],
        ['4-1', 1],
      ],
    )
  })

  test('a NOACK group read leaves no PEL to bump', async () => {
    const { session, server } = await groupWithEntries(2)
    await session.execute(
      'xreadgroup',
      buf('GROUP', 'g', 'c', 'NOACK', 'STREAMS', 's', '>'),
    )
    await session.execute(
      'xreadgroup',
      buf('GROUP', 'g', 'c', 'STREAMS', 's', '0'),
    )
    assert.strictEqual(group(server).pending.size, 0)
  })
})
