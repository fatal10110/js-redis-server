import { describe, test } from 'node:test'
import assert from 'node:assert'

import {
  createStreamData,
  type RedisStreamConsumerGroup,
  type RedisStreamData,
  type StreamId,
} from '../src/state/data-types'
import {
  streamEntriesReadAfterDelivery,
  streamGroupLag,
  type StreamLagRules,
} from '../src/state/stream-groups'

const REDIS_7_2: StreamLagRules = { tombstoneCheck: false, shortcuts: false }
const VALKEY_9_0: StreamLagRules = { tombstoneCheck: true, shortcuts: false }
const REDIS_7_4: StreamLagRules = { tombstoneCheck: true, shortcuts: true }

function id(ms: number): StreamId {
  return { ms: BigInt(ms), seq: 1n }
}

/** A stream that had entries `ms`-1 for each of `added`, of which `kept` remain. */
function stream(
  added: number[],
  kept: number[],
  maxDeleted = 0,
): RedisStreamData {
  const s = createStreamData()
  s.entries = kept.map(ms => ({ id: id(ms), fields: [] }))
  s.lastId = id(added[added.length - 1])
  s.entriesAdded = BigInt(added.length)
  s.maxDeletedEntryId = maxDeleted ? id(maxDeleted) : { ms: 0n, seq: 0n }
  return s
}

function group(
  lastMs: number,
  entriesRead: bigint | null,
): RedisStreamConsumerGroup {
  return {
    name: Buffer.from('g'),
    lastDeliveredId: lastMs ? id(lastMs) : { ms: 0n, seq: 0n },
    entriesRead,
    consumers: new Map(),
    pending: new Map(),
  }
}

describe('consumer-group lag port (real t_stream.c)', () => {
  test('a stream emptied by XDEL, then appended to', () => {
    // 1..3 added, all read, 2-1 and 3-1 deleted after 1-1: empty, then 4-1.
    const s = stream([1, 2, 3, 4], [4], 3)
    const g = group(3, 3n)
    for (const rules of [REDIS_7_2, VALKEY_9_0, REDIS_7_4]) {
      assert.strictEqual(streamGroupLag(s, g, rules), 1n)
    }
  })

  test('a read past a deletion ahead of the group', () => {
    // 1..3, group read 1-1, 2-1 deleted; the next read delivers 3-1.
    const s = stream([1, 2, 3], [1, 3], 2)
    const g = group(1, 1n)
    assert.strictEqual(streamGroupLag(s, g, REDIS_7_2), null)
    assert.strictEqual(
      streamEntriesReadAfterDelivery(s, g, id(3), REDIS_7_2),
      2n,
    )
    assert.strictEqual(
      streamEntriesReadAfterDelivery(s, g, id(3), VALKEY_9_0),
      3n,
    )
  })

  test('an int64 read counter', () => {
    const s = stream([1, 2], [1, 2])
    const g = group(0, 9223372036854775807n)
    assert.strictEqual(streamGroupLag(s, g, REDIS_7_2), -9223372036854775805n)
    assert.strictEqual(streamGroupLag(s, g, VALKEY_9_0), -9223372036854775805n)
    assert.strictEqual(streamGroupLag(s, g, REDIS_7_4), 2n)
  })
})
