import type {
  RedisStreamConsumer,
  RedisStreamConsumerGroup,
  RedisStreamData,
  RedisStreamEntry,
  RedisStreamPendingEntry,
  StreamId,
} from './data-types'
import { compareStreamId } from './stream-ids'

// Pure consumer-group lookups. State-layer so TrackedStreamData can reach them;
// the command-layer groups module re-exports them.

export function findEntry(
  stream: RedisStreamData,
  id: StreamId,
): RedisStreamEntry | null {
  return (
    stream.entries.find(entry => compareStreamId(entry.id, id) === 0) ?? null
  )
}

export function pendingEntriesSorted(
  group: RedisStreamConsumerGroup,
): RedisStreamPendingEntry[] {
  return Array.from(group.pending.values()).sort((a, b) =>
    compareStreamId(a.id, b.id),
  )
}

export function ensureConsumer(
  group: RedisStreamConsumerGroup,
  name: Buffer,
  now: number,
): RedisStreamConsumer {
  const id = name.toString('hex')
  const existing = group.consumers.get(id)
  if (existing) {
    existing.seenAt = now
    return existing
  }

  const consumer: RedisStreamConsumer = {
    name: Buffer.from(name),
    seenAt: now,
    activeAt: null,
  }
  group.consumers.set(id, consumer)
  return consumer
}
