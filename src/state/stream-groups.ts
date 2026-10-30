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

/**
 * Which form of real Redis' consumer-group lag bookkeeping to follow
 * (`streamReplyWithCGLag` and its helpers in t_stream.c):
 * - `tombstoneCheck`: the Redis 7.4.2 / Valkey 8.0.3 estimate and tombstone
 *   helper, and the XREADGROUP counter update that checks for tombstones
 *   after the group's last id (`stream.group-lag-tombstone-check`);
 * - `readFirstIdCheck`: Redis 7.4.4's XREADGROUP counter update that also
 *   needs the group's last id not behind the first entry
 *   (`stream.group-lag-read-first-id-check`; only with `tombstoneCheck`);
 * - `shortcuts`: Redis 7.4.2's direct lag answers for an emptied stream and
 *   for a group and last tombstone both behind the first entry
 *   (`stream.group-lag-shortcuts`).
 * All off is the Redis 7.0 - 7.2.4 / Valkey 8.0.0 form.
 */
export type StreamLagRules = {
  tombstoneCheck: boolean
  readFirstIdCheck: boolean
  shortcuts: boolean
}

const MIN_STREAM_ID: StreamId = { ms: 0n, seq: 0n }

function isZeroId(id: StreamId): boolean {
  return id.ms === 0n && id.seq === 0n
}

/** Real Redis' `first_id`: the first entry's id, 0-0 for an empty stream. */
export function streamFirstId(stream: RedisStreamData): StreamId {
  return stream.entries[0]?.id ?? MIN_STREAM_ID
}

/**
 * `streamRangeHasTombstones(s, start, NULL)`: whether an entry deleted with
 * XDEL may lie at or after `start`.
 */
export function streamRangeHasTombstones(
  stream: RedisStreamData,
  start: StreamId,
  rules: StreamLagRules,
): boolean {
  const maxDeleted = stream.maxDeletedEntryId
  if (stream.entries.length === 0 || isZeroId(maxDeleted)) return false
  if (
    !rules.tombstoneCheck &&
    compareStreamId(streamFirstId(stream), maxDeleted) > 0
  ) {
    return false
  }
  return compareStreamId(start, maxDeleted) <= 0
}

/**
 * `streamEstimateDistanceFromFirstEverEntry`: the logical read counter of
 * `id` (how many entries were ever added up to it), or null when it cannot
 * be known.
 */
export function streamEstimateEntriesRead(
  stream: RedisStreamData,
  id: StreamId,
  rules: StreamLagRules,
): bigint | null {
  if (stream.entriesAdded === 0n) return 0n

  const length = BigInt(stream.entries.length)
  if (length === 0n && compareStreamId(id, stream.lastId) <= 0) {
    return stream.entriesAdded
  }

  if (
    rules.tombstoneCheck &&
    !isZeroId(id) &&
    compareStreamId(id, stream.maxDeletedEntryId) < 0
  ) {
    return null
  }

  const cmpLast = compareStreamId(id, stream.lastId)
  if (cmpLast === 0) return stream.entriesAdded
  if (cmpLast > 0) return null

  const first = streamFirstId(stream)
  if (
    isZeroId(stream.maxDeletedEntryId) ||
    compareStreamId(stream.maxDeletedEntryId, first) < 0
  ) {
    const cmpFirst = compareStreamId(id, first)
    if (cmpFirst < 0) return stream.entriesAdded - length
    if (cmpFirst === 0) return stream.entriesAdded - length + 1n
  }
  return null
}

/** `streamReplyWithCGLag`: the group's lag, or null (a nil reply) when unknown. */
export function streamGroupLag(
  stream: RedisStreamData,
  group: RedisStreamConsumerGroup,
  rules: StreamLagRules,
): bigint | null {
  if (stream.entriesAdded === 0n) return 0n

  if (rules.shortcuts) {
    const length = BigInt(stream.entries.length)
    if (length === 0n) return 0n
    const first = streamFirstId(stream)
    if (
      compareStreamId(group.lastDeliveredId, first) < 0 &&
      compareStreamId(stream.maxDeletedEntryId, first) < 0
    ) {
      return length
    }
  }

  if (
    group.entriesRead !== null &&
    !streamRangeHasTombstones(stream, group.lastDeliveredId, rules)
  ) {
    return stream.entriesAdded - group.entriesRead
  }

  const estimate = streamEstimateEntriesRead(
    stream,
    group.lastDeliveredId,
    rules,
  )
  return estimate === null ? null : stream.entriesAdded - estimate
}

/**
 * The group's read counter after XREADGROUP delivers `id` (newer than the
 * group's last id, which is still the previous one here), as in real Redis'
 * `streamReplyWithRange`.
 */
export function streamEntriesReadAfterDelivery(
  stream: RedisStreamData,
  group: RedisStreamConsumerGroup,
  id: StreamId,
  rules: StreamLagRules,
): bigint | null {
  const counting = rules.tombstoneCheck
    ? group.entriesRead !== null &&
      (!rules.readFirstIdCheck ||
        compareStreamId(group.lastDeliveredId, streamFirstId(stream)) >= 0) &&
      !streamRangeHasTombstones(stream, group.lastDeliveredId, rules)
    : group.entriesRead !== null && !streamRangeHasTombstones(stream, id, rules)
  if (counting) return (group.entriesRead as bigint) + 1n
  if (stream.entriesAdded !== 0n) {
    return streamEstimateEntriesRead(stream, id, rules)
  }
  return group.entriesRead
}
