import type { CompatibilityProfile } from '../../core/compatibility'
import { errors } from '../../core/redis-error'
import type { RedisDatabase } from '../../state/database'
import type { StreamLagRules } from '../../state/stream-groups'
import type {
  RedisStreamConsumerGroup,
  RedisStreamData,
  StreamId,
} from '../../state/data-types'
import { bufferId, cloneStreamId, maxStreamId } from './ids'

// Pure consumer-group lookups moved to the state layer (so TrackedStreamData can
// use them); re-exported here for the streams command modules.
export {
  findEntry,
  pendingEntriesSorted,
  ensureConsumer,
  streamGroupLag,
} from '../../state/stream-groups'

/** The profile's form of the consumer-group lag bookkeeping. */
export function streamLagRules(profile: CompatibilityProfile): StreamLagRules {
  return {
    tombstoneCheck: profile.has('stream.group-lag-tombstone-check'),
    readFirstIdCheck: profile.has('stream.group-lag-read-first-id-check'),
    shortcuts: profile.has('stream.group-lag-shortcuts'),
  }
}

/** Buffers in byte order, the order of real Redis' rax-keyed listings. */
export function byName<T extends { name: Buffer }>(items: Iterable<T>): T[] {
  return Array.from(items).sort((a, b) => Buffer.compare(a.name, b.name))
}

export function updateMaxDeletedId(
  stream: RedisStreamData,
  id: StreamId,
): void {
  stream.maxDeletedEntryId = cloneStreamId(
    maxStreamId(stream.maxDeletedEntryId, id),
  )
}

export function streamGroup(
  stream: RedisStreamData,
  groupName: Buffer,
): RedisStreamConsumerGroup | null {
  return stream.groups.get(bufferId(groupName)) ?? null
}

export function requireStreamGroup(
  stream: RedisStreamData | null,
  key: Buffer,
  groupName: Buffer,
  commandName?: string,
): RedisStreamConsumerGroup {
  const group = stream ? streamGroup(stream, groupName) : null
  if (!group) {
    throw errors.noSuchStreamGroup(key, groupName, commandName)
  }
  return group
}

/**
 * Create `consumerName` in the group if it does not exist yet, for XREADGROUP
 * / XCLAIM / XAUTOCLAIM. From Redis 7.0 that is its own mutation published as
 * `xgroup-createconsumer` (their own work publishes nothing); 6.2 creates the
 * consumer silently (`stream.implicit-consumer-event`). Like XGROUP
 * CREATECONSUMER it leaves a WATCH on the stream intact. The caller has
 * already validated that the group exists.
 */
export function createConsumerIfMissing(
  db: RedisDatabase,
  key: Buffer,
  groupName: Buffer,
  consumerName: Buffer,
  now: number,
  profile: CompatibilityProfile,
): void {
  if (!profile.has('stream.implicit-consumer-event')) {
    db.updateStream(key, stream => {
      const group = streamGroup(stream.value, groupName)
      const consumerId = bufferId(consumerName)
      if (!group || group.consumers.has(consumerId)) return
      group.consumers.set(consumerId, {
        name: Buffer.from(consumerName),
        seenAt: now,
        activeAt: null,
      })
    })
    return
  }

  db.withOrigin('xgroup-createconsumer').updateStream(key, stream => {
    const group = streamGroup(stream.value, groupName)
    if (!group) return
    stream.addConsumer(group, bufferId(consumerName), {
      name: Buffer.from(consumerName),
      seenAt: now,
      activeAt: null,
    })
  })
}

export function consumerPendingCount(
  group: RedisStreamConsumerGroup,
  consumerId: string,
): number {
  let count = 0
  for (const pending of group.pending.values()) {
    if (pending.consumerId === consumerId) count++
  }
  return count
}
