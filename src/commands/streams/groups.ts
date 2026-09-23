import { errors } from '../../core/redis-error'
import type { RedisDatabase } from '../../state/database'
import type {
  RedisStreamConsumerGroup,
  RedisStreamData,
  StreamId,
} from '../../state/data-types'
import { bufferId, cloneStreamId, compareStreamId, maxStreamId } from './ids'

// Pure consumer-group lookups moved to the state layer (so TrackedStreamData can
// use them); re-exported here for the streams command modules.
export {
  findEntry,
  pendingEntriesSorted,
  ensureConsumer,
} from '../../state/stream-groups'

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
 * Create `consumerName` in the group if it does not exist yet, as its own
 * mutation published as `xgroup-createconsumer` — what real Redis announces
 * when XREADGROUP / XCLAIM / XAUTOCLAIM create a consumer implicitly (their
 * own work publishes nothing). Like XGROUP CREATECONSUMER it leaves a WATCH on
 * the stream intact. The caller has already validated that the group exists.
 */
export function createConsumerIfMissing(
  db: RedisDatabase,
  key: Buffer,
  groupName: Buffer,
  consumerName: Buffer,
  now: number,
): void {
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

export function streamLag(
  stream: RedisStreamData,
  group: RedisStreamConsumerGroup,
): number {
  let lag = 0
  for (const entry of stream.entries) {
    if (compareStreamId(entry.id, group.lastDeliveredId) > 0) {
      lag++
    }
  }
  return lag
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
