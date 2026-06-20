import { RedisCommandError } from '../../core/redis-error'
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

export class BusyStreamGroupError extends RedisCommandError {
  constructor() {
    super('Consumer Group name already exists', 'BUSYGROUP')
  }
}

export class NoSuchStreamGroupError extends RedisCommandError {
  constructor(key: Buffer, group: Buffer, commandName?: string) {
    const suffix =
      commandName === 'XREADGROUP' ? ' in XREADGROUP with GROUP option' : ''
    super(
      `No such key '${key.toString()}' or consumer group '${group.toString()}'${suffix}`,
      'NOGROUP',
    )
  }
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
    throw new NoSuchStreamGroupError(key, groupName, commandName)
  }
  return group
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
