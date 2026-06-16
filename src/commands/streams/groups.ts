import { RedisCommandError } from '../../core/redis-error'
import type {
  RedisStreamConsumer,
  RedisStreamConsumerGroup,
  RedisStreamData,
  RedisStreamEntry,
  RedisStreamPendingEntry,
  StreamId,
} from '../../state/data-types'
import { bufferId, cloneStreamId, compareStreamId, maxStreamId } from './ids'

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

export function findEntry(
  stream: RedisStreamData,
  id: StreamId,
): RedisStreamEntry | null {
  return (
    stream.entries.find(entry => compareStreamId(entry.id, id) === 0) ?? null
  )
}

export function ensureConsumer(
  group: RedisStreamConsumerGroup,
  name: Buffer,
  now: number,
): RedisStreamConsumer {
  const id = bufferId(name)
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

export function pendingEntriesSorted(
  group: RedisStreamConsumerGroup,
): RedisStreamPendingEntry[] {
  return Array.from(group.pending.values()).sort((a, b) =>
    compareStreamId(a.id, b.id),
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
