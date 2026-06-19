export type RedisDataTypeName =
  | 'string'
  | 'hash'
  | 'list'
  | 'set'
  | 'zset'
  | 'stream'

export type RedisStringData = {
  type: 'string'
  value: Buffer
}

export type RedisHashData = {
  type: 'hash'
  fields: Map<string, RedisHashField>
}

export type RedisHashField = {
  field: Buffer
  value: Buffer
  expiresAt?: number
}

export type RedisListData = {
  type: 'list'
  values: Buffer[]
}

export type RedisSetData = {
  type: 'set'
  members: Map<string, Buffer>
}

export type RedisSortedSetData = {
  type: 'zset'
  members: Map<string, RedisSortedSetMember>
}

export type RedisSortedSetMember = {
  member: Buffer
  score: number
}

export type StreamId = {
  ms: bigint
  seq: bigint
}

export type RedisStreamEntry = {
  id: StreamId
  // Flat [field1, value1, field2, value2, ...], as Redis stores and replies.
  fields: Buffer[]
}

export type RedisStreamConsumer = {
  name: Buffer
  seenAt: number
  activeAt: number | null
}

export type RedisStreamPendingEntry = {
  id: StreamId
  consumerId: string
  deliveredAt: number
  deliveryCount: number
}

export type RedisStreamConsumerGroup = {
  name: Buffer
  lastDeliveredId: StreamId
  entriesRead: number | null
  consumers: Map<string, RedisStreamConsumer>
  pending: Map<string, RedisStreamPendingEntry>
}

export type RedisStreamData = {
  type: 'stream'
  // Entries kept in ascending id order (append-only; ids are monotonic).
  entries: RedisStreamEntry[]
  // Highest id ever added; drives `*` / `ms-*` id generation. 0-0 when empty.
  lastId: StreamId
  entriesAdded: number
  maxDeletedEntryId: StreamId
  groups: Map<string, RedisStreamConsumerGroup>
}

export type RedisDataValue =
  | RedisStringData
  | RedisHashData
  | RedisListData
  | RedisSetData
  | RedisSortedSetData
  | RedisStreamData

export function cloneRedisDataValue(value: RedisDataValue): RedisDataValue {
  switch (value.type) {
    case 'string':
      return { type: 'string', value: Buffer.from(value.value) }
    case 'hash':
      return {
        type: 'hash',
        fields: new Map(
          Array.from(value.fields, ([id, field]) => [
            id,
            {
              field: Buffer.from(field.field),
              value: Buffer.from(field.value),
              expiresAt: field.expiresAt,
            },
          ]),
        ),
      }
    case 'list':
      return {
        type: 'list',
        values: value.values.map(item => Buffer.from(item)),
      }
    case 'set':
      return {
        type: 'set',
        members: new Map(
          Array.from(value.members, ([id, member]) => [
            id,
            Buffer.from(member),
          ]),
        ),
      }
    case 'zset':
      return {
        type: 'zset',
        members: new Map(
          Array.from(value.members, ([id, member]) => [
            id,
            {
              member: Buffer.from(member.member),
              score: member.score,
            },
          ]),
        ),
      }
    case 'stream':
      return {
        type: 'stream',
        entries: value.entries.map(entry => ({
          id: { ms: entry.id.ms, seq: entry.id.seq },
          fields: entry.fields.map(part => Buffer.from(part)),
        })),
        lastId: { ms: value.lastId.ms, seq: value.lastId.seq },
        entriesAdded: value.entriesAdded,
        maxDeletedEntryId: {
          ms: value.maxDeletedEntryId.ms,
          seq: value.maxDeletedEntryId.seq,
        },
        groups: new Map(
          Array.from(value.groups, ([id, group]) => [
            id,
            {
              name: Buffer.from(group.name),
              lastDeliveredId: {
                ms: group.lastDeliveredId.ms,
                seq: group.lastDeliveredId.seq,
              },
              entriesRead: group.entriesRead,
              consumers: new Map(
                Array.from(group.consumers, ([consumerId, consumer]) => [
                  consumerId,
                  {
                    name: Buffer.from(consumer.name),
                    seenAt: consumer.seenAt,
                    activeAt: consumer.activeAt,
                  },
                ]),
              ),
              pending: new Map(
                Array.from(group.pending, ([pendingId, pending]) => [
                  pendingId,
                  {
                    id: { ms: pending.id.ms, seq: pending.id.seq },
                    consumerId: pending.consumerId,
                    deliveredAt: pending.deliveredAt,
                    deliveryCount: pending.deliveryCount,
                  },
                ]),
              ),
            },
          ]),
        ),
      }
  }
}

export function createStringData(value: Buffer): RedisStringData {
  return { type: 'string', value: Buffer.from(value) }
}

export function createHashData(): RedisHashData {
  return { type: 'hash', fields: new Map() }
}

export function createListData(): RedisListData {
  return { type: 'list', values: [] }
}

export function createSetData(): RedisSetData {
  return { type: 'set', members: new Map() }
}

export function createSortedSetData(): RedisSortedSetData {
  return { type: 'zset', members: new Map() }
}

export function createStreamData(): RedisStreamData {
  return {
    type: 'stream',
    entries: [],
    lastId: { ms: 0n, seq: 0n },
    entriesAdded: 0,
    maxDeletedEntryId: { ms: 0n, seq: 0n },
    groups: new Map(),
  }
}
