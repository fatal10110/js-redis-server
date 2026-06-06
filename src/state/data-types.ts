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

export type RedisStreamData = {
  type: 'stream'
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
      return { type: 'stream' }
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
