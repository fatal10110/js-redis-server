import { RedisValue } from '../../core/redis-value'
import type { StreamId } from '../../state/data-types'
import { formatStreamId } from './ids'

export function bulkString(value: string | Buffer): RedisValue {
  return RedisValue.bulkString(
    typeof value === 'string' ? Buffer.from(value) : value,
  )
}

export function nullBulk(): RedisValue {
  return RedisValue.bulkString(null)
}

export function integerValue(value: number | bigint): RedisValue {
  return RedisValue.integer(value)
}

export function streamIdValue(id: StreamId): RedisValue {
  return bulkString(formatStreamId(id))
}

export function entryToReply(id: StreamId, fields: Buffer[]): RedisValue {
  return RedisValue.array([
    RedisValue.bulkString(Buffer.from(formatStreamId(id))),
    RedisValue.array(fields.map(part => RedisValue.bulkString(part))),
  ])
}

// A history entry deleted from the stream: `[id, nil]`, where real Redis
// sends the nil as a null array (`*-1` in RESP2, `_` in RESP3).
export function deletedEntryToReply(id: StreamId): RedisValue {
  return RedisValue.array([
    RedisValue.bulkString(Buffer.from(formatStreamId(id))),
    RedisValue.nullArray(),
  ])
}
