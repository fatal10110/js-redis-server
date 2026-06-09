import { defineCommand } from '../core/command-definition'
import { t, type ParseContext } from '../core/command-schema'
import {
  InvalidStreamIdError,
  StreamIdEqualOrSmallerError,
  StreamIdNotGreaterThanZeroError,
  WrongNumberOfArgumentsError,
} from '../core/redis-error'
import { RedisValue } from '../core/redis-value'
import type { RedisStreamData, StreamId } from '../state/data-types'
import { array, bulk, integer } from './helpers'

const MAX_UINT64 = (1n << 64n) - 1n
const MIN_ID: StreamId = { ms: 0n, seq: 0n }
const MAX_ID: StreamId = { ms: MAX_UINT64, seq: MAX_UINT64 }

function formatStreamId(id: StreamId): string {
  return `${id.ms}-${id.seq}`
}

function compareStreamId(a: StreamId, b: StreamId): number {
  if (a.ms !== b.ms) return a.ms < b.ms ? -1 : 1
  if (a.seq !== b.seq) return a.seq < b.seq ? -1 : 1
  return 0
}

function parseUint64(token: string): bigint | null {
  if (!/^\d+$/.test(token)) return null
  const value = BigInt(token)
  return value > MAX_UINT64 ? null : value
}

// XADD id: "*", "<ms>-*", "<ms>-<seq>", or "<ms>" (seq defaults to 0).
function resolveXaddId(spec: string, lastId: StreamId): StreamId {
  if (spec === '*') {
    return nextAutoId(lastId)
  }

  const dash = spec.indexOf('-')
  if (dash === -1) {
    const ms = parseUint64(spec)
    if (ms === null) throw new InvalidStreamIdError()
    return { ms, seq: 0n }
  }

  const ms = parseUint64(spec.slice(0, dash))
  if (ms === null) throw new InvalidStreamIdError()

  const seqPart = spec.slice(dash + 1)
  if (seqPart === '*') {
    return nextSeqForMs(ms, lastId)
  }

  const seq = parseUint64(seqPart)
  if (seq === null) throw new InvalidStreamIdError()
  return { ms, seq }
}

function nextAutoId(lastId: StreamId): StreamId {
  const now = BigInt(Date.now())
  if (now > lastId.ms) return { ms: now, seq: 0n }
  return { ms: lastId.ms, seq: lastId.seq + 1n }
}

function nextSeqForMs(ms: bigint, lastId: StreamId): StreamId {
  if (ms > lastId.ms) return { ms, seq: 0n }
  // Same ms (or smaller, which is rejected later by the monotonicity check).
  return { ms, seq: lastId.seq + 1n }
}

// XRANGE/XREVRANGE bound. `-`/`+` are the open ends; a leading `(` is exclusive;
// a bare ms defaults its seq to 0 (start) or the max (end).
type RangeBound = { id: StreamId; exclusive: boolean }

function parseRangeId(token: string, isStart: boolean): RangeBound {
  let exclusive = false
  let body = token
  if (body.startsWith('(')) {
    exclusive = true
    body = body.slice(1)
  }

  if (body === '-') return { id: MIN_ID, exclusive }
  if (body === '+') return { id: MAX_ID, exclusive }

  const dash = body.indexOf('-')
  if (dash === -1) {
    const ms = parseUint64(body)
    if (ms === null) throw new InvalidStreamIdError()
    return { id: { ms, seq: isStart ? 0n : MAX_UINT64 }, exclusive }
  }

  const ms = parseUint64(body.slice(0, dash))
  const seq = parseUint64(body.slice(dash + 1))
  if (ms === null || seq === null) throw new InvalidStreamIdError()
  return { id: { ms, seq }, exclusive }
}

// XDEL ids are exact: "<ms>-<seq>", or "<ms>" meaning "<ms>-0".
function parseExactId(token: string): StreamId {
  const dash = token.indexOf('-')
  if (dash === -1) {
    const ms = parseUint64(token)
    if (ms === null) throw new InvalidStreamIdError()
    return { ms, seq: 0n }
  }

  const ms = parseUint64(token.slice(0, dash))
  const seq = parseUint64(token.slice(dash + 1))
  if (ms === null || seq === null) throw new InvalidStreamIdError()
  return { ms, seq }
}

function entryToReply(id: StreamId, fields: Buffer[]): RedisValue {
  return RedisValue.array([
    RedisValue.bulkString(Buffer.from(formatStreamId(id))),
    RedisValue.array(fields.map(part => RedisValue.bulkString(part))),
  ])
}

type FieldList = Buffer[]

// Parses the trailing `field value [field value ...]` of XADD into a flat
// [field1, value1, ...] array, requiring at least one complete pair.
function createStreamFieldsSchema() {
  return t.custom<FieldList>(
    (input: readonly Buffer[], index: number, ctx: ParseContext) => {
      const fields: FieldList = []
      let cursor = index
      while (cursor < input.length) {
        const field = input[cursor]
        const value = input[cursor + 1]
        if (value === undefined) {
          throw new WrongNumberOfArgumentsError(ctx.commandName)
        }
        fields.push(field, value)
        cursor += 2
      }
      if (fields.length === 0) {
        throw new WrongNumberOfArgumentsError(ctx.commandName)
      }
      return { value: fields, nextIndex: cursor }
    },
  )
}

export const xaddCommand = defineCommand({
  name: 'xadd',
  schema: t.object({
    key: t.key(),
    id: t.string(),
    fields: createStreamFieldsSchema(),
  }),
  flags: ['write', 'denyoom', 'fast'],
  keys: args => [args.key],
  execute: (args, ctx) => {
    const id = ctx.db.updateStream(args.key, stream => {
      const next = resolveXaddId(args.id, stream.lastId)

      // An id must be > 0-0 and strictly greater than the stream's last id.
      // lastId is 0-0 for a brand-new stream and is retained even after XDEL
      // empties the stream, so this single comparison covers every case.
      if (compareStreamId(next, MIN_ID) <= 0) {
        throw new StreamIdNotGreaterThanZeroError()
      }
      if (compareStreamId(next, stream.lastId) <= 0) {
        throw new StreamIdEqualOrSmallerError()
      }

      stream.entries.push({ id: next, fields: args.fields })
      stream.lastId = next
      return next
    })

    return bulk(Buffer.from(formatStreamId(id)))
  },
})

export const xlenCommand = defineCommand({
  name: 'xlen',
  schema: t.object({ key: t.key() }),
  flags: ['readonly', 'fast'],
  keys: args => [args.key],
  execute: (args, ctx) => {
    const stream = ctx.db.getStream(args.key)
    return integer(stream?.entries.length ?? 0)
  },
})

function rangeReply(
  stream: RedisStreamData | null,
  startTok: string,
  endTok: string,
  count: number | null,
  reverse: boolean,
): RedisValue[] {
  if (!stream) return []

  const start = parseRangeId(startTok, true)
  const end = parseRangeId(endTok, false)

  const items: RedisValue[] = []
  for (const entry of stream.entries) {
    const afterStart = exclusiveAware(
      compareStreamId(entry.id, start.id),
      start.exclusive,
      true,
    )
    const beforeEnd = exclusiveAware(
      compareStreamId(entry.id, end.id),
      end.exclusive,
      false,
    )
    if (afterStart && beforeEnd) {
      items.push(entryToReply(entry.id, entry.fields))
    }
  }

  if (reverse) items.reverse()
  if (count !== null && count >= 0 && items.length > count) {
    items.length = count
  }
  return items
}

// cmp = compareStreamId(entry, bound). For the lower bound we need entry >= bound
// (or > when exclusive); for the upper bound entry <= bound (or < when exclusive).
function exclusiveAware(
  cmp: number,
  exclusive: boolean,
  isLowerBound: boolean,
): boolean {
  if (isLowerBound) {
    return exclusive ? cmp > 0 : cmp >= 0
  }
  return exclusive ? cmp < 0 : cmp <= 0
}

export const xrangeCommand = defineCommand({
  name: 'xrange',
  schema: t.object({
    key: t.key(),
    start: t.string(),
    end: t.string(),
    count: createCountSchema(),
  }),
  flags: ['readonly'],
  keys: args => [args.key],
  execute: (args, ctx) => {
    const stream = ctx.db.getStream(args.key)
    return array(rangeReply(stream, args.start, args.end, args.count, false))
  },
})

export const xrevrangeCommand = defineCommand({
  name: 'xrevrange',
  schema: t.object({
    key: t.key(),
    end: t.string(),
    start: t.string(),
    count: createCountSchema(),
  }),
  flags: ['readonly'],
  keys: args => [args.key],
  execute: (args, ctx) => {
    const stream = ctx.db.getStream(args.key)
    return array(rangeReply(stream, args.start, args.end, args.count, true))
  },
})

export const xdelCommand = defineCommand({
  name: 'xdel',
  schema: t.object({
    key: t.key(),
    ids: t.variadic(t.string(), { min: 1 }),
  }),
  flags: ['write', 'fast'],
  keys: args => [args.key],
  execute: (args, ctx) => {
    const targets = args.ids.map(parseExactId)
    // XDEL on a missing key returns 0 without creating the stream.
    if (ctx.db.getType(args.key) === null) {
      return integer(0)
    }

    const deleted = ctx.db.updateStream(args.key, stream => {
      let count = 0
      for (const target of targets) {
        const idx = stream.entries.findIndex(
          entry => compareStreamId(entry.id, target) === 0,
        )
        if (idx !== -1) {
          stream.entries.splice(idx, 1)
          count++
        }
      }
      return count
    })
    // Streams are not removed when they become empty, matching Redis.
    return integer(deleted)
  },
})

// Optional `COUNT <n>` tail for XRANGE/XREVRANGE. Returns null when absent.
function createCountSchema() {
  return t.custom<number | null>(
    (input: readonly Buffer[], index: number, ctx: ParseContext) => {
      if (index >= input.length) {
        return { value: null, nextIndex: index }
      }
      if (input[index].toString().toUpperCase() !== 'COUNT') {
        throw new WrongNumberOfArgumentsError(ctx.commandName)
      }
      const raw = input[index + 1]
      if (raw === undefined) {
        throw new WrongNumberOfArgumentsError(ctx.commandName)
      }
      const value = Number(raw.toString())
      if (!Number.isInteger(value)) {
        throw new InvalidStreamIdError()
      }
      return { value, nextIndex: index + 2 }
    },
  )
}

export const streamsCommands = [
  xaddCommand,
  xlenCommand,
  xrangeCommand,
  xrevrangeCommand,
  xdelCommand,
]
