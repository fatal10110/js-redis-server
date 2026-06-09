import { defineCommand } from '../core/command-definition'
import {
  SchemaMismatchError,
  t,
  type ParseContext,
} from '../core/command-schema'
import {
  InvalidStreamIdError,
  RedisSyntaxError,
  StreamIdEqualOrSmallerError,
  StreamIdNotGreaterThanZeroError,
  WrongNumberOfArgumentsError,
} from '../core/redis-error'
import { RedisValue } from '../core/redis-value'
import { RedisResult } from '../core/redis-result'
import type { RedisExecutionContext } from '../core/redis-context'
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

// Trim specification shared by XADD and XTRIM.
type TrimSpec =
  | { strategy: 'maxlen'; count: bigint; approximate: boolean }
  | { strategy: 'minid'; minId: StreamId; approximate: boolean }

function createTrimSpecSchema() {
  return t.custom<TrimSpec>(
    (input: readonly Buffer[], index: number, ctx: ParseContext) => {
      if (index >= input.length) throw new SchemaMismatchError()

      const keyword = input[index].toString().toUpperCase()
      if (keyword !== 'MAXLEN' && keyword !== 'MINID')
        throw new SchemaMismatchError()

      let cursor = index + 1
      let approximate = false
      if (cursor < input.length && input[cursor].toString() === '~') {
        approximate = true
        cursor++
      }

      const rawValue = input[cursor]?.toString()
      if (rawValue === undefined)
        throw new WrongNumberOfArgumentsError(ctx.commandName)
      cursor++

      if (keyword === 'MAXLEN') {
        const count = parseUint64(rawValue)
        if (count === null) throw new RedisSyntaxError()
        return {
          value: { strategy: 'maxlen', count, approximate },
          nextIndex: cursor,
        }
      } else {
        const minId = parseExactId(rawValue)
        return {
          value: { strategy: 'minid', minId, approximate },
          nextIndex: cursor,
        }
      }
    },
  )
}

function applyTrim(stream: RedisStreamData, spec: TrimSpec): number {
  if (spec.strategy === 'maxlen') {
    const removeCount = stream.entries.length - Number(spec.count)
    if (removeCount <= 0) return 0
    stream.entries.splice(0, removeCount)
    return removeCount
  } else {
    let i = 0
    while (
      i < stream.entries.length &&
      compareStreamId(stream.entries[i].id, spec.minId) < 0
    ) {
      i++
    }
    if (i === 0) return 0
    stream.entries.splice(0, i)
    return i
  }
}

export const xaddCommand = defineCommand({
  name: 'xadd',
  schema: t.object({
    key: t.key(),
    nomkstream: t.optional(t.keyword('NOMKSTREAM')),
    trim: t.optional(createTrimSpecSchema()),
    id: t.string(),
    fields: createStreamFieldsSchema(),
  }),
  flags: ['write', 'denyoom', 'fast'],
  keys: args => [args.key],
  execute: (args, ctx) => {
    if (args.nomkstream !== undefined && ctx.db.getType(args.key) === null) {
      return bulk(null)
    }

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

      if (args.trim !== undefined) {
        applyTrim(stream, args.trim)
      }

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

export const xtrimCommand = defineCommand({
  name: 'xtrim',
  schema: t.object({
    key: t.key(),
    trim: createTrimSpecSchema(),
  }),
  flags: ['write'],
  keys: args => [args.key],
  execute: (args, ctx) => {
    if (ctx.db.getType(args.key) === null) {
      return integer(0)
    }
    const removed = ctx.db.updateStream(args.key, stream =>
      applyTrim(stream, args.trim),
    )
    return integer(removed)
  },
})

// XREAD [COUNT count] STREAMS key [key ...] id [id ...]
// `$` means "start after the stream's current last id" (returns nothing on a non-blocking call).
type XreadStream = { key: Buffer; afterId: StreamId | '$' }

function createXreadSchema() {
  return t.custom<{
    count: number | null
    blockMs: number | null
    streams: XreadStream[]
  }>((input: readonly Buffer[], index: number, ctx: ParseContext) => {
    let cursor = index
    let count: number | null = null
    let blockMs: number | null = null

    // Optional COUNT <n> and BLOCK <ms> in any order
    while (cursor < input.length) {
      const token = input[cursor].toString().toUpperCase()
      if (token === 'COUNT') {
        cursor++
        const raw = input[cursor]?.toString()
        if (raw === undefined)
          throw new WrongNumberOfArgumentsError(ctx.commandName)
        const n = Number(raw)
        if (!Number.isInteger(n) || n < 0) throw new RedisSyntaxError()
        count = n
        cursor++
      } else if (token === 'BLOCK') {
        cursor++
        const raw = input[cursor]?.toString()
        if (raw === undefined)
          throw new WrongNumberOfArgumentsError(ctx.commandName)
        const ms = Number(raw)
        if (!Number.isInteger(ms) || ms < 0) throw new RedisSyntaxError()
        blockMs = ms
        cursor++
      } else {
        break
      }
    }

    // Required STREAMS keyword
    if (
      cursor >= input.length ||
      input[cursor].toString().toUpperCase() !== 'STREAMS'
    ) {
      throw new WrongNumberOfArgumentsError(ctx.commandName)
    }
    cursor++

    // Remaining tokens: first half = keys, second half = ids
    const remaining = input.length - cursor
    if (remaining === 0 || remaining % 2 !== 0) {
      throw new WrongNumberOfArgumentsError(ctx.commandName)
    }

    const half = remaining / 2
    const streams: XreadStream[] = []
    for (let i = 0; i < half; i++) {
      const key = input[cursor + i]
      const idTok = input[cursor + half + i].toString()
      const afterId: StreamId | '$' = idTok === '$' ? '$' : parseExactId(idTok)
      streams.push({ key, afterId })
    }

    return { value: { count, blockMs, streams }, nextIndex: input.length }
  })
}

type ResolvedXreadStream = { key: Buffer; afterId: StreamId }

function readStreamEntries(
  streams: ResolvedXreadStream[],
  count: number | null,
  ctx: RedisExecutionContext,
): RedisResult | null {
  const results: RedisValue[] = []

  for (const { key, afterId } of streams) {
    const stream = ctx.db.getStream(key)
    if (!stream) continue

    const entries: RedisValue[] = []
    for (const entry of stream.entries) {
      if (compareStreamId(entry.id, afterId) > 0) {
        entries.push(entryToReply(entry.id, entry.fields))
        if (count !== null && count > 0 && entries.length >= count) break
      }
    }

    if (entries.length > 0) {
      results.push(
        RedisValue.array([
          RedisValue.bulkString(key),
          RedisValue.array(entries),
        ]),
      )
    }
  }

  return results.length > 0
    ? RedisResult.create(RedisValue.array(results))
    : null
}

async function blockingXread(
  streams: ResolvedXreadStream[],
  count: number | null,
  blockMs: number,
  ctx: RedisExecutionContext,
): Promise<RedisResult> {
  const timeoutMs = blockMs === 0 ? undefined : blockMs
  const deadline = timeoutMs !== undefined ? Date.now() + timeoutMs : undefined
  const keys = streams.map(s => s.key)

  while (true) {
    const remaining =
      deadline !== undefined ? Math.max(0, deadline - Date.now()) : undefined
    if (remaining === 0) return bulk(null)

    let wake!: (v: true) => void
    const waitFor = new Promise<true>(resolve => {
      wake = () => resolve(true)
    })

    const unsubs = keys.map(key =>
      ctx.db.subscribeKey(key, event => {
        if (event.type === 'write') wake(true)
      }),
    )

    const woken = await ctx.park({
      waitFor,
      timeoutMs: remaining,
      signal: ctx.signal,
    })
    for (const unsub of unsubs) {
      try {
        unsub()
      } catch {
        // ignore errors from individual unsubscribers so all are attempted
      }
    }

    if (woken === null) return bulk(null)

    const result = readStreamEntries(streams, count, ctx)
    if (result) return result
  }
}

export const xreadCommand = defineCommand({
  name: 'xread',
  schema: t.object({ args: createXreadSchema() }),
  flags: ['readonly', 'noscript'],
  keys: args => args.args.streams.map(s => s.key),
  execute: (args, ctx) => {
    const { count, blockMs, streams } = args.args

    // Resolve '$' to the stream's current last ID before any blocking,
    // so entries added after this call (not before) are returned.
    const resolved: ResolvedXreadStream[] = streams.map(s => ({
      key: s.key,
      afterId:
        s.afterId === '$'
          ? (ctx.db.getStream(s.key)?.lastId ?? MIN_ID)
          : s.afterId,
    }))

    const immediate = readStreamEntries(resolved, count, ctx)
    if (immediate || blockMs === null) return immediate ?? bulk(null)

    return blockingXread(resolved, count, blockMs, ctx)
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
  xtrimCommand,
  xreadCommand,
]
