import { defineCommand } from '../core/command-definition'
import {
  SchemaMismatchError,
  t,
  type ParseContext,
} from '../core/command-schema'
import {
  ExpectedIntegerError,
  InvalidStreamIdError,
  NoSuchKeyError,
  RedisCommandError,
  RedisSyntaxError,
  StreamLimitNegativeError,
  StreamLimitRequiresApproxError,
  StreamElementTooLargeError,
  StreamIdExhaustedError,
  StreamIdEqualOrSmallerError,
  StreamIdNotGreaterThanZeroError,
  WrongNumberOfArgumentsError,
} from '../core/redis-error'
import { RedisValue } from '../core/redis-value'
import { RedisResult } from '../core/redis-result'
import type { RedisExecutionContext } from '../core/redis-context'
import type {
  RedisStreamConsumer,
  RedisStreamConsumerGroup,
  RedisStreamData,
  RedisStreamEntry,
  RedisStreamPendingEntry,
  StreamId,
} from '../state/data-types'
import { array, bulk, integer, ok } from './helpers'

const MAX_UINT64 = (1n << 64n) - 1n
const MIN_ID: StreamId = { ms: 0n, seq: 0n }
const MAX_ID: StreamId = { ms: MAX_UINT64, seq: MAX_UINT64 }

function formatStreamId(id: StreamId): string {
  return `${id.ms}-${id.seq}`
}

function streamIdKey(id: StreamId): string {
  return formatStreamId(id)
}

function bufferId(value: Buffer): string {
  return value.toString('hex')
}

function compareStreamId(a: StreamId, b: StreamId): number {
  if (a.ms !== b.ms) return a.ms < b.ms ? -1 : 1
  if (a.seq !== b.seq) return a.seq < b.seq ? -1 : 1
  return 0
}

function cloneStreamId(id: StreamId): StreamId {
  return { ms: id.ms, seq: id.seq }
}

function maxStreamId(a: StreamId, b: StreamId): StreamId {
  return compareStreamId(a, b) >= 0 ? a : b
}

function updateMaxDeletedId(stream: RedisStreamData, id: StreamId): void {
  stream.maxDeletedEntryId = cloneStreamId(
    maxStreamId(stream.maxDeletedEntryId, id),
  )
}

function findEntry(
  stream: RedisStreamData,
  id: StreamId,
): RedisStreamEntry | null {
  return (
    stream.entries.find(entry => compareStreamId(entry.id, id) === 0) ?? null
  )
}

function ensureConsumer(
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

function pendingEntriesSorted(
  group: RedisStreamConsumerGroup,
): RedisStreamPendingEntry[] {
  return Array.from(group.pending.values()).sort((a, b) =>
    compareStreamId(a.id, b.id),
  )
}

function streamGroup(
  stream: RedisStreamData,
  groupName: Buffer,
): RedisStreamConsumerGroup | null {
  return stream.groups.get(bufferId(groupName)) ?? null
}

function requireStreamGroup(
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

function streamLag(
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

function consumerPendingCount(
  group: RedisStreamConsumerGroup,
  consumerId: string,
): number {
  let count = 0
  for (const pending of group.pending.values()) {
    if (pending.consumerId === consumerId) count++
  }
  return count
}

function bulkString(value: string | Buffer): RedisValue {
  return RedisValue.bulkString(
    typeof value === 'string' ? Buffer.from(value) : value,
  )
}

function nullBulk(): RedisValue {
  return RedisValue.bulkString(null)
}

function integerValue(value: number | bigint): RedisValue {
  return RedisValue.integer(value)
}

function streamIdValue(id: StreamId): RedisValue {
  return bulkString(formatStreamId(id))
}

class BusyStreamGroupError extends RedisCommandError {
  constructor() {
    super('Consumer Group name already exists', 'BUSYGROUP')
  }
}

class NoSuchStreamGroupError extends RedisCommandError {
  constructor(key: Buffer, group: Buffer, commandName?: string) {
    const suffix =
      commandName === 'XREADGROUP' ? ' in XREADGROUP with GROUP option' : ''
    super(
      `No such key '${key.toString()}' or consumer group '${group.toString()}'${suffix}`,
      'NOGROUP',
    )
  }
}

class XgroupCreateMissingKeyError extends RedisCommandError {
  constructor() {
    super(
      'The XGROUP subcommand requires the key to exist. Note that for CREATE you may want to use the MKSTREAM option to create an empty stream automatically.',
    )
  }
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
  if (lastId.seq < MAX_UINT64) return { ms: lastId.ms, seq: lastId.seq + 1n }
  if (lastId.ms < MAX_UINT64) return { ms: lastId.ms + 1n, seq: 0n }

  throw new StreamIdExhaustedError()
}

function nextSeqForMs(ms: bigint, lastId: StreamId): StreamId {
  if (ms > lastId.ms) return { ms, seq: 0n }
  if (ms < lastId.ms) {
    return { ms, seq: lastId.seq === MAX_UINT64 ? MAX_UINT64 : lastId.seq + 1n }
  }
  if (lastId.seq === MAX_UINT64) {
    if (ms === MAX_UINT64) throw new StreamIdExhaustedError()
    throw new StreamElementTooLargeError()
  }

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

function deletedEntryToReply(id: StreamId): RedisValue {
  return RedisValue.array([
    RedisValue.bulkString(Buffer.from(formatStreamId(id))),
    RedisValue.bulkString(null),
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
  | {
      strategy: 'maxlen'
      count: bigint
      approximate: boolean
      limit: bigint | null
    }
  | {
      strategy: 'minid'
      minId: StreamId
      approximate: boolean
      limit: bigint | null
    }

function parseTrimLimit(raw: string): bigint {
  if (/^-\d+$/.test(raw)) {
    throw new StreamLimitNegativeError()
  }

  const value = parseUint64(raw)
  if (value === null) {
    throw new ExpectedIntegerError()
  }

  return value
}

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

      let trim: TrimSpec
      if (keyword === 'MAXLEN') {
        const count = parseUint64(rawValue)
        if (count === null) throw new RedisSyntaxError()
        trim = { strategy: 'maxlen', count, approximate, limit: null }
      } else {
        const minId = parseExactId(rawValue)
        trim = { strategy: 'minid', minId, approximate, limit: null }
      }

      while (cursor < input.length) {
        if (input[cursor]!.toString().toUpperCase() !== 'LIMIT') {
          break
        }

        if (!approximate) {
          throw new StreamLimitRequiresApproxError()
        }

        const rawLimit = input[cursor + 1]?.toString()
        if (rawLimit === undefined) {
          throw new RedisSyntaxError()
        }

        trim.limit = parseTrimLimit(rawLimit)
        cursor += 2
      }

      return { value: trim, nextIndex: cursor }
    },
  )
}

function applyTrim(stream: RedisStreamData, spec: TrimSpec): number {
  if (spec.strategy === 'maxlen') {
    const removeCount = stream.entries.length - Number(spec.count)
    if (removeCount <= 0) return 0
    for (const entry of stream.entries.slice(0, removeCount)) {
      updateMaxDeletedId(stream, entry.id)
    }
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
    for (const entry of stream.entries.slice(0, i)) {
      updateMaxDeletedId(stream, entry.id)
    }
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
      stream.entriesAdded++

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
          updateMaxDeletedId(stream, stream.entries[idx].id)
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
  const results: [RedisValue, RedisValue][] = []

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
      results.push([RedisValue.bulkString(key), RedisValue.array(entries)])
    }
  }

  return results.length > 0
    ? RedisResult.create(RedisValue.mapPairs(results))
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

    let woken: boolean | null
    try {
      woken = await ctx.park({
        waitFor,
        timeoutMs: remaining,
        signal: ctx.signal,
      })
    } finally {
      for (const unsub of unsubs) {
        try {
          unsub()
        } catch {
          // ignore errors from individual unsubscribers so all are attempted
        }
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
  flags: ['readonly'],
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

type XgroupArgs =
  | {
      subcommand: 'create'
      key: Buffer
      group: Buffer
      id: StreamId | '$'
      mkstream: boolean
      entriesRead: number | null
    }
  | {
      subcommand: 'setid'
      key: Buffer
      group: Buffer
      id: StreamId | '$'
      entriesRead: number | null
    }
  | { subcommand: 'destroy'; key: Buffer; group: Buffer }
  | {
      subcommand: 'createconsumer'
      key: Buffer
      group: Buffer
      consumer: Buffer
    }
  | { subcommand: 'delconsumer'; key: Buffer; group: Buffer; consumer: Buffer }

function createXgroupSchema() {
  return t.custom<XgroupArgs>(
    (input: readonly Buffer[], index: number, ctx: ParseContext) => {
      const subcommand = input[index]?.toString().toUpperCase()
      if (!subcommand) throw new WrongNumberOfArgumentsError(ctx.commandName)

      if (subcommand === 'CREATE' || subcommand === 'SETID') {
        const key = input[index + 1]
        const group = input[index + 2]
        const rawId = input[index + 3]?.toString()
        if (!key || !group || rawId === undefined) {
          throw new WrongNumberOfArgumentsError(ctx.commandName)
        }

        let cursor = index + 4
        let mkstream = false
        let entriesRead: number | null = null
        while (cursor < input.length) {
          const option = input[cursor].toString().toUpperCase()
          if (subcommand === 'CREATE' && option === 'MKSTREAM') {
            mkstream = true
            cursor++
            continue
          }

          if (option === 'ENTRIESREAD') {
            const rawEntriesRead = input[cursor + 1]
            if (!rawEntriesRead) {
              throw new WrongNumberOfArgumentsError(ctx.commandName)
            }
            entriesRead = parseNonNegativeInteger(rawEntriesRead)
            cursor += 2
            continue
          }

          throw new RedisSyntaxError()
        }

        return {
          value: {
            subcommand: subcommand === 'CREATE' ? 'create' : 'setid',
            key,
            group,
            id: rawId === '$' ? '$' : parseExactId(rawId),
            mkstream,
            entriesRead,
          },
          nextIndex: input.length,
        }
      }

      if (subcommand === 'DESTROY') {
        const key = input[index + 1]
        const group = input[index + 2]
        if (!key || !group || input.length !== index + 3) {
          throw new WrongNumberOfArgumentsError(ctx.commandName)
        }
        return {
          value: { subcommand: 'destroy', key, group },
          nextIndex: input.length,
        }
      }

      if (subcommand === 'CREATECONSUMER' || subcommand === 'DELCONSUMER') {
        const key = input[index + 1]
        const group = input[index + 2]
        const consumer = input[index + 3]
        if (!key || !group || !consumer || input.length !== index + 4) {
          throw new WrongNumberOfArgumentsError(ctx.commandName)
        }
        return {
          value: {
            subcommand:
              subcommand === 'CREATECONSUMER'
                ? 'createconsumer'
                : 'delconsumer',
            key,
            group,
            consumer,
          },
          nextIndex: input.length,
        }
      }

      throw new RedisCommandError(
        `Unknown subcommand or wrong number of arguments for '${subcommand}'. Try XGROUP HELP.`,
      )
    },
  )
}

export const xgroupCommand = defineCommand({
  name: 'xgroup',
  schema: t.object({ args: createXgroupSchema() }),
  flags: ['write'],
  keys: args => [args.args.key],
  execute: (args, ctx) => {
    const command = args.args

    if (command.subcommand === 'create') {
      const type = ctx.db.getType(command.key)
      if (type === null && !command.mkstream) {
        throw new XgroupCreateMissingKeyError()
      }

      const lastDeliveredId =
        command.id === '$'
          ? (ctx.db.getStream(command.key)?.lastId ?? MIN_ID)
          : command.id

      ctx.db.updateStream(command.key, stream => {
        const groupId = bufferId(command.group)
        if (stream.groups.has(groupId)) {
          throw new BusyStreamGroupError()
        }

        stream.groups.set(groupId, {
          name: Buffer.from(command.group),
          lastDeliveredId: cloneStreamId(lastDeliveredId),
          entriesRead: command.entriesRead,
          consumers: new Map(),
          pending: new Map(),
        })
      })
      return ok()
    }

    if (command.subcommand === 'setid') {
      requireStreamGroup(
        ctx.db.getStream(command.key),
        command.key,
        command.group,
      )
      ctx.db.updateStream(command.key, stream => {
        const group = requireStreamGroup(stream, command.key, command.group)
        group.lastDeliveredId =
          command.id === '$'
            ? cloneStreamId(stream.lastId)
            : cloneStreamId(command.id)
        group.entriesRead = command.entriesRead
      })
      return ok()
    }

    if (command.subcommand === 'destroy') {
      const stream = ctx.db.getStream(command.key)
      if (!stream) return integer(0)

      const removed = ctx.db.updateStream(command.key, writable =>
        writable.groups.delete(bufferId(command.group)),
      )
      return integer(removed ? 1 : 0)
    }

    if (command.subcommand === 'createconsumer') {
      requireStreamGroup(
        ctx.db.getStream(command.key),
        command.key,
        command.group,
      )
      const created = ctx.db.updateStream(command.key, stream => {
        const group = requireStreamGroup(stream, command.key, command.group)
        const consumerId = bufferId(command.consumer)
        if (group.consumers.has(consumerId)) return false

        group.consumers.set(consumerId, {
          name: Buffer.from(command.consumer),
          seenAt: Date.now(),
          activeAt: null,
        })
        return true
      })
      return integer(created ? 1 : 0)
    }

    requireStreamGroup(
      ctx.db.getStream(command.key),
      command.key,
      command.group,
    )
    const deleted = ctx.db.updateStream(command.key, stream => {
      const group = requireStreamGroup(stream, command.key, command.group)
      const consumerId = bufferId(command.consumer)
      if (!group.consumers.delete(consumerId)) return 0

      let removedPending = 0
      for (const [pendingId, pending] of Array.from(group.pending)) {
        if (pending.consumerId !== consumerId) continue
        group.pending.delete(pendingId)
        removedPending++
      }
      return removedPending
    })
    return integer(deleted)
  },
})

type XreadGroupStream = { key: Buffer; id: StreamId | '>' }

function createXreadGroupSchema() {
  return t.custom<{
    group: Buffer
    consumer: Buffer
    count: number | null
    blockMs: number | null
    noack: boolean
    streams: XreadGroupStream[]
  }>((input: readonly Buffer[], index: number, ctx: ParseContext) => {
    if (input[index]?.toString().toUpperCase() !== 'GROUP') {
      throw new WrongNumberOfArgumentsError(ctx.commandName)
    }

    const group = input[index + 1]
    const consumer = input[index + 2]
    if (!group || !consumer) {
      throw new WrongNumberOfArgumentsError(ctx.commandName)
    }

    let cursor = index + 3
    let count: number | null = null
    let blockMs: number | null = null
    let noack = false

    while (cursor < input.length) {
      const token = input[cursor].toString().toUpperCase()
      if (token === 'COUNT') {
        const raw = input[cursor + 1]
        if (!raw) throw new WrongNumberOfArgumentsError(ctx.commandName)
        count = parseNonNegativeInteger(raw)
        cursor += 2
        continue
      }

      if (token === 'BLOCK') {
        const raw = input[cursor + 1]
        if (!raw) throw new WrongNumberOfArgumentsError(ctx.commandName)
        blockMs = parseNonNegativeInteger(raw)
        cursor += 2
        continue
      }

      if (token === 'NOACK') {
        noack = true
        cursor++
        continue
      }

      break
    }

    if (
      cursor >= input.length ||
      input[cursor].toString().toUpperCase() !== 'STREAMS'
    ) {
      throw new WrongNumberOfArgumentsError(ctx.commandName)
    }
    cursor++

    const remaining = input.length - cursor
    if (remaining === 0 || remaining % 2 !== 0) {
      throw new WrongNumberOfArgumentsError(ctx.commandName)
    }

    const half = remaining / 2
    const streams: XreadGroupStream[] = []
    for (let i = 0; i < half; i++) {
      const key = input[cursor + i]
      const rawId = input[cursor + half + i].toString()
      streams.push({
        key,
        id: rawId === '>' ? '>' : parseExactId(rawId),
      })
    }

    return {
      value: { group, consumer, count, blockMs, noack, streams },
      nextIndex: input.length,
    }
  })
}

function readGroupEntries(
  groupName: Buffer,
  consumerName: Buffer,
  streams: XreadGroupStream[],
  count: number | null,
  noack: boolean,
  ctx: RedisExecutionContext,
): RedisResult | null {
  const now = Date.now()
  const results: [RedisValue, RedisValue][] = []

  for (const { key } of streams) {
    requireStreamGroup(ctx.db.getStream(key), key, groupName, 'XREADGROUP')
  }

  for (const { key, id } of streams) {
    const entries = ctx.db.updateStream(key, stream => {
      const group = requireStreamGroup(stream, key, groupName, 'XREADGROUP')
      const consumer = ensureConsumer(group, consumerName, now)
      consumer.activeAt = now

      const consumerId = bufferId(consumerName)
      const replies: RedisValue[] = []

      if (id === '>') {
        for (const entry of stream.entries) {
          if (compareStreamId(entry.id, group.lastDeliveredId) <= 0) continue

          replies.push(entryToReply(entry.id, entry.fields))
          group.lastDeliveredId = cloneStreamId(entry.id)
          group.entriesRead = (group.entriesRead ?? 0) + 1

          if (!noack) {
            group.pending.set(streamIdKey(entry.id), {
              id: cloneStreamId(entry.id),
              consumerId,
              deliveredAt: now,
              deliveryCount: 1,
            })
          }

          if (count !== null && count > 0 && replies.length >= count) break
        }
      } else {
        for (const pending of pendingEntriesSorted(group)) {
          if (pending.consumerId !== consumerId) continue
          if (compareStreamId(pending.id, id) <= 0) continue

          const entry = findEntry(stream, pending.id)
          replies.push(
            entry
              ? entryToReply(entry.id, entry.fields)
              : deletedEntryToReply(pending.id),
          )

          if (count !== null && count > 0 && replies.length >= count) break
        }
      }

      return replies
    })

    if (entries.length > 0 || id !== '>') {
      results.push([bulkString(key), RedisValue.array(entries)])
    }
  }

  return results.length > 0
    ? RedisResult.create(RedisValue.mapPairs(results))
    : null
}

async function blockingXreadGroup(
  groupName: Buffer,
  consumerName: Buffer,
  streams: XreadGroupStream[],
  count: number | null,
  noack: boolean,
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

    let woken: boolean | null
    try {
      woken = await ctx.park({
        waitFor,
        timeoutMs: remaining,
        signal: ctx.signal,
      })
    } finally {
      for (const unsub of unsubs) {
        try {
          unsub()
        } catch {
          // ignore errors from individual unsubscribers so all are attempted
        }
      }
    }

    if (woken === null) return bulk(null)

    const result = readGroupEntries(
      groupName,
      consumerName,
      streams,
      count,
      noack,
      ctx,
    )
    if (result) return result
  }
}

export const xreadgroupCommand = defineCommand({
  name: 'xreadgroup',
  schema: t.object({ args: createXreadGroupSchema() }),
  flags: ['write', 'blocking'],
  capabilities: { blocking: true },
  keys: args => args.args.streams.map(s => s.key),
  execute: (args, ctx) => {
    const { group, consumer, streams, count, blockMs, noack } = args.args
    const immediate = readGroupEntries(
      group,
      consumer,
      streams,
      count,
      noack,
      ctx,
    )
    if (
      immediate ||
      blockMs === null ||
      streams.some(stream => stream.id !== '>')
    ) {
      return immediate ?? bulk(null)
    }

    return blockingXreadGroup(
      group,
      consumer,
      streams,
      count,
      noack,
      blockMs,
      ctx,
    )
  },
})

export const xackCommand = defineCommand({
  name: 'xack',
  schema: t.object({
    key: t.key(),
    group: t.bulk(),
    ids: t.variadic(t.string(), { min: 1 }),
  }),
  flags: ['write', 'fast'],
  keys: args => [args.key],
  execute: (args, ctx) => {
    const ids = args.ids.map(parseExactId)
    requireStreamGroup(ctx.db.getStream(args.key), args.key, args.group)
    const acknowledged = ctx.db.updateStream(args.key, stream => {
      const group = requireStreamGroup(stream, args.key, args.group)
      let count = 0
      for (const id of ids) {
        if (group.pending.delete(streamIdKey(id))) count++
      }
      return count
    })
    return integer(acknowledged)
  },
})

type XpendingArgs =
  | { mode: 'summary'; key: Buffer; group: Buffer }
  | {
      mode: 'range'
      key: Buffer
      group: Buffer
      minIdleMs: number | null
      start: RangeBound
      end: RangeBound
      count: number
      consumer: Buffer | null
    }

function createXpendingSchema() {
  return t.custom<XpendingArgs>(
    (input: readonly Buffer[], index: number, ctx: ParseContext) => {
      const key = input[index]
      const group = input[index + 1]
      if (!key || !group) throw new WrongNumberOfArgumentsError(ctx.commandName)

      if (input.length === index + 2) {
        return {
          value: { mode: 'summary', key, group },
          nextIndex: input.length,
        }
      }

      let cursor = index + 2
      let minIdleMs: number | null = null
      if (input[cursor]?.toString().toUpperCase() === 'IDLE') {
        const raw = input[cursor + 1]
        if (!raw) throw new WrongNumberOfArgumentsError(ctx.commandName)
        minIdleMs = parseNonNegativeInteger(raw)
        cursor += 2
      }

      const startToken = input[cursor]?.toString()
      const endToken = input[cursor + 1]?.toString()
      const rawCount = input[cursor + 2]
      if (startToken === undefined || endToken === undefined || !rawCount) {
        throw new WrongNumberOfArgumentsError(ctx.commandName)
      }
      cursor += 3

      const consumer = input[cursor] ?? null
      if (cursor + (consumer ? 1 : 0) !== input.length) {
        throw new WrongNumberOfArgumentsError(ctx.commandName)
      }

      return {
        value: {
          mode: 'range',
          key,
          group,
          minIdleMs,
          start: parseRangeId(startToken, true),
          end: parseRangeId(endToken, false),
          count: parseNonNegativeInteger(rawCount),
          consumer,
        },
        nextIndex: input.length,
      }
    },
  )
}

export const xpendingCommand = defineCommand({
  name: 'xpending',
  schema: t.object({ args: createXpendingSchema() }),
  flags: ['readonly'],
  keys: args => [args.args.key],
  execute: (args, ctx) => {
    const command = args.args
    const stream = ctx.db.getStream(command.key)
    const group = requireStreamGroup(stream, command.key, command.group)

    if (command.mode === 'summary') {
      const pending = pendingEntriesSorted(group)
      if (pending.length === 0) {
        return array([
          integerValue(0),
          nullBulk(),
          nullBulk(),
          RedisValue.array([]),
        ])
      }

      const counts = new Map<string, number>()
      for (const entry of pending) {
        counts.set(entry.consumerId, (counts.get(entry.consumerId) ?? 0) + 1)
      }

      return array([
        integerValue(pending.length),
        streamIdValue(pending[0].id),
        streamIdValue(pending[pending.length - 1].id),
        RedisValue.array(
          Array.from(counts, ([consumerId, count]) => {
            const consumer = group.consumers.get(consumerId)
            return RedisValue.array([
              bulkString(consumer?.name ?? Buffer.from(consumerId, 'hex')),
              integerValue(count),
            ])
          }),
        ),
      ])
    }

    const now = Date.now()
    const consumerId = command.consumer ? bufferId(command.consumer) : null
    const replies: RedisValue[] = []
    for (const pending of pendingEntriesSorted(group)) {
      if (
        !exclusiveAware(
          compareStreamId(pending.id, command.start.id),
          command.start.exclusive,
          true,
        )
      ) {
        continue
      }
      if (
        !exclusiveAware(
          compareStreamId(pending.id, command.end.id),
          command.end.exclusive,
          false,
        )
      ) {
        continue
      }
      if (consumerId !== null && pending.consumerId !== consumerId) continue

      const idleMs = Math.max(0, now - pending.deliveredAt)
      if (command.minIdleMs !== null && idleMs < command.minIdleMs) continue

      const consumer = group.consumers.get(pending.consumerId)
      replies.push(
        RedisValue.array([
          streamIdValue(pending.id),
          bulkString(consumer?.name ?? Buffer.from(pending.consumerId, 'hex')),
          integerValue(idleMs),
          integerValue(pending.deliveryCount),
        ]),
      )
      if (replies.length >= command.count) break
    }

    return array(replies)
  },
})

type XclaimArgs = {
  key: Buffer
  group: Buffer
  consumer: Buffer
  minIdleMs: number
  ids: StreamId[]
  idleMs: number | null
  timeMs: number | null
  retryCount: number | null
  force: boolean
  justId: boolean
  lastId: StreamId | null
}

function createXclaimSchema() {
  return t.custom<XclaimArgs>(
    (input: readonly Buffer[], index: number, ctx: ParseContext) => {
      const key = input[index]
      const group = input[index + 1]
      const consumer = input[index + 2]
      const rawMinIdle = input[index + 3]
      if (!key || !group || !consumer || !rawMinIdle) {
        throw new WrongNumberOfArgumentsError(ctx.commandName)
      }

      let cursor = index + 4
      const ids: StreamId[] = []
      while (cursor < input.length && !isXclaimOption(input[cursor])) {
        ids.push(parseExactId(input[cursor].toString()))
        cursor++
      }
      if (ids.length === 0)
        throw new WrongNumberOfArgumentsError(ctx.commandName)

      let idleMs: number | null = null
      let timeMs: number | null = null
      let retryCount: number | null = null
      let force = false
      let justId = false
      let lastId: StreamId | null = null

      while (cursor < input.length) {
        const option = input[cursor].toString().toUpperCase()
        if (option === 'IDLE' || option === 'TIME' || option === 'RETRYCOUNT') {
          const rawValue = input[cursor + 1]
          if (!rawValue) throw new WrongNumberOfArgumentsError(ctx.commandName)
          const value = parseNonNegativeInteger(rawValue)
          if (option === 'IDLE') idleMs = value
          if (option === 'TIME') timeMs = value
          if (option === 'RETRYCOUNT') retryCount = value
          cursor += 2
          continue
        }

        if (option === 'FORCE') {
          force = true
          cursor++
          continue
        }

        if (option === 'JUSTID') {
          justId = true
          cursor++
          continue
        }

        if (option === 'LASTID') {
          const rawValue = input[cursor + 1]
          if (!rawValue) throw new WrongNumberOfArgumentsError(ctx.commandName)
          lastId = parseExactId(rawValue.toString())
          cursor += 2
          continue
        }

        throw new RedisSyntaxError()
      }

      return {
        value: {
          key,
          group,
          consumer,
          minIdleMs: parseNonNegativeInteger(rawMinIdle),
          ids,
          idleMs,
          timeMs,
          retryCount,
          force,
          justId,
          lastId,
        },
        nextIndex: input.length,
      }
    },
  )
}

function isXclaimOption(token: Buffer): boolean {
  const option = token.toString().toUpperCase()
  return (
    option === 'IDLE' ||
    option === 'TIME' ||
    option === 'RETRYCOUNT' ||
    option === 'FORCE' ||
    option === 'JUSTID' ||
    option === 'LASTID'
  )
}

export const xclaimCommand = defineCommand({
  name: 'xclaim',
  schema: t.object({ args: createXclaimSchema() }),
  flags: ['write'],
  keys: args => [args.args.key],
  execute: (args, ctx) => {
    const command = args.args
    const now = Date.now()
    requireStreamGroup(
      ctx.db.getStream(command.key),
      command.key,
      command.group,
    )
    const claimed = ctx.db.updateStream(command.key, stream => {
      const group = requireStreamGroup(stream, command.key, command.group)
      ensureConsumer(group, command.consumer, now).activeAt = now
      const consumerId = bufferId(command.consumer)
      if (command.lastId) group.lastDeliveredId = cloneStreamId(command.lastId)

      const replies: RedisValue[] = []
      for (const id of command.ids) {
        const pendingId = streamIdKey(id)
        const entry = findEntry(stream, id)
        let pending = group.pending.get(pendingId)

        if (!pending && command.force && entry) {
          pending = {
            id: cloneStreamId(id),
            consumerId,
            deliveredAt: now,
            deliveryCount: 0,
          }
          group.pending.set(pendingId, pending)
        }

        if (!pending) continue
        if (!entry) {
          group.pending.delete(pendingId)
          continue
        }

        const idleTime = Math.max(0, now - pending.deliveredAt)
        if (idleTime < command.minIdleMs) continue

        pending.consumerId = consumerId
        pending.deliveredAt =
          command.timeMs ??
          (command.idleMs !== null ? now - command.idleMs : now)
        if (command.retryCount !== null) {
          pending.deliveryCount = command.retryCount
        } else if (!command.justId) {
          pending.deliveryCount++
        }

        replies.push(
          command.justId
            ? streamIdValue(entry.id)
            : entryToReply(entry.id, entry.fields),
        )
      }
      return replies
    })

    return array(claimed)
  },
})

type XautoclaimArgs = {
  key: Buffer
  group: Buffer
  consumer: Buffer
  minIdleMs: number
  start: StreamId
  count: number
  justId: boolean
}

function createXautoclaimSchema() {
  return t.custom<XautoclaimArgs>(
    (input: readonly Buffer[], index: number, ctx: ParseContext) => {
      const key = input[index]
      const group = input[index + 1]
      const consumer = input[index + 2]
      const rawMinIdle = input[index + 3]
      const rawStart = input[index + 4]
      if (!key || !group || !consumer || !rawMinIdle || !rawStart) {
        throw new WrongNumberOfArgumentsError(ctx.commandName)
      }

      let cursor = index + 5
      let count = 100
      let justId = false
      while (cursor < input.length) {
        const option = input[cursor].toString().toUpperCase()
        if (option === 'COUNT') {
          const rawCount = input[cursor + 1]
          if (!rawCount) throw new WrongNumberOfArgumentsError(ctx.commandName)
          count = parseNonNegativeInteger(rawCount)
          cursor += 2
          continue
        }

        if (option === 'JUSTID') {
          justId = true
          cursor++
          continue
        }

        throw new RedisSyntaxError()
      }

      return {
        value: {
          key,
          group,
          consumer,
          minIdleMs: parseNonNegativeInteger(rawMinIdle),
          start: parseExactId(rawStart.toString()),
          count,
          justId,
        },
        nextIndex: input.length,
      }
    },
  )
}

export const xautoclaimCommand = defineCommand({
  name: 'xautoclaim',
  schema: t.object({ args: createXautoclaimSchema() }),
  flags: ['write'],
  keys: args => [args.args.key],
  execute: (args, ctx) => {
    const command = args.args
    const now = Date.now()
    requireStreamGroup(
      ctx.db.getStream(command.key),
      command.key,
      command.group,
    )
    const result = ctx.db.updateStream(command.key, stream => {
      const group = requireStreamGroup(stream, command.key, command.group)
      ensureConsumer(group, command.consumer, now).activeAt = now
      const consumerId = bufferId(command.consumer)
      const claimed: RedisValue[] = []
      const deleted: RedisValue[] = []
      let nextStartId: StreamId = MIN_ID

      for (const pending of pendingEntriesSorted(group)) {
        if (compareStreamId(pending.id, command.start) < 0) continue

        const entry = findEntry(stream, pending.id)
        if (!entry) {
          group.pending.delete(streamIdKey(pending.id))
          deleted.push(streamIdValue(pending.id))
          continue
        }

        const idleTime = Math.max(0, now - pending.deliveredAt)
        if (idleTime < command.minIdleMs) continue

        pending.consumerId = consumerId
        pending.deliveredAt = now
        if (!command.justId) pending.deliveryCount++
        claimed.push(
          command.justId
            ? streamIdValue(entry.id)
            : entryToReply(entry.id, entry.fields),
        )

        if (claimed.length >= command.count) {
          const next = pendingEntriesSorted(group).find(
            item => compareStreamId(item.id, pending.id) > 0,
          )
          nextStartId = next ? cloneStreamId(next.id) : MIN_ID
          break
        }
      }

      return { nextStartId, claimed, deleted }
    })

    return array([
      streamIdValue(result.nextStartId),
      RedisValue.array(result.claimed),
      RedisValue.array(result.deleted),
    ])
  },
})

type XinfoArgs =
  | { subcommand: 'stream'; key: Buffer; full: boolean; count: number | null }
  | { subcommand: 'groups'; key: Buffer }
  | { subcommand: 'consumers'; key: Buffer; group: Buffer }

function createXinfoSchema() {
  return t.custom<XinfoArgs>(
    (input: readonly Buffer[], index: number, ctx: ParseContext) => {
      const subcommand = input[index]?.toString().toUpperCase()
      if (!subcommand) throw new WrongNumberOfArgumentsError(ctx.commandName)

      if (subcommand === 'STREAM') {
        const key = input[index + 1]
        if (!key) throw new WrongNumberOfArgumentsError(ctx.commandName)
        let cursor = index + 2
        let full = false
        let count: number | null = null

        if (cursor < input.length) {
          if (input[cursor].toString().toUpperCase() !== 'FULL') {
            throw new RedisSyntaxError()
          }
          full = true
          cursor++
        }

        if (cursor < input.length) {
          if (input[cursor].toString().toUpperCase() !== 'COUNT') {
            throw new RedisSyntaxError()
          }
          const rawCount = input[cursor + 1]
          if (!rawCount) throw new WrongNumberOfArgumentsError(ctx.commandName)
          count = parseNonNegativeInteger(rawCount)
          cursor += 2
        }

        if (cursor !== input.length) {
          throw new WrongNumberOfArgumentsError(ctx.commandName)
        }

        return {
          value: { subcommand: 'stream', key, full, count },
          nextIndex: input.length,
        }
      }

      if (subcommand === 'GROUPS') {
        const key = input[index + 1]
        if (!key || input.length !== index + 2) {
          throw new WrongNumberOfArgumentsError(ctx.commandName)
        }
        return {
          value: { subcommand: 'groups', key },
          nextIndex: input.length,
        }
      }

      if (subcommand === 'CONSUMERS') {
        const key = input[index + 1]
        const group = input[index + 2]
        if (!key || !group || input.length !== index + 3) {
          throw new WrongNumberOfArgumentsError(ctx.commandName)
        }
        return {
          value: { subcommand: 'consumers', key, group },
          nextIndex: input.length,
        }
      }

      throw new RedisCommandError(
        `unknown subcommand '${subcommand}'. Try XINFO HELP.`,
      )
    },
  )
}

export const xinfoCommand = defineCommand({
  name: 'xinfo',
  schema: t.object({ args: createXinfoSchema() }),
  flags: ['readonly'],
  keys: args => [args.args.key],
  execute: (args, ctx) => {
    const command = args.args
    const stream = ctx.db.getStream(command.key)
    if (!stream) throw new NoSuchKeyError()

    if (command.subcommand === 'stream') {
      return array(streamInfoReply(stream, command.full, command.count))
    }

    if (command.subcommand === 'groups') {
      return array(Array.from(stream.groups.values(), groupInfoReply(stream)))
    }

    const group = requireStreamGroup(stream, command.key, command.group)
    const now = Date.now()
    return array(
      Array.from(group.consumers.entries()).map(([consumerId, consumer]) =>
        consumerInfoReply(group, consumerId, consumer, now),
      ),
    )
  },
})

function streamInfoReply(
  stream: RedisStreamData,
  full: boolean,
  count: number | null,
): RedisValue[] {
  const firstEntry = stream.entries[0] ?? null
  const lastEntry = stream.entries[stream.entries.length - 1] ?? null

  const fields: RedisValue[] = [
    bulkString('length'),
    integerValue(stream.entries.length),
    bulkString('radix-tree-keys'),
    integerValue(stream.entries.length > 0 ? 1 : 0),
    bulkString('radix-tree-nodes'),
    integerValue(stream.entries.length > 0 ? 2 : 1),
    bulkString('last-generated-id'),
    streamIdValue(stream.lastId),
    bulkString('max-deleted-entry-id'),
    streamIdValue(stream.maxDeletedEntryId),
    bulkString('entries-added'),
    integerValue(stream.entriesAdded),
    bulkString('recorded-first-entry-id'),
    firstEntry ? streamIdValue(firstEntry.id) : bulkString('0-0'),
    bulkString('groups'),
  ]

  if (!full) {
    fields.push(
      integerValue(stream.groups.size),
      bulkString('first-entry'),
      firstEntry ? entryToReply(firstEntry.id, firstEntry.fields) : nullBulk(),
      bulkString('last-entry'),
      lastEntry ? entryToReply(lastEntry.id, lastEntry.fields) : nullBulk(),
    )
    return fields
  }

  const fullCount = count ?? 10
  fields.push(
    RedisValue.array(
      Array.from(stream.groups.values(), group =>
        fullGroupInfoReply(stream, group, fullCount),
      ),
    ),
    bulkString('entries'),
    RedisValue.array(
      stream.entries
        .slice(0, fullCount)
        .map(entry => entryToReply(entry.id, entry.fields)),
    ),
  )
  return fields
}

function groupInfoReply(stream: RedisStreamData) {
  return (group: RedisStreamConsumerGroup): RedisValue =>
    RedisValue.array([
      bulkString('name'),
      bulkString(group.name),
      bulkString('consumers'),
      integerValue(group.consumers.size),
      bulkString('pending'),
      integerValue(group.pending.size),
      bulkString('last-delivered-id'),
      streamIdValue(group.lastDeliveredId),
      bulkString('entries-read'),
      group.entriesRead === null ? nullBulk() : integerValue(group.entriesRead),
      bulkString('lag'),
      integerValue(streamLag(stream, group)),
    ])
}

function fullGroupInfoReply(
  stream: RedisStreamData,
  group: RedisStreamConsumerGroup,
  count: number,
): RedisValue {
  return RedisValue.array([
    bulkString('name'),
    bulkString(group.name),
    bulkString('last-delivered-id'),
    streamIdValue(group.lastDeliveredId),
    bulkString('entries-read'),
    group.entriesRead === null ? nullBulk() : integerValue(group.entriesRead),
    bulkString('lag'),
    integerValue(streamLag(stream, group)),
    bulkString('pel-count'),
    integerValue(group.pending.size),
    bulkString('pending'),
    RedisValue.array(
      pendingEntriesSorted(group)
        .slice(0, count)
        .map(pending =>
          RedisValue.array([
            streamIdValue(pending.id),
            bulkString(
              group.consumers.get(pending.consumerId)?.name ??
                Buffer.from(pending.consumerId, 'hex'),
            ),
            integerValue(Math.max(0, Date.now() - pending.deliveredAt)),
            integerValue(pending.deliveryCount),
          ]),
        ),
    ),
    bulkString('consumers'),
    RedisValue.array(
      Array.from(group.consumers.entries()).map(([consumerId, consumer]) =>
        consumerInfoReply(group, consumerId, consumer, Date.now()),
      ),
    ),
  ])
}

function consumerInfoReply(
  group: RedisStreamConsumerGroup,
  consumerId: string,
  consumer: RedisStreamConsumer,
  now: number,
): RedisValue {
  const idle = Math.max(0, now - consumer.seenAt)
  const inactive =
    consumer.activeAt === null ? idle : Math.max(0, now - consumer.activeAt)
  return RedisValue.array([
    bulkString('name'),
    bulkString(consumer.name),
    bulkString('pending'),
    integerValue(consumerPendingCount(group, consumerId)),
    bulkString('idle'),
    integerValue(idle),
    bulkString('inactive'),
    integerValue(inactive),
  ])
}

function parseNonNegativeInteger(token: Buffer): number {
  const raw = token.toString()
  if (!/^\d+$/.test(raw)) {
    throw new ExpectedIntegerError()
  }

  const value = Number(raw)
  if (!Number.isSafeInteger(value)) {
    throw new ExpectedIntegerError()
  }

  return value
}

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
        throw new ExpectedIntegerError()
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
  xgroupCommand,
  xreadgroupCommand,
  xackCommand,
  xpendingCommand,
  xclaimCommand,
  xautoclaimCommand,
  xinfoCommand,
]
