import { defineCommand } from '../../core/command-definition'
import { t, type ParseContext } from '../../core/command-schema'
import {
  RedisSyntaxError,
  WrongNumberOfArgumentsError,
} from '../../core/redis-error'
import type { RedisExecutionContext } from '../../core/redis-context'
import { RedisResult } from '../../core/redis-result'
import { RedisValue } from '../../core/redis-value'
import type { StreamId } from '../../state/data-types'
import { bulk } from '../helpers'
import { compareStreamId, MIN_ID, parseExactId } from './ids'
import { entryToReply } from './replies'

// XREAD [COUNT count] STREAMS key [key ...] id [id ...]
// `$` means "start after the stream's current last id"; `+` means "return the stream's latest entry".
type XreadStream = { key: Buffer; afterId: StreamId | '$' | '+' }

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
      let afterId: XreadStream['afterId']
      if (idTok === '$') {
        afterId = '$'
      } else if (idTok === '+' && ctx.profile.has('xread.plus-id')) {
        afterId = '+'
      } else {
        afterId = parseExactId(idTok)
      }
      streams.push({ key, afterId })
    }

    return { value: { count, blockMs, streams }, nextIndex: input.length }
  })
}

type ResolvedXreadStream =
  | { key: Buffer; kind: 'after'; afterId: StreamId }
  | { key: Buffer; kind: 'latest' }

function readStreamEntries(
  streams: ResolvedXreadStream[],
  count: number | null,
  ctx: RedisExecutionContext,
): RedisResult | null {
  const results: [RedisValue, RedisValue][] = []

  for (const request of streams) {
    const { key } = request
    const stream = ctx.db.getStream(key)
    if (!stream) continue

    const entries: RedisValue[] = []

    if (request.kind === 'latest') {
      const entry = stream.entries.at(-1)
      if (entry) {
        entries.push(entryToReply(entry.id, entry.fields))
      }
    } else {
      for (const entry of stream.entries) {
        if (compareStreamId(entry.id, request.afterId) > 0) {
          entries.push(entryToReply(entry.id, entry.fields))
          if (count !== null && count > 0 && entries.length >= count) break
        }
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
    const resolved: ResolvedXreadStream[] = streams.map(s => {
      if (s.afterId === '+') {
        return { key: s.key, kind: 'latest' }
      }

      return {
        key: s.key,
        kind: 'after',
        afterId:
          s.afterId === '$'
            ? (ctx.db.getStream(s.key)?.lastId ?? MIN_ID)
            : s.afterId,
      }
    })

    const immediate = readStreamEntries(resolved, count, ctx)
    if (immediate || blockMs === null) return immediate ?? bulk(null)

    return blockingXread(resolved, count, blockMs, ctx)
  },
})
