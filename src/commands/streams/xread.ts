import { defineCommand } from '../../core/command-definition'
import { xreadGetKeys } from '../../core/key-specs'
import { commandKeywordKeySpec } from '../introspection'
import { t, type ParseContext } from '../../core/command-schema'
import type { RedisExecutionContext } from '../../core/redis-context'
import { RedisResult } from '../../core/redis-result'
import { RedisValue } from '../../core/redis-value'
import type { StreamId } from '../../state/data-types'
import { bulk } from '../helpers'
import { compareStreamId, MIN_ID, parseExactId } from './ids'
import { entryToReply } from './replies'
import { blockOnKeys } from '../blocking'
import { parseXreadOptions } from './xread-options'

// XREAD [COUNT count] STREAMS key [key ...] id [id ...]
// `$` means "start after the stream's current last id"; `+` means "return the stream's latest entry".
type XreadStream = { key: Buffer; afterId: StreamId | '$' | '+' }

function createXreadSchema() {
  return t.custom<{
    count: number | null
    blockMs: number | null
    streams: XreadStream[]
  }>((input: readonly Buffer[], index: number, ctx: ParseContext) => {
    const options = parseXreadOptions(input, index, ctx, false)
    const { count, blockMs } = options
    const cursor = options.streamsStart
    const half = options.streamCount
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
  const result = await blockOnKeys(ctx, {
    keys: streams.map(s => s.key),
    type: 'stream',
    timeoutMs: blockMs === 0 ? undefined : blockMs,
    attempt: () => readStreamEntries(streams, count, ctx),
  })
  return result ?? bulk(null)
}

export const xreadCommand = defineCommand({
  name: 'xread',
  rawKeys: xreadGetKeys,
  schema: t.object({ args: t.withLayout(createXreadSchema(), { min: 3 }) }),
  flags: ['readonly'],
  introspection: {
    keySpecs: [
      commandKeywordKeySpec(
        'STREAMS',
        1,
        { lastKey: -1, keyStep: 1, limit: 2 },
        ['RO', 'access'],
      ),
    ],
  },
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
