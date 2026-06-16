import { defineCommand } from '../../core/command-definition'
import { t, type ParseContext } from '../../core/command-schema'
import {
  ExpectedIntegerError,
  WrongNumberOfArgumentsError,
} from '../../core/redis-error'
import { RedisValue } from '../../core/redis-value'
import type { RedisStreamData } from '../../state/data-types'
import { array } from '../helpers'
import { compareStreamId, exclusiveAware, parseRangeId } from './ids'
import { entryToReply } from './replies'

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
