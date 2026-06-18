import { defineCommand } from '../../core/command-definition'
import { t } from '../../core/command-schema'
import type { RedisExecutionContext } from '../../core/redis-context'
import {
  RedisSyntaxError,
  WrongNumberOfArgumentsError,
  ZrangeLimitWithoutByError,
  ZrangeWithScoresByLexError,
} from '../../core/redis-error'
import { RedisValue } from '../../core/redis-value'
import type { RedisSortedSetMember } from '../../state/data-types'
import { array, integer, scoreBuffer } from '../helpers'
import {
  applyLexLimit,
  getLexSortedMembers,
  lexMemberWithinBounds,
  type LexLimit,
  parseLexBoundArg,
  parseLexLimitInt,
} from './lex'
import { getSortedMembers } from './helpers'
import { parseScoreBoundArg, scoreWithinBounds } from './score'

// Modern ZRANGE (Redis 6.2+):
//   ZRANGE key min max [BYSCORE | BYLEX] [REV] [LIMIT offset count] [WITHSCORES]
// The legacy index-based form is the default (no BYSCORE/BYLEX). BYSCORE/BYLEX
// share the score-range and lex-range parsing/filtering helpers used by
// ZRANGEBYSCORE/ZRANGEBYLEX. With REV, the min/max arguments are given in
// reverse order (max then min), matching ZREVRANGEBYSCORE/ZREVRANGEBYLEX.
type ZrangeBy = 'index' | 'score' | 'lex'

type ZrangeSelectionArgs = {
  key: Buffer
  min: Buffer
  max: Buffer
  by: ZrangeBy
  rev: boolean
  limit?: LexLimit
}

type ZrangeArgs = ZrangeSelectionArgs & {
  withScores: boolean
}

type ZrangestoreArgs = ZrangeSelectionArgs & {
  destination: Buffer
}

type ParsedZrangeOptions = {
  by: ZrangeBy
  rev: boolean
  limit?: LexLimit
  withScores: boolean
}

function parseZrangeOptions(
  input: readonly Buffer[],
  cursor: number,
  options: { allowWithScores: boolean },
): ParsedZrangeOptions {
  let by: ZrangeBy = 'index'
  let rev = false
  let withScores = false
  let limit: LexLimit | undefined

  while (cursor < input.length) {
    const option = input[cursor]!.toString().toUpperCase()

    if (option === 'BYSCORE') {
      if (by === 'lex') throw new RedisSyntaxError()
      by = 'score'
      cursor++
      continue
    }

    if (option === 'BYLEX') {
      if (by === 'score') throw new RedisSyntaxError()
      by = 'lex'
      cursor++
      continue
    }

    if (option === 'REV') {
      rev = true
      cursor++
      continue
    }

    if (option === 'WITHSCORES') {
      if (!options.allowWithScores) throw new RedisSyntaxError()
      withScores = true
      cursor++
      continue
    }

    if (option === 'LIMIT') {
      const offsetTok = input[cursor + 1]
      const countTok = input[cursor + 2]
      if (!offsetTok || !countTok) throw new RedisSyntaxError()
      limit = {
        offset: parseLexLimitInt(offsetTok),
        count: parseLexLimitInt(countTok),
      }
      cursor += 3
      continue
    }

    throw new RedisSyntaxError()
  }

  if (limit && by === 'index') throw new ZrangeLimitWithoutByError()
  if (withScores && by === 'lex') throw new ZrangeWithScoresByLexError()

  return { by, rev, limit, withScores }
}

function createZrangeSchema() {
  return t.custom<ZrangeArgs>((input, index, ctx) => {
    const key = input[index]
    const min = input[index + 1]
    const max = input[index + 2]
    if (!key || !min || !max) {
      throw new WrongNumberOfArgumentsError(ctx.commandName)
    }

    return {
      value: {
        key,
        min,
        max,
        ...parseZrangeOptions(input, index + 3, { allowWithScores: true }),
      },
      nextIndex: input.length,
    }
  })
}

function createZrangestoreSchema() {
  return t.custom<ZrangestoreArgs>((input, index, ctx) => {
    const destination = input[index]
    const key = input[index + 1]
    const min = input[index + 2]
    const max = input[index + 3]
    if (!destination || !key || !min || !max) {
      throw new WrongNumberOfArgumentsError(ctx.commandName)
    }

    const { by, rev, limit } = parseZrangeOptions(input, index + 4, {
      allowWithScores: false,
    })

    return {
      value: { destination, key, min, max, by, rev, limit },
      nextIndex: input.length,
    }
  })
}

function selectZrangeMembers(
  args: ZrangeSelectionArgs,
  ctx: RedisExecutionContext,
): RedisSortedSetMember[] {
  // With REV the bounds are supplied as `max min`, so swap before parsing.
  const minTok = args.rev ? args.max : args.min
  const maxTok = args.rev ? args.min : args.max

  if (args.by === 'index') {
    // parse index bounds up front so a bad index errors even on a missing key
    const startIdx = parseLexLimitInt(args.min)
    const stopIdx = parseLexLimitInt(args.max)
    const zset = ctx.db.getSortedSet(args.key)
    if (!zset) return []
    return sliceByIndex(getSortedMembers(zset), startIdx, stopIdx, args.rev)
  }

  if (args.by === 'score') {
    const min = parseScoreBoundArg(minTok.toString())
    const max = parseScoreBoundArg(maxTok.toString())
    const zset = ctx.db.getSortedSet(args.key)
    if (!zset) return []
    let matched = getSortedMembers(zset).filter(m =>
      scoreWithinBounds(m.score, min, max),
    )
    if (args.rev) matched = matched.reverse()
    return applyLexLimit(matched, args.limit)
  }

  // BYLEX
  const min = parseLexBoundArg(minTok)
  const max = parseLexBoundArg(maxTok)
  const zset = ctx.db.getSortedSet(args.key)
  if (!zset) return []
  let matched = getLexSortedMembers(zset).filter(m =>
    lexMemberWithinBounds(m.member, min, max),
  )
  if (args.rev) matched = matched.reverse()
  return applyLexLimit(matched, args.limit)
}

function buildStoredMembers(
  members: RedisSortedSetMember[],
): Map<string, RedisSortedSetMember> {
  return new Map(
    members.map(entry => [
      entry.member.toString('hex'),
      { member: Buffer.from(entry.member), score: entry.score },
    ]),
  )
}

function buildRangeOutput(
  members: RedisSortedSetMember[],
  withScores: boolean,
): RedisValue[] {
  const items: RedisValue[] = []
  for (const entry of members) {
    items.push(RedisValue.bulkString(entry.member))
    if (withScores) {
      items.push(RedisValue.bulkString(scoreBuffer(entry.score)))
    }
  }
  return items
}

function sliceByIndex(
  sorted: RedisSortedSetMember[],
  startIdx: number,
  stopIdx: number,
  rev: boolean,
): RedisSortedSetMember[] {
  const ordered = rev ? sorted.slice().reverse() : sorted
  const len = ordered.length
  const start = startIdx < 0 ? Math.max(0, len + startIdx) : startIdx
  const stop = stopIdx < 0 ? len + stopIdx : Math.min(stopIdx, len - 1)
  if (start > stop) return []
  return ordered.slice(start, stop + 1)
}

export const zrangeCommand = defineCommand({
  name: 'zrange',
  schema: createZrangeSchema(),
  flags: ['readonly'],
  keys: args => [args.key],
  execute: (args, ctx) => {
    return array(
      buildRangeOutput(selectZrangeMembers(args, ctx), args.withScores),
    )
  },
})

export const zrangestoreCommand = defineCommand({
  name: 'zrangestore',
  schema: createZrangestoreSchema(),
  flags: ['write'],
  keys: args => [args.destination, args.key],
  execute: (args, ctx) => {
    const members = buildStoredMembers(selectZrangeMembers(args, ctx))
    if (members.size === 0) {
      ctx.db.delete(args.destination)
      return integer(0)
    }

    ctx.db.delete(args.destination)
    ctx.db.updateSortedSet(args.destination, zset => {
      zset.replaceMembers(members, { forceDirty: true })
    })
    return integer(members.size)
  },
})

export const zrevrangeCommand = defineCommand({
  name: 'zrevrange',
  schema: t.object({
    key: t.key(),
    start: t.integer(),
    stop: t.integer(),
    withScores: t.optional(t.keyword('WITHSCORES')),
  }),
  flags: ['readonly'],
  keys: args => [args.key],
  execute: (args, ctx) => {
    const zset = ctx.db.getSortedSet(args.key)
    if (!zset) return array([])
    const sorted = getSortedMembers(zset).slice().reverse()
    const len = sorted.length
    const start = args.start < 0 ? Math.max(0, len + args.start) : args.start
    const stop = args.stop < 0 ? len + args.stop : Math.min(args.stop, len - 1)
    if (start > stop) return array([])
    const slice = sorted.slice(start, stop + 1)
    const items: RedisValue[] = []
    for (const entry of slice) {
      items.push(RedisValue.bulkString(entry.member))
      if (args.withScores) {
        items.push(RedisValue.bulkString(scoreBuffer(entry.score)))
      }
    }
    return array(items)
  },
})
