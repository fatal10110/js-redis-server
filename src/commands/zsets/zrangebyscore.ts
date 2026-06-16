import { defineCommand } from '../../core/command-definition'
import { t } from '../../core/command-schema'
import {
  ExpectedIntegerError,
  RedisSyntaxError,
  WrongNumberOfArgumentsError,
} from '../../core/redis-error'
import { RedisValue } from '../../core/redis-value'
import type { RedisSortedSetMember } from '../../state/data-types'
import { array, integer, scoreBuffer } from '../helpers'
import { getSortedMembers, deleteSortedSetIfEmpty } from './helpers'
import { parseScoreBoundArg, scoreWithinBounds } from './score'

// ZRANGEBYSCORE key min max [WITHSCORES] [LIMIT offset count]
// ZREVRANGEBYSCORE key max min [WITHSCORES] [LIMIT offset count]
// WITHSCORES and LIMIT may appear in either order, matching real Redis.
type ScoreLimit = { offset: number; count: number }

type ScoreRangeArgs = {
  key: Buffer
  first: Buffer
  second: Buffer
  withScores: boolean
  limit?: ScoreLimit
}

function parseScoreLimitInt(token: Buffer): number {
  const raw = token.toString()
  if (!/^-?\d+$/.test(raw)) throw new ExpectedIntegerError()
  const value = Number(raw)
  if (!Number.isSafeInteger(value)) throw new ExpectedIntegerError()
  return value
}

function createScoreRangeSchema() {
  return t.custom<ScoreRangeArgs>((input, index, ctx) => {
    const key = input[index]
    const first = input[index + 1]
    const second = input[index + 2]
    if (!key || !first || !second) {
      throw new WrongNumberOfArgumentsError(ctx.commandName)
    }

    let withScores = false
    let limit: ScoreLimit | undefined

    let cursor = index + 3
    while (cursor < input.length) {
      const option = input[cursor]!.toString().toUpperCase()

      if (option === 'WITHSCORES') {
        withScores = true
        cursor++
        continue
      }

      if (option === 'LIMIT') {
        const offsetTok = input[cursor + 1]
        const countTok = input[cursor + 2]
        if (!offsetTok || !countTok) throw new RedisSyntaxError()
        limit = {
          offset: parseScoreLimitInt(offsetTok),
          count: parseScoreLimitInt(countTok),
        }
        cursor += 3
        continue
      }

      throw new RedisSyntaxError()
    }

    return {
      value: { key, first, second, withScores, limit },
      nextIndex: input.length,
    }
  })
}

function applyScoreLimit(
  members: RedisSortedSetMember[],
  limit: ScoreLimit | undefined,
): RedisSortedSetMember[] {
  if (!limit) return members
  if (limit.offset < 0) return []
  const end = limit.count < 0 ? members.length : limit.offset + limit.count
  return members.slice(limit.offset, end)
}

function buildScoreRangeOutput(
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

export const zrangebyscoreCommand = defineCommand({
  name: 'zrangebyscore',
  schema: createScoreRangeSchema(),
  flags: ['readonly'],
  keys: args => [args.key],
  execute: (args, ctx) => {
    const min = parseScoreBoundArg(args.first.toString())
    const max = parseScoreBoundArg(args.second.toString())
    const zset = ctx.db.getSortedSet(args.key)
    if (!zset) return array([])
    const matched = getSortedMembers(zset).filter(m =>
      scoreWithinBounds(m.score, min, max),
    )
    return array(
      buildScoreRangeOutput(
        applyScoreLimit(matched, args.limit),
        args.withScores,
      ),
    )
  },
})

export const zrevrangebyscoreCommand = defineCommand({
  name: 'zrevrangebyscore',
  schema: createScoreRangeSchema(),
  flags: ['readonly'],
  keys: args => [args.key],
  execute: (args, ctx) => {
    // ZREVRANGEBYSCORE takes bounds as `max min`.
    const max = parseScoreBoundArg(args.first.toString())
    const min = parseScoreBoundArg(args.second.toString())
    const zset = ctx.db.getSortedSet(args.key)
    if (!zset) return array([])
    const matched = getSortedMembers(zset)
      .filter(m => scoreWithinBounds(m.score, min, max))
      .reverse()
    return array(
      buildScoreRangeOutput(
        applyScoreLimit(matched, args.limit),
        args.withScores,
      ),
    )
  },
})

export const zremrangebyscoreCommand = defineCommand({
  name: 'zremrangebyscore',
  schema: t.object({ key: t.key(), min: t.string(), max: t.string() }),
  flags: ['write'],
  keys: args => [args.key],
  execute: (args, ctx) => {
    const min = parseScoreBoundArg(args.min)
    const max = parseScoreBoundArg(args.max)
    let removed = 0
    ctx.db.updateSortedSet(args.key, zset => {
      for (const [hex, entry] of zset.entries()) {
        if (scoreWithinBounds(entry.score, min, max)) {
          zset.deleteMemberId(hex)
          removed++
        }
      }
    })
    if (removed > 0) deleteSortedSetIfEmpty(ctx.db, args.key)
    return integer(removed)
  },
})

export const zremrangebyrankCommand = defineCommand({
  name: 'zremrangebyrank',
  schema: t.object({ key: t.key(), start: t.integer(), stop: t.integer() }),
  flags: ['write'],
  keys: args => [args.key],
  execute: (args, ctx) => {
    const zset = ctx.db.getSortedSet(args.key)
    if (!zset) return integer(0)
    const sorted = getSortedMembers(zset)
    const len = sorted.length
    const start = args.start < 0 ? Math.max(0, len + args.start) : args.start
    const stop = args.stop < 0 ? len + args.stop : Math.min(args.stop, len - 1)
    if (start > stop) return integer(0)

    const toRemove = new Set(
      sorted.slice(start, stop + 1).map(m => m.member.toString('hex')),
    )
    let removed = 0
    ctx.db.updateSortedSet(args.key, set => {
      for (const hex of toRemove) {
        if (set.deleteMemberId(hex)) removed++
      }
    })
    if (removed > 0) deleteSortedSetIfEmpty(ctx.db, args.key)
    return integer(removed)
  },
})

export const zcountCommand = defineCommand({
  name: 'zcount',
  schema: t.object({ key: t.key(), min: t.string(), max: t.string() }),
  flags: ['readonly', 'fast'],
  keys: args => [args.key],
  execute: (args, ctx) => {
    const min = parseScoreBoundArg(args.min)
    const max = parseScoreBoundArg(args.max)
    const zset = ctx.db.getSortedSet(args.key)
    if (!zset) return integer(0)
    let count = 0
    for (const entry of zset.members.values()) {
      if (scoreWithinBounds(entry.score, min, max)) count++
    }
    return integer(count)
  },
})
