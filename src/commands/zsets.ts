import { defineCommand } from '../core/command-definition'
import { t, type ParseContext } from '../core/command-schema'
import {
  ExpectedFloatError,
  ExpectedIntegerError,
  InvalidLexRangeError,
  MinMaxNotFloatError,
  PositiveCountError,
  RedisSyntaxError,
  ResultingScoreNaNError,
  WrongNumberOfArgumentsError,
  ZaddGtLtNxConflictError,
  ZaddIncrPairError,
  ZaddNxXxConflictError,
} from '../core/redis-error'
import { RedisResult } from '../core/redis-result'
import { RedisValue } from '../core/redis-value'
import type { RedisDatabase } from '../state/database'
import type {
  RedisSortedSetData,
  RedisSortedSetMember,
} from '../state/data-types'
import { bulk, integer, array, scoreBuffer } from './helpers'

function getSortedMembers(zset: RedisSortedSetData): RedisSortedSetMember[] {
  return Array.from(zset.members.values()).sort((a, b) =>
    a.score !== b.score
      ? a.score - b.score
      : a.member.toString().localeCompare(b.member.toString()),
  )
}

function parseFloatArg(s: string): number {
  const normalized = s.toLowerCase()
  if (normalized === 'inf' || normalized === '+inf') return Infinity
  if (normalized === '-inf') return -Infinity

  const n = Number(s)
  if (!Number.isFinite(n)) throw new ExpectedFloatError()
  return n
}

function assertValidResultingScore(score: number) {
  if (Number.isNaN(score)) {
    throw new ResultingScoreNaNError()
  }
}

type ScoreBound = { value: number; exclusive: boolean }

function parseScoreBoundArg(s: string): ScoreBound {
  const exclusive = s.startsWith('(')
  const raw = exclusive ? s.slice(1) : s

  if (raw.length === 0) throw new MinMaxNotFloatError()

  const normalized = raw.toLowerCase()
  if (normalized === '+inf') return { value: Infinity, exclusive }
  if (normalized === '-inf') return { value: -Infinity, exclusive }

  const n = Number(raw)
  if (!Number.isFinite(n)) throw new MinMaxNotFloatError()
  return { value: n, exclusive }
}

function scoreWithinBounds(score: number, min: ScoreBound, max: ScoreBound) {
  if (min.exclusive ? score <= min.value : score < min.value) return false
  if (max.exclusive ? score >= max.value : score > max.value) return false
  return true
}

// Lexicographic range bounds — used by ZRANGEBYLEX/ZREVRANGEBYLEX/ZLEXCOUNT/
// ZREMRANGEBYLEX. All members are assumed to share the same score, so ordering
// is by raw byte comparison (not localeCompare — see #41).
type LexBound =
  | { kind: 'min' } // `-` negative infinity
  | { kind: 'max' } // `+` positive infinity
  | { kind: 'value'; value: Buffer; exclusive: boolean }

function parseLexBoundArg(token: Buffer): LexBound {
  if (token.length === 1) {
    if (token[0] === 0x2d /* - */) return { kind: 'min' }
    if (token[0] === 0x2b /* + */) return { kind: 'max' }
  }

  const prefix = token[0]
  if (prefix === 0x5b /* [ */) {
    return { kind: 'value', value: token.subarray(1), exclusive: false }
  }
  if (prefix === 0x28 /* ( */) {
    return { kind: 'value', value: token.subarray(1), exclusive: true }
  }

  throw new InvalidLexRangeError()
}

function lexMemberWithinBounds(
  member: Buffer,
  min: LexBound,
  max: LexBound,
): boolean {
  if (min.kind === 'max') return false
  if (min.kind === 'value') {
    const cmp = Buffer.compare(member, min.value)
    if (min.exclusive ? cmp <= 0 : cmp < 0) return false
  }

  if (max.kind === 'min') return false
  if (max.kind === 'value') {
    const cmp = Buffer.compare(member, max.value)
    if (max.exclusive ? cmp >= 0 : cmp > 0) return false
  }

  return true
}

function getLexSortedMembers(zset: RedisSortedSetData): RedisSortedSetMember[] {
  return Array.from(zset.members.values()).sort((a, b) =>
    a.score !== b.score
      ? a.score - b.score
      : Buffer.compare(a.member, b.member),
  )
}

type LexLimit = { offset: number; count: number }

type LexRangeArgs = {
  key: Buffer
  first: Buffer
  second: Buffer
  limit?: LexLimit
}

function parseLexLimitInt(token: Buffer): number {
  const raw = token.toString()
  if (!/^-?\d+$/.test(raw)) throw new ExpectedIntegerError()
  const value = Number(raw)
  if (!Number.isSafeInteger(value)) throw new ExpectedIntegerError()
  return value
}

// ZRANGEBYLEX key min max [LIMIT offset count]
// ZREVRANGEBYLEX key max min [LIMIT offset count] — caller maps first/second.
function createLexRangeSchema() {
  return t.custom<LexRangeArgs>((input, index, ctx) => {
    const key = input[index]
    const first = input[index + 1]
    const second = input[index + 2]
    if (!key || !first || !second) {
      throw new WrongNumberOfArgumentsError(ctx.commandName)
    }

    let cursor = index + 3
    let limit: LexLimit | undefined
    if (cursor < input.length) {
      if (input[cursor]!.toString().toUpperCase() !== 'LIMIT') {
        throw new RedisSyntaxError()
      }
      const offsetTok = input[cursor + 1]
      const countTok = input[cursor + 2]
      if (!offsetTok || !countTok) throw new RedisSyntaxError()
      limit = {
        offset: parseLexLimitInt(offsetTok),
        count: parseLexLimitInt(countTok),
      }
      cursor += 3
      if (cursor < input.length) throw new RedisSyntaxError()
    }

    return { value: { key, first, second, limit }, nextIndex: cursor }
  })
}

function applyLexLimit(
  members: RedisSortedSetMember[],
  limit: LexLimit | undefined,
): RedisSortedSetMember[] {
  if (!limit) return members
  if (limit.offset < 0) return []
  const end = limit.count < 0 ? members.length : limit.offset + limit.count
  return members.slice(limit.offset, end)
}

function parsePopCountArg(s: string): number {
  if (!/^-?\d+$/.test(s)) {
    throw new PositiveCountError()
  }

  const count = Number(s)
  if (!Number.isSafeInteger(count) || count < 0) {
    throw new PositiveCountError()
  }

  return count
}

type ZaddPair = { score: number; member: Buffer }
type ZaddCondition = 'NX' | 'XX'
type ZaddComparison = 'GT' | 'LT'

type ZaddOptions = {
  condition?: ZaddCondition
  comparison?: ZaddComparison
  ch: boolean
  incr: boolean
}

type ZaddArgs = {
  key: Buffer
  options: ZaddOptions
  pairs: ZaddPair[]
}

function createZaddSchema() {
  return t.custom<ZaddArgs>(
    (input: readonly Buffer[], index: number, ctx: ParseContext) => {
      const key = input[index]
      if (!key) {
        throw new WrongNumberOfArgumentsError(ctx.commandName)
      }

      const options: ZaddOptions = { ch: false, incr: false }
      let cursor = index + 1

      while (cursor < input.length) {
        const option = input[cursor]!.toString().toUpperCase()

        if (option === 'NX') {
          if (options.condition === 'XX') throw new ZaddNxXxConflictError()
          options.condition = 'NX'
          cursor++
          continue
        }

        if (option === 'XX') {
          if (options.condition === 'NX') throw new ZaddNxXxConflictError()
          options.condition = 'XX'
          cursor++
          continue
        }

        if (option === 'GT') {
          if (options.comparison === 'LT') {
            throw new ZaddGtLtNxConflictError()
          }
          options.comparison = 'GT'
          cursor++
          continue
        }

        if (option === 'LT') {
          if (options.comparison === 'GT') {
            throw new ZaddGtLtNxConflictError()
          }
          options.comparison = 'LT'
          cursor++
          continue
        }

        if (option === 'CH') {
          options.ch = true
          cursor++
          continue
        }

        if (option === 'INCR') {
          options.incr = true
          cursor++
          continue
        }

        break
      }

      if (options.condition === 'NX' && options.comparison) {
        throw new ZaddGtLtNxConflictError()
      }

      const pairs = parseZaddPairs(input, cursor, ctx)
      if (options.incr && pairs.length !== 1) {
        throw new ZaddIncrPairError()
      }

      return { value: { key, options, pairs }, nextIndex: input.length }
    },
  )
}

function parseZaddPairs(
  input: readonly Buffer[],
  index: number,
  ctx: ParseContext,
): ZaddPair[] {
  const pairs: ZaddPair[] = []
  let cursor = index

  if (cursor >= input.length) {
    throw new WrongNumberOfArgumentsError(ctx.commandName)
  }

  while (cursor < input.length) {
    const scoreToken = input[cursor]
    const memberToken = input[cursor + 1]
    if (!scoreToken || !memberToken) {
      throw new WrongNumberOfArgumentsError(ctx.commandName)
    }

    const score = parseFloatArg(scoreToken.toString())

    pairs.push({ score, member: memberToken })
    cursor += 2
  }

  return pairs
}

function shouldApplyZaddUpdate(
  existing: RedisSortedSetMember | undefined,
  score: number,
  options: ZaddOptions,
): boolean {
  if (!existing) return options.condition !== 'XX'
  if (options.condition === 'NX') return false
  if (options.comparison === 'GT') return score > existing.score
  if (options.comparison === 'LT') return score < existing.score
  return true
}

function deleteSortedSetIfEmpty(db: RedisDatabase, key: Buffer) {
  if ((db.getSortedSet(key)?.members.size ?? 0) === 0) {
    db.delete(key)
  }
}

export const zaddCommand = defineCommand({
  name: 'zadd',
  schema: createZaddSchema(),
  flags: ['write', 'denyoom', 'fast'],
  keys: args => [args.key],
  execute: (args, ctx) => {
    if (args.options.incr) {
      const [{ score, member }] = args.pairs
      const newScore = ctx.db.updateSortedSet(args.key, zset => {
        const hex = member.toString('hex')
        const existing = zset.members.get(hex)
        const nextScore = (existing?.score ?? 0) + score
        assertValidResultingScore(nextScore)

        if (!shouldApplyZaddUpdate(existing, nextScore, args.options)) {
          return null
        }

        zset.members.set(hex, { member, score: nextScore })
        return nextScore
      })
      deleteSortedSetIfEmpty(ctx.db, args.key)
      return bulk(newScore === null ? null : scoreBuffer(newScore))
    }

    const changed = ctx.db.updateSortedSet(args.key, zset => {
      let count = 0
      for (const { score, member } of args.pairs) {
        const hex = member.toString('hex')
        const existing = zset.members.get(hex)

        if (!shouldApplyZaddUpdate(existing, score, args.options)) {
          continue
        }

        if (!existing || args.options.ch) {
          if (!existing || existing.score !== score) count++
        }

        zset.members.set(hex, { member, score })
      }
      return count
    })
    deleteSortedSetIfEmpty(ctx.db, args.key)
    return integer(changed)
  },
})

export const zremCommand = defineCommand({
  name: 'zrem',
  schema: t.object({ key: t.key(), members: t.variadic(t.key(), { min: 1 }) }),
  flags: ['write', 'fast'],
  keys: args => [args.key],
  execute: (args, ctx) => {
    let removed = 0
    ctx.db.updateSortedSet(args.key, zset => {
      for (const member of args.members) {
        if (zset.members.delete(member.toString('hex'))) removed++
      }
    })
    if (
      removed > 0 &&
      (ctx.db.getSortedSet(args.key)?.members.size ?? 0) === 0
    ) {
      ctx.db.delete(args.key)
    }
    return integer(removed)
  },
})

export const zcardCommand = defineCommand({
  name: 'zcard',
  schema: t.object({ key: t.key() }),
  flags: ['readonly', 'fast'],
  keys: args => [args.key],
  execute: (args, ctx) => {
    const zset = ctx.db.getSortedSet(args.key)
    return integer(zset?.members.size ?? 0)
  },
})

export const zrankCommand = defineCommand({
  name: 'zrank',
  schema: t.object({ key: t.key(), member: t.key() }),
  flags: ['readonly', 'fast'],
  keys: args => [args.key],
  execute: (args, ctx) => {
    const zset = ctx.db.getSortedSet(args.key)
    if (!zset) return RedisResult.nil()
    const hex = args.member.toString('hex')
    if (!zset.members.has(hex)) return RedisResult.nil()
    const sorted = getSortedMembers(zset)
    const rank = sorted.findIndex(m => m.member.toString('hex') === hex)
    return integer(rank)
  },
})

export const zrevrankCommand = defineCommand({
  name: 'zrevrank',
  schema: t.object({ key: t.key(), member: t.key() }),
  flags: ['readonly', 'fast'],
  keys: args => [args.key],
  execute: (args, ctx) => {
    const zset = ctx.db.getSortedSet(args.key)
    if (!zset) return RedisResult.nil()
    const hex = args.member.toString('hex')
    if (!zset.members.has(hex)) return RedisResult.nil()
    const sorted = getSortedMembers(zset)
    const rank = sorted
      .slice()
      .reverse()
      .findIndex(m => m.member.toString('hex') === hex)
    return integer(rank)
  },
})

export const zscoreCommand = defineCommand({
  name: 'zscore',
  schema: t.object({ key: t.key(), member: t.key() }),
  flags: ['readonly', 'fast'],
  keys: args => [args.key],
  execute: (args, ctx) => {
    const zset = ctx.db.getSortedSet(args.key)
    if (!zset) return bulk(null)
    const entry = zset.members.get(args.member.toString('hex'))
    if (!entry) return bulk(null)
    return bulk(scoreBuffer(entry.score))
  },
})

export const zincrbyCommand = defineCommand({
  name: 'zincrby',
  schema: t.object({ key: t.key(), increment: t.string(), member: t.key() }),
  flags: ['write', 'fast'],
  keys: args => [args.key],
  execute: (args, ctx) => {
    const inc = parseFloatArg(args.increment)
    const newScore = ctx.db.updateSortedSet(args.key, zset => {
      const hex = args.member.toString('hex')
      const existing = zset.members.get(hex)
      const score = (existing?.score ?? 0) + inc
      assertValidResultingScore(score)
      zset.members.set(hex, { member: args.member, score })
      return score
    })
    return bulk(scoreBuffer(newScore))
  },
})

export const zrangeCommand = defineCommand({
  name: 'zrange',
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
    const sorted = getSortedMembers(zset)
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

export const zrangebyscoreCommand = defineCommand({
  name: 'zrangebyscore',
  schema: t.object({ key: t.key(), min: t.string(), max: t.string() }),
  flags: ['readonly'],
  keys: args => [args.key],
  execute: (args, ctx) => {
    const min = parseScoreBoundArg(args.min)
    const max = parseScoreBoundArg(args.max)
    const zset = ctx.db.getSortedSet(args.key)
    if (!zset) return array([])
    const items: RedisValue[] = []
    for (const entry of getSortedMembers(zset)) {
      if (scoreWithinBounds(entry.score, min, max)) {
        items.push(RedisValue.bulkString(entry.member))
      }
    }
    return array(items)
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
      for (const [hex, entry] of zset.members) {
        if (scoreWithinBounds(entry.score, min, max)) {
          zset.members.delete(hex)
          removed++
        }
      }
    })
    if (
      removed > 0 &&
      (ctx.db.getSortedSet(args.key)?.members.size ?? 0) === 0
    ) {
      ctx.db.delete(args.key)
    }
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

export const zrangebylexCommand = defineCommand({
  name: 'zrangebylex',
  schema: createLexRangeSchema(),
  flags: ['readonly'],
  keys: args => [args.key],
  execute: (args, ctx) => {
    const min = parseLexBoundArg(args.first)
    const max = parseLexBoundArg(args.second)
    const zset = ctx.db.getSortedSet(args.key)
    if (!zset) return array([])
    const matched = getLexSortedMembers(zset).filter(m =>
      lexMemberWithinBounds(m.member, min, max),
    )
    const limited = applyLexLimit(matched, args.limit)
    return array(limited.map(m => RedisValue.bulkString(m.member)))
  },
})

export const zrevrangebylexCommand = defineCommand({
  name: 'zrevrangebylex',
  schema: createLexRangeSchema(),
  flags: ['readonly'],
  keys: args => [args.key],
  execute: (args, ctx) => {
    // ZREVRANGEBYLEX takes bounds as `max min`.
    const max = parseLexBoundArg(args.first)
    const min = parseLexBoundArg(args.second)
    const zset = ctx.db.getSortedSet(args.key)
    if (!zset) return array([])
    const matched = getLexSortedMembers(zset)
      .filter(m => lexMemberWithinBounds(m.member, min, max))
      .reverse()
    const limited = applyLexLimit(matched, args.limit)
    return array(limited.map(m => RedisValue.bulkString(m.member)))
  },
})

export const zlexcountCommand = defineCommand({
  name: 'zlexcount',
  schema: t.object({ key: t.key(), min: t.bulk(), max: t.bulk() }),
  flags: ['readonly', 'fast'],
  keys: args => [args.key],
  execute: (args, ctx) => {
    const min = parseLexBoundArg(args.min)
    const max = parseLexBoundArg(args.max)
    const zset = ctx.db.getSortedSet(args.key)
    if (!zset) return integer(0)
    let count = 0
    for (const entry of zset.members.values()) {
      if (lexMemberWithinBounds(entry.member, min, max)) count++
    }
    return integer(count)
  },
})

export const zremrangebylexCommand = defineCommand({
  name: 'zremrangebylex',
  schema: t.object({ key: t.key(), min: t.bulk(), max: t.bulk() }),
  flags: ['write'],
  keys: args => [args.key],
  execute: (args, ctx) => {
    const min = parseLexBoundArg(args.min)
    const max = parseLexBoundArg(args.max)
    let removed = 0
    ctx.db.updateSortedSet(args.key, zset => {
      for (const [hex, entry] of zset.members) {
        if (lexMemberWithinBounds(entry.member, min, max)) {
          zset.members.delete(hex)
          removed++
        }
      }
    })
    if (
      removed > 0 &&
      (ctx.db.getSortedSet(args.key)?.members.size ?? 0) === 0
    ) {
      ctx.db.delete(args.key)
    }
    return integer(removed)
  },
})

export const zpopminCommand = defineCommand({
  name: 'zpopmin',
  schema: t.object({ key: t.key(), count: t.optional(zpopCountSchema()) }),
  flags: ['write', 'fast'],
  keys: args => [args.key],
  execute: (args, ctx) => {
    const zset = ctx.db.getSortedSet(args.key)
    if (!zset) return array([])
    const count = args.count ?? 1
    const sorted = getSortedMembers(zset)
    const toRemove = sorted.slice(0, count)
    if (toRemove.length === 0) return array([])
    ctx.db.updateSortedSet(args.key, z => {
      for (const entry of toRemove) {
        z.members.delete(entry.member.toString('hex'))
      }
    })
    if ((ctx.db.getSortedSet(args.key)?.members.size ?? 0) === 0) {
      ctx.db.delete(args.key)
    }
    const items: RedisValue[] = []
    for (const entry of toRemove) {
      items.push(RedisValue.bulkString(entry.member))
      items.push(RedisValue.bulkString(scoreBuffer(entry.score)))
    }
    return array(items)
  },
})

export const zpopmaxCommand = defineCommand({
  name: 'zpopmax',
  schema: t.object({ key: t.key(), count: t.optional(zpopCountSchema()) }),
  flags: ['write', 'fast'],
  keys: args => [args.key],
  execute: (args, ctx) => {
    const zset = ctx.db.getSortedSet(args.key)
    if (!zset) return array([])
    const count = args.count ?? 1
    const sorted = getSortedMembers(zset).slice().reverse()
    const toRemove = sorted.slice(0, count)
    if (toRemove.length === 0) return array([])
    ctx.db.updateSortedSet(args.key, z => {
      for (const entry of toRemove) {
        z.members.delete(entry.member.toString('hex'))
      }
    })
    if ((ctx.db.getSortedSet(args.key)?.members.size ?? 0) === 0) {
      ctx.db.delete(args.key)
    }
    const items: RedisValue[] = []
    for (const entry of toRemove) {
      items.push(RedisValue.bulkString(entry.member))
      items.push(RedisValue.bulkString(scoreBuffer(entry.score)))
    }
    return array(items)
  },
})

function zpopCountSchema() {
  return t.custom<number>((input, index, ctx) => {
    const token = input[index]
    if (!token) {
      throw new WrongNumberOfArgumentsError(ctx.commandName)
    }

    return {
      value: parsePopCountArg(token.toString()),
      nextIndex: index + 1,
    }
  })
}

export const zsetsCommands = [
  zaddCommand,
  zremCommand,
  zcardCommand,
  zrankCommand,
  zrevrankCommand,
  zscoreCommand,
  zincrbyCommand,
  zrangeCommand,
  zrevrangeCommand,
  zrangebyscoreCommand,
  zremrangebyscoreCommand,
  zcountCommand,
  zrangebylexCommand,
  zrevrangebylexCommand,
  zlexcountCommand,
  zremrangebylexCommand,
  zpopminCommand,
  zpopmaxCommand,
]
