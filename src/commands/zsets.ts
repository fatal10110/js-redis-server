import { defineCommand } from '../core/command-definition'
import { t, type ParseContext } from '../core/command-schema'
import {
  ExpectedFloatError,
  WrongNumberOfArgumentsError,
} from '../core/redis-error'
import { RedisResult } from '../core/redis-result'
import { RedisValue } from '../core/redis-value'
import type {
  RedisSortedSetData,
  RedisSortedSetMember,
} from '../state/data-types'
import { bulk, integer, array } from './helpers'

function getSortedMembers(zset: RedisSortedSetData): RedisSortedSetMember[] {
  return Array.from(zset.members.values()).sort((a, b) =>
    a.score !== b.score
      ? a.score - b.score
      : a.member.toString().localeCompare(b.member.toString()),
  )
}

function parseFloatArg(s: string): number {
  const n = Number(s)
  if (!Number.isFinite(n)) throw new ExpectedFloatError()
  return n
}

type ZaddPair = { score: number; member: Buffer }

function createZaddPairsSchema() {
  return t.custom<ZaddPair[]>(
    (input: readonly Buffer[], index: number, ctx: ParseContext) => {
      const pairs: ZaddPair[] = []
      let cursor = index
      if (cursor >= input.length)
        throw new WrongNumberOfArgumentsError(ctx.commandName)
      while (cursor < input.length) {
        const scoreToken = input[cursor]
        const memberToken = input[cursor + 1]
        if (!scoreToken || !memberToken)
          throw new WrongNumberOfArgumentsError(ctx.commandName)
        const score = Number(scoreToken.toString())
        if (!Number.isFinite(score)) throw new ExpectedFloatError()
        pairs.push({ score, member: memberToken })
        cursor += 2
      }
      return { value: pairs, nextIndex: cursor }
    },
  )
}

export const zaddCommand = defineCommand({
  name: 'zadd',
  schema: t.object({ key: t.key(), pairs: createZaddPairsSchema() }),
  flags: ['write', 'denyoom', 'fast'],
  keys: args => [args.key],
  execute: (args, ctx) => {
    const added = ctx.db.updateSortedSet(args.key, zset => {
      let count = 0
      for (const { score, member } of args.pairs) {
        const hex = member.toString('hex')
        if (!zset.members.has(hex)) count++
        zset.members.set(hex, { member, score })
      }
      return count
    })
    return integer(added)
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
    return bulk(Buffer.from(entry.score.toString()))
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
      zset.members.set(hex, { member: args.member, score })
      return score
    })
    return bulk(Buffer.from(newScore.toString()))
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
        items.push(RedisValue.bulkString(Buffer.from(entry.score.toString())))
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
        items.push(RedisValue.bulkString(Buffer.from(entry.score.toString())))
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
    const min = parseFloatArg(args.min)
    const max = parseFloatArg(args.max)
    const zset = ctx.db.getSortedSet(args.key)
    if (!zset) return array([])
    const items: RedisValue[] = []
    for (const entry of getSortedMembers(zset)) {
      if (entry.score >= min && entry.score <= max) {
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
    const min = parseFloatArg(args.min)
    const max = parseFloatArg(args.max)
    let removed = 0
    ctx.db.updateSortedSet(args.key, zset => {
      for (const [hex, entry] of zset.members) {
        if (entry.score >= min && entry.score <= max) {
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
    const min = parseFloatArg(args.min)
    const max = parseFloatArg(args.max)
    const zset = ctx.db.getSortedSet(args.key)
    if (!zset) return integer(0)
    let count = 0
    for (const entry of zset.members.values()) {
      if (entry.score >= min && entry.score <= max) count++
    }
    return integer(count)
  },
})

export const zpopminCommand = defineCommand({
  name: 'zpopmin',
  schema: t.object({ key: t.key(), count: t.optional(t.integer()) }),
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
      items.push(RedisValue.bulkString(Buffer.from(entry.score.toString())))
    }
    return array(items)
  },
})

export const zpopmaxCommand = defineCommand({
  name: 'zpopmax',
  schema: t.object({ key: t.key(), count: t.optional(t.integer()) }),
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
      items.push(RedisValue.bulkString(Buffer.from(entry.score.toString())))
    }
    return array(items)
  },
})

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
  zpopminCommand,
  zpopmaxCommand,
]
