import { defineCommand } from '../../core/command-definition'
import { t } from '../../core/command-schema'
import { RedisValue } from '../../core/redis-value'
import { array, integer } from '../helpers'
import { deleteSortedSetIfEmpty } from './helpers'
import {
  applyLexLimit,
  createLexRangeSchema,
  getLexSortedMembers,
  lexMemberWithinBounds,
  parseLexBoundArg,
} from './lex'

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
    if (removed > 0) deleteSortedSetIfEmpty(ctx.db, args.key)
    return integer(removed)
  },
})
