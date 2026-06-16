import { defineCommand } from '../../core/command-definition'
import { t } from '../../core/command-schema'
import { RedisValue } from '../../core/redis-value'
import { array, integer } from '../helpers'
import { getSortedMembers, deleteSortedSetIfEmpty } from './helpers'
import { parseScoreBoundArg, scoreWithinBounds } from './score'

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
