import { defineCommand } from '../../core/command-definition'
import { t } from '../../core/command-schema'
import { RedisValue } from '../../core/redis-value'
import { array, bulk, scoreBuffer } from '../helpers'

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

export const zmscoreCommand = defineCommand({
  name: 'zmscore',
  schema: t.object({ key: t.key(), members: t.variadic(t.key(), { min: 1 }) }),
  flags: ['readonly', 'fast'],
  keys: args => [args.key],
  execute: (args, ctx) => {
    const zset = ctx.db.getSortedSet(args.key)
    const items: RedisValue[] = []
    for (const member of args.members) {
      const entry = zset?.members.get(member.toString('hex'))
      items.push(RedisValue.bulkString(entry ? scoreBuffer(entry.score) : null))
    }
    return array(items)
  },
})
