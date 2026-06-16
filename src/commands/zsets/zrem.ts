import { defineCommand } from '../../core/command-definition'
import { t } from '../../core/command-schema'
import { integer } from '../helpers'

export const zremCommand = defineCommand({
  name: 'zrem',
  schema: t.object({ key: t.key(), members: t.variadic(t.key(), { min: 1 }) }),
  flags: ['write', 'fast'],
  keys: args => [args.key],
  execute: (args, ctx) => {
    let removed = 0
    ctx.db.updateSortedSet(args.key, zset => {
      for (const member of args.members) {
        if (zset.deleteMember(member)) removed++
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
