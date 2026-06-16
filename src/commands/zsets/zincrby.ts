import { defineCommand } from '../../core/command-definition'
import { t } from '../../core/command-schema'
import { bulk, scoreBuffer } from '../helpers'
import { assertValidResultingScore, parseFloatArg } from './helpers'

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
