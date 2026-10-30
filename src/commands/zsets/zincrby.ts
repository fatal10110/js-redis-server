import { defineCommand } from '../../core/command-definition'
import { t } from '../../core/command-schema'
import { RedisResult } from '../../core/redis-result'
import { scoreValue } from '../helpers'
import {
  assertValidResultingScore,
  parseFloatArg,
  zsetEncodingRules,
} from './helpers'

export const zincrbyCommand = defineCommand({
  name: 'zincrby',
  schema: t.object({ key: t.key(), increment: t.string(), member: t.bulk() }),
  flags: ['write', 'fast'],
  keys: args => [args.key],
  execute: (args, ctx) => {
    const inc = parseFloatArg(args.increment, ctx.server.profile)
    const rules = zsetEncodingRules(ctx.server)
    const newScore = ctx.db.updateSortedSet(args.key, zset => {
      zset.prepareForAdd([args.member], rules)
      const existing = zset.getMember(args.member)
      // A new member takes the increment as is (zsetAdd): ZINCRBY -0 is -0.
      const score = existing ? existing.score + inc : inc
      assertValidResultingScore(score)
      // An equal score is no change (zsetAdd), so a stored -0 stays.
      if (!existing || existing.score !== score) {
        zset.addScore(args.member, score, rules)
      }
      return score
    })
    return RedisResult.create(scoreValue(newScore))
  },
})
