import { defineCommand } from '../../core/command-definition'
import { t } from '../../core/command-schema'
import { RedisResult } from '../../core/redis-result'
import { integer } from '../helpers'
import { getSortedMembers } from './helpers'

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
