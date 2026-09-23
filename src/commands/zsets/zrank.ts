import {
  defineCommand,
  type CommandIntrospection,
} from '../../core/command-definition'
import { t } from '../../core/command-schema'
import { WrongNumberOfArgumentsError, errors } from '../../core/redis-error'
import { RedisResult } from '../../core/redis-result'
import { RedisValue } from '../../core/redis-value'
import { array, integer, scoreValue } from '../helpers'
import { getSortedMembers } from './helpers'

type ZRankArgs = {
  key: Buffer
  member: Buffer
  withScore: boolean
}

const zrankLayout = { min: 2, max: 3, keys: [0] }
const zrankSchema = t.custom<ZRankArgs>(zrankLayout, (input, index, ctx) => {
  const remaining = input.length - index
  const maxArgs = ctx.profile.has('zrank.withscore') ? 3 : 2
  if (remaining < 2 || remaining > maxArgs) {
    throw new WrongNumberOfArgumentsError(ctx.commandName)
  }

  const key = input[index]
  const member = input[index + 1]
  if (!key || !member) {
    throw new WrongNumberOfArgumentsError(ctx.commandName)
  }

  const option = input[index + 2]
  if (option && option.toString().toUpperCase() !== 'WITHSCORE') {
    throw errors.syntax()
  }

  return {
    value: { key, member, withScore: option !== undefined },
    nextIndex: input.length,
  }
})

// WITHSCORE arrived in 7.2; before it the parser above takes exactly
// `key member` and Redis reported arity 3.
const zrankIntrospection: CommandIntrospection = {
  arity: profile => (profile.has('zrank.withscore') ? -3 : 3),
}

function rankResponse(rank: number, score: number, withScore: boolean) {
  if (!withScore) {
    return integer(rank)
  }

  return array([RedisValue.integer(rank), scoreValue(score)])
}

export const zrankCommand = defineCommand({
  name: 'zrank',
  schema: zrankSchema,
  flags: ['readonly', 'fast'],
  introspection: zrankIntrospection,
  keys: args => [args.key],
  execute: (args, ctx) => {
    const zset = ctx.db.getSortedSet(args.key)
    if (!zset) return RedisResult.nil()
    const hex = args.member.toString('hex')
    const entry = zset.members.get(hex)
    if (!entry) return RedisResult.nil()
    const sorted = getSortedMembers(zset)
    const rank = sorted.findIndex(m => m.member.toString('hex') === hex)
    return rankResponse(rank, entry.score, args.withScore)
  },
})

export const zrevrankCommand = defineCommand({
  name: 'zrevrank',
  schema: zrankSchema,
  flags: ['readonly', 'fast'],
  introspection: zrankIntrospection,
  keys: args => [args.key],
  execute: (args, ctx) => {
    const zset = ctx.db.getSortedSet(args.key)
    if (!zset) return RedisResult.nil()
    const hex = args.member.toString('hex')
    const entry = zset.members.get(hex)
    if (!entry) return RedisResult.nil()
    const sorted = getSortedMembers(zset)
    const rank = sorted
      .slice()
      .reverse()
      .findIndex(m => m.member.toString('hex') === hex)
    return rankResponse(rank, entry.score, args.withScore)
  },
})
