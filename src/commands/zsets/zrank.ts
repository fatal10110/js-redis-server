import { defineCommand } from '../../core/command-definition'
import { t } from '../../core/command-schema'
import {
  RedisSyntaxError,
  WrongNumberOfArgumentsError,
} from '../../core/redis-error'
import { RedisResult } from '../../core/redis-result'
import { RedisValue } from '../../core/redis-value'
import { array, integer, scoreBuffer } from '../helpers'
import { getSortedMembers } from './helpers'

type ZRankArgs = {
  key: Buffer
  member: Buffer
  withScore: boolean
}

const zrankSchema = t.custom<ZRankArgs>((input, index, ctx) => {
  const remaining = input.length - index
  if (remaining < 2 || remaining > 3) {
    throw new WrongNumberOfArgumentsError(ctx.commandName)
  }

  const key = input[index]
  const member = input[index + 1]
  if (!key || !member) {
    throw new WrongNumberOfArgumentsError(ctx.commandName)
  }

  const option = input[index + 2]
  if (option && option.toString().toUpperCase() !== 'WITHSCORE') {
    throw new RedisSyntaxError()
  }

  return {
    value: { key, member, withScore: option !== undefined },
    nextIndex: input.length,
  }
})

function rankResponse(rank: number, score: number, withScore: boolean) {
  if (!withScore) {
    return integer(rank)
  }

  return array([
    RedisValue.integer(rank),
    RedisValue.bulkString(scoreBuffer(score)),
  ])
}

export const zrankCommand = defineCommand({
  name: 'zrank',
  schema: zrankSchema,
  flags: ['readonly', 'fast'],
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
