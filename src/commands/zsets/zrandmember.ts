import { defineCommand } from '../../core/command-definition'
import { t } from '../../core/command-schema'
import {
  RedisSyntaxError,
  WrongNumberOfArgumentsError,
} from '../../core/redis-error'
import { RedisValue } from '../../core/redis-value'
import type { RedisSortedSetMember } from '../../state/data-types'
import { array, bulk, scoreBuffer } from '../helpers'
import { parseLexLimitInt } from './lex'

type ZrandmemberArgs = {
  key: Buffer
  count?: number
  withScores: boolean
}

function createZrandmemberSchema() {
  return t.custom<ZrandmemberArgs>((input, index, ctx) => {
    const key = input[index]
    if (!key) throw new WrongNumberOfArgumentsError(ctx.commandName)

    let cursor = index + 1
    let count: number | undefined
    let withScores = false

    if (cursor < input.length) {
      count = parseLexLimitInt(input[cursor]!)
      cursor++
      if (cursor < input.length) {
        if (input[cursor]!.toString().toUpperCase() !== 'WITHSCORES') {
          throw new RedisSyntaxError()
        }
        withScores = true
        cursor++
      }
    }

    if (cursor < input.length) throw new RedisSyntaxError()
    return { value: { key, count, withScores }, nextIndex: input.length }
  })
}

export const zrandmemberCommand = defineCommand({
  name: 'zrandmember',
  schema: createZrandmemberSchema(),
  flags: ['readonly'],
  keys: args => [args.key],
  execute: (args, ctx) => {
    const zset = ctx.db.getSortedSet(args.key)

    if (args.count === undefined) {
      if (!zset || zset.members.size === 0) return bulk(null)
      const all = Array.from(zset.members.values())
      const pick = all[Math.floor(Math.random() * all.length)]
      return bulk(pick.member)
    }

    if (!zset || zset.members.size === 0) return array([])
    const all = Array.from(zset.members.values())
    const chosen: RedisSortedSetMember[] = []

    if (args.count < 0) {
      const n = -args.count
      for (let i = 0; i < n; i++) {
        chosen.push(all[Math.floor(Math.random() * all.length)])
      }
    } else {
      const shuffled = all.slice()
      for (let i = shuffled.length - 1; i > 0; i--) {
        const j = Math.floor(Math.random() * (i + 1))
        ;[shuffled[i], shuffled[j]] = [shuffled[j], shuffled[i]]
      }
      chosen.push(...shuffled.slice(0, Math.min(args.count, shuffled.length)))
    }

    const items: RedisValue[] = []
    for (const entry of chosen) {
      items.push(RedisValue.bulkString(entry.member))
      if (args.withScores) {
        items.push(RedisValue.bulkString(scoreBuffer(entry.score)))
      }
    }
    return array(items)
  },
})
