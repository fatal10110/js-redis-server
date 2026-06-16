import { defineCommand } from '../../core/command-definition'
import { t } from '../../core/command-schema'
import {
  LposCountNegativeError,
  LposMaxlenNegativeError,
  LposRankZeroError,
  RedisSyntaxError,
  WrongNumberOfArgumentsError,
} from '../../core/redis-error'
import { RedisValue } from '../../core/redis-value'
import { array, bulk, integer, parseIntegerToken } from '../helpers'

type LposArgs = {
  key: Buffer
  element: Buffer
  rank: number
  count?: number
  maxlen: number
}

export const lposCommand = defineCommand({
  name: 'lpos',
  schema: t.custom<LposArgs>((input, index, ctx) => {
    const key = input[index]
    const element = input[index + 1]
    if (!key || !element) {
      throw new WrongNumberOfArgumentsError(ctx.commandName)
    }

    let rank = 1
    let count: number | undefined
    let maxlen = 0
    let cursor = index + 2

    while (cursor < input.length) {
      const option = input[cursor].toString().toUpperCase()
      const valueToken = input[cursor + 1]
      if (!valueToken) {
        throw new RedisSyntaxError()
      }

      if (option === 'RANK') {
        rank = parseIntegerToken(valueToken)
        if (rank === 0) throw new LposRankZeroError()
      } else if (option === 'COUNT') {
        count = parseIntegerToken(valueToken)
        if (count < 0) throw new LposCountNegativeError()
      } else if (option === 'MAXLEN') {
        maxlen = parseIntegerToken(valueToken)
        if (maxlen < 0) throw new LposMaxlenNegativeError()
      } else {
        throw new RedisSyntaxError()
      }

      cursor += 2
    }

    return { value: { key, element, rank, count, maxlen }, nextIndex: cursor }
  }),
  flags: ['readonly'],
  keys: args => [args.key],
  execute: (args, ctx) => {
    const hasCount = args.count !== undefined
    const list = ctx.db.getList(args.key)
    if (!list || list.values.length === 0) {
      return hasCount ? array([]) : bulk(null)
    }

    const target = args.element.toString('binary')
    const forward = args.rank > 0
    const step = forward ? 1 : -1
    const len = list.values.length
    // count===0 means "return all matches"; absent means "return first only"
    const limit = hasCount ? (args.count === 0 ? Infinity : args.count!) : 1

    const results: number[] = []
    let toSkip = Math.abs(args.rank) - 1
    let comparisons = 0

    for (let i = forward ? 0 : len - 1; i >= 0 && i < len; i += step) {
      if (args.maxlen !== 0 && comparisons >= args.maxlen) break
      comparisons++
      if (list.values[i].toString('binary') !== target) continue

      if (toSkip > 0) {
        toSkip--
        continue
      }
      results.push(i)
      if (results.length >= limit) break
    }

    if (!hasCount) {
      return results.length === 0 ? bulk(null) : integer(results[0])
    }
    return array(results.map(idx => RedisValue.integer(idx)))
  },
})
