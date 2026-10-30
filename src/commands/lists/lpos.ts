import { asciiUpperCase } from '../../core/ascii-case'
import { defineCommand } from '../../core/command-definition'
import { t } from '../../core/command-schema'
import { WrongNumberOfArgumentsError, errors } from '../../core/redis-error'
import { RedisValue } from '../../core/redis-value'
import type { CompatibilityProfile } from '../../core/compatibility/profile'
import { INT64_MIN, array, bulk, integer, parseInt64Token } from '../helpers'

const MAX_SAFE = BigInt(Number.MAX_SAFE_INTEGER)

/**
 * An int64 option value (`getLongFromObjectOrReply`), clamped to the safe
 * integer range: no list is long enough to tell the difference.
 */
function parseLposInt(token: Buffer): number {
  const value = parseInt64Token(token)
  if (value > MAX_SAFE) return Number.MAX_SAFE_INTEGER
  if (value < -MAX_SAFE) return -Number.MAX_SAFE_INTEGER
  return Number(value)
}

/**
 * RANK -9223372036854775808 is out of range from 7.2 (`lpos.rank-range-check`);
 * before that LPOS negates it into itself, still negative, and the first match
 * from the tail wins: RANK -1.
 */
function parseLposRank(token: Buffer, profile: CompatibilityProfile): number {
  if (parseInt64Token(token) === INT64_MIN) {
    if (profile.has('lpos.rank-range-check')) {
      throw errors.lposRankOutOfRange()
    }
    return -1
  }
  return parseLposInt(token)
}

type LposArgs = {
  key: Buffer
  element: Buffer
  rank: number
  count?: number
  maxlen: number
}

export const lposCommand = defineCommand({
  name: 'lpos',
  schema: t.custom<LposArgs>({ min: 2, keys: [0] }, (input, index, ctx) => {
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
      const option = asciiUpperCase(input[cursor].toString())
      const valueToken = input[cursor + 1]
      if (!valueToken) {
        throw errors.syntax()
      }

      if (option === 'RANK') {
        rank = parseLposRank(valueToken, ctx.profile)
        if (rank === 0) throw errors.lposRankZero(ctx.profile)
      } else if (option === 'COUNT') {
        count = parseLposInt(valueToken)
        if (count < 0) throw errors.lposCountNegative()
      } else if (option === 'MAXLEN') {
        maxlen = parseLposInt(valueToken)
        if (maxlen < 0) throw errors.lposMaxlenNegative()
      } else {
        throw errors.syntax()
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
