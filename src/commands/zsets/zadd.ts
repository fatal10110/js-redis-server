import { asciiUpperCase } from '../../core/ascii-case'
import { defineCommand } from '../../core/command-definition'
import { t, type ParseContext } from '../../core/command-schema'
import { WrongNumberOfArgumentsError, errors } from '../../core/redis-error'
import type { RedisSortedSetMember } from '../../state/data-types'
import { RedisResult } from '../../core/redis-result'
import { bulk, integer, scoreValue } from '../helpers'
import { assertValidResultingScore, parseFloatArg } from './helpers'

type ZaddPair = { score: number; member: Buffer }
type ZaddCondition = 'NX' | 'XX'
type ZaddComparison = 'GT' | 'LT'

type ZaddOptions = {
  condition?: ZaddCondition
  comparison?: ZaddComparison
  ch: boolean
  incr: boolean
}

type ZaddArgs = {
  key: Buffer
  options: ZaddOptions
  pairs: ZaddPair[]
}

function createZaddSchema() {
  return t.custom<ZaddArgs>(
    { min: 3, keys: [0] },
    (input: readonly Buffer[], index: number, ctx: ParseContext) => {
      const key = input[index]
      if (!key) {
        throw new WrongNumberOfArgumentsError(ctx.commandName)
      }

      const options: ZaddOptions = { ch: false, incr: false }
      let nx = false
      let xx = false
      let gt = false
      let lt = false
      let cursor = index + 1

      while (cursor < input.length) {
        const option = asciiUpperCase(input[cursor]!.toString())
        if (option === 'NX') nx = true
        else if (option === 'XX') xx = true
        else if (option === 'GT') gt = true
        else if (option === 'LT') lt = true
        else if (option === 'CH') options.ch = true
        else if (option === 'INCR') options.incr = true
        else break
        cursor++
      }

      // Real Redis (zaddGenericCommand) checks the score/member parity before
      // the option conflicts and before parsing any score.
      const elements = input.length - cursor
      if (elements === 0 || elements % 2 !== 0) {
        throw errors.syntax()
      }
      if (nx && xx) throw errors.zaddNxXxConflict()
      if ((gt && nx) || (lt && nx) || (gt && lt)) {
        throw errors.zaddGtLtNxConflict()
      }
      if (options.incr && elements > 2) throw errors.zaddIncrPair()

      if (nx) options.condition = 'NX'
      else if (xx) options.condition = 'XX'
      if (gt) options.comparison = 'GT'
      else if (lt) options.comparison = 'LT'

      const pairs: ZaddPair[] = []
      for (; cursor < input.length; cursor += 2) {
        const score = parseFloatArg(input[cursor]!, ctx.profile)
        pairs.push({ score, member: input[cursor + 1]! })
      }

      return { value: { key, options, pairs }, nextIndex: input.length }
    },
  )
}

function shouldApplyZaddUpdate(
  existing: RedisSortedSetMember | undefined,
  score: number,
  options: ZaddOptions,
): boolean {
  if (!existing) return options.condition !== 'XX'
  if (options.condition === 'NX') return false
  if (options.comparison === 'GT') return score > existing.score
  if (options.comparison === 'LT') return score < existing.score
  return true
}

export const zaddCommand = defineCommand({
  name: 'zadd',
  schema: createZaddSchema(),
  flags: ['write', 'denyoom', 'fast'],
  keys: args => [args.key],
  execute: (args, ctx) => {
    if (args.options.incr) {
      const [{ score, member }] = args.pairs
      const newScore = ctx.db.updateSortedSet(args.key, zset => {
        const existing = zset.getMember(member)
        const nextScore = (existing?.score ?? 0) + score
        assertValidResultingScore(nextScore)

        if (!shouldApplyZaddUpdate(existing, nextScore, args.options)) {
          return null
        }

        zset.setScore(member, nextScore)
        return nextScore
      })
      return newScore === null
        ? bulk(null)
        : RedisResult.create(scoreValue(newScore))
    }

    const replyCount = ctx.db.updateSortedSet(args.key, zset => {
      let count = 0
      for (const { score, member } of args.pairs) {
        const existing = zset.getMember(member)

        if (!shouldApplyZaddUpdate(existing, score, args.options)) {
          continue
        }

        if (!existing || args.options.ch) {
          if (!existing || existing.score !== score) count++
        }

        zset.setScore(member, score)
      }
      return count
    })
    return integer(replyCount)
  },
})
