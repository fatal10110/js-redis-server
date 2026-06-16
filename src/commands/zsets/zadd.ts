import { defineCommand } from '../../core/command-definition'
import { t, type ParseContext } from '../../core/command-schema'
import {
  WrongNumberOfArgumentsError,
  ZaddGtLtNxConflictError,
  ZaddIncrPairError,
  ZaddNxXxConflictError,
} from '../../core/redis-error'
import type { RedisSortedSetMember } from '../../state/data-types'
import { bulk, integer, scoreBuffer } from '../helpers'
import {
  assertValidResultingScore,
  deleteSortedSetIfEmpty,
  parseFloatArg,
} from './helpers'

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
    (input: readonly Buffer[], index: number, ctx: ParseContext) => {
      const key = input[index]
      if (!key) {
        throw new WrongNumberOfArgumentsError(ctx.commandName)
      }

      const options: ZaddOptions = { ch: false, incr: false }
      let cursor = index + 1

      while (cursor < input.length) {
        const option = input[cursor]!.toString().toUpperCase()

        if (option === 'NX') {
          if (options.condition === 'XX') throw new ZaddNxXxConflictError()
          options.condition = 'NX'
          cursor++
          continue
        }

        if (option === 'XX') {
          if (options.condition === 'NX') throw new ZaddNxXxConflictError()
          options.condition = 'XX'
          cursor++
          continue
        }

        if (option === 'GT') {
          if (options.comparison === 'LT') {
            throw new ZaddGtLtNxConflictError()
          }
          options.comparison = 'GT'
          cursor++
          continue
        }

        if (option === 'LT') {
          if (options.comparison === 'GT') {
            throw new ZaddGtLtNxConflictError()
          }
          options.comparison = 'LT'
          cursor++
          continue
        }

        if (option === 'CH') {
          options.ch = true
          cursor++
          continue
        }

        if (option === 'INCR') {
          options.incr = true
          cursor++
          continue
        }

        break
      }

      if (options.condition === 'NX' && options.comparison) {
        throw new ZaddGtLtNxConflictError()
      }

      const pairs = parseZaddPairs(input, cursor, ctx)
      if (options.incr && pairs.length !== 1) {
        throw new ZaddIncrPairError()
      }

      return { value: { key, options, pairs }, nextIndex: input.length }
    },
  )
}

function parseZaddPairs(
  input: readonly Buffer[],
  index: number,
  ctx: ParseContext,
): ZaddPair[] {
  const pairs: ZaddPair[] = []
  let cursor = index

  if (cursor >= input.length) {
    throw new WrongNumberOfArgumentsError(ctx.commandName)
  }

  while (cursor < input.length) {
    const scoreToken = input[cursor]
    const memberToken = input[cursor + 1]
    if (!scoreToken || !memberToken) {
      throw new WrongNumberOfArgumentsError(ctx.commandName)
    }

    const score = parseFloatArg(scoreToken.toString())

    pairs.push({ score, member: memberToken })
    cursor += 2
  }

  return pairs
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
      deleteSortedSetIfEmpty(ctx.db, args.key)
      return bulk(newScore === null ? null : scoreBuffer(newScore))
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
    deleteSortedSetIfEmpty(ctx.db, args.key)
    return integer(replyCount)
  },
})
