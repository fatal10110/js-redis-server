import { t } from '../../core/command-schema'
import {
  ExpectedIntegerError,
  InvalidLexRangeError,
  RedisSyntaxError,
  WrongNumberOfArgumentsError,
} from '../../core/redis-error'
import type {
  RedisSortedSetData,
  RedisSortedSetMember,
} from '../../state/data-types'

// Lexicographic range bounds — used by ZRANGEBYLEX/ZREVRANGEBYLEX/ZLEXCOUNT/
// ZREMRANGEBYLEX. All members are assumed to share the same score, so ordering
// is by raw byte comparison (not localeCompare — see #41).
export type LexBound =
  | { kind: 'min' } // `-` negative infinity
  | { kind: 'max' } // `+` positive infinity
  | { kind: 'value'; value: Buffer; exclusive: boolean }

export function parseLexBoundArg(token: Buffer): LexBound {
  if (token.length === 1) {
    if (token[0] === 0x2d /* - */) return { kind: 'min' }
    if (token[0] === 0x2b /* + */) return { kind: 'max' }
  }

  const prefix = token[0]
  if (prefix === 0x5b /* [ */) {
    return { kind: 'value', value: token.subarray(1), exclusive: false }
  }
  if (prefix === 0x28 /* ( */) {
    return { kind: 'value', value: token.subarray(1), exclusive: true }
  }

  throw new InvalidLexRangeError()
}

export function lexMemberWithinBounds(
  member: Buffer,
  min: LexBound,
  max: LexBound,
): boolean {
  if (min.kind === 'max') return false
  if (min.kind === 'value') {
    const cmp = Buffer.compare(member, min.value)
    if (min.exclusive ? cmp <= 0 : cmp < 0) return false
  }

  if (max.kind === 'min') return false
  if (max.kind === 'value') {
    const cmp = Buffer.compare(member, max.value)
    if (max.exclusive ? cmp >= 0 : cmp > 0) return false
  }

  return true
}

export function getLexSortedMembers(
  zset: RedisSortedSetData,
): RedisSortedSetMember[] {
  return Array.from(zset.members.values()).sort((a, b) =>
    a.score !== b.score
      ? a.score - b.score
      : Buffer.compare(a.member, b.member),
  )
}

export type LexLimit = { offset: number; count: number }

export type LexRangeArgs = {
  key: Buffer
  first: Buffer
  second: Buffer
  limit?: LexLimit
}

export function parseLexLimitInt(token: Buffer): number {
  const raw = token.toString()
  if (!/^-?\d+$/.test(raw)) throw new ExpectedIntegerError()
  const value = Number(raw)
  if (!Number.isSafeInteger(value)) throw new ExpectedIntegerError()
  return value
}

// ZRANGEBYLEX key min max [LIMIT offset count]
// ZREVRANGEBYLEX key max min [LIMIT offset count] — caller maps first/second.
export function createLexRangeSchema() {
  return t.custom<LexRangeArgs>((input, index, ctx) => {
    const key = input[index]
    const first = input[index + 1]
    const second = input[index + 2]
    if (!key || !first || !second) {
      throw new WrongNumberOfArgumentsError(ctx.commandName)
    }

    let cursor = index + 3
    let limit: LexLimit | undefined
    if (cursor < input.length) {
      if (input[cursor]!.toString().toUpperCase() !== 'LIMIT') {
        throw new RedisSyntaxError()
      }
      const offsetTok = input[cursor + 1]
      const countTok = input[cursor + 2]
      if (!offsetTok || !countTok) throw new RedisSyntaxError()
      limit = {
        offset: parseLexLimitInt(offsetTok),
        count: parseLexLimitInt(countTok),
      }
      cursor += 3
      if (cursor < input.length) throw new RedisSyntaxError()
    }

    return { value: { key, first, second, limit }, nextIndex: cursor }
  })
}

export function applyLexLimit(
  members: RedisSortedSetMember[],
  limit: LexLimit | undefined,
): RedisSortedSetMember[] {
  if (!limit) return members
  if (limit.offset < 0) return []
  const end = limit.count < 0 ? members.length : limit.offset + limit.count
  return members.slice(limit.offset, end)
}
