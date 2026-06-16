import {
  ExpectedFloatError,
  ResultingScoreNaNError,
} from '../../core/redis-error'
import type { RedisDatabase } from '../../state/database'
import type {
  RedisSortedSetData,
  RedisSortedSetMember,
} from '../../state/data-types'

export function getSortedMembers(
  zset: RedisSortedSetData,
): RedisSortedSetMember[] {
  return Array.from(zset.members.values()).sort((a, b) =>
    a.score !== b.score
      ? a.score - b.score
      : a.member.toString().localeCompare(b.member.toString()),
  )
}

export function parseFloatArg(s: string): number {
  const normalized = s.toLowerCase()
  if (normalized === 'inf' || normalized === '+inf') return Infinity
  if (normalized === '-inf') return -Infinity

  const n = Number(s)
  if (!Number.isFinite(n)) throw new ExpectedFloatError()
  return n
}

export function assertValidResultingScore(score: number) {
  if (Number.isNaN(score)) {
    throw new ResultingScoreNaNError()
  }
}

export function deleteSortedSetIfEmpty(db: RedisDatabase, key: Buffer) {
  if ((db.getSortedSet(key)?.members.size ?? 0) === 0) {
    db.delete(key)
  }
}
