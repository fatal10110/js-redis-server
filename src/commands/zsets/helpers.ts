import type { CompatibilityProfile } from '../../core/compatibility/profile'
import { parseString2dToken } from '../../core/double-token'
import { errors } from '../../core/redis-error'
import type {
  RedisSortedSetData,
  RedisSortedSetMember,
} from '../../state/data-types'
import type { TrackedSortedSetData } from '../../state/tracked-values'
import { configuredZsetListpackLimits } from '../config'

export function getSortedMembers(
  zset: RedisSortedSetData,
): RedisSortedSetMember[] {
  return Array.from(zset.members.values()).sort((a, b) =>
    a.score !== b.score
      ? a.score - b.score
      : // Equal scores tie-break by raw byte order (memcmp), matching Redis —
        // not localeCompare, which would mis-sort non-ASCII members (see #41).
        Buffer.compare(a.member, b.member),
  )
}

/** A ZADD / ZINCRBY score, read like Redis' `string2d()`. */
export function parseFloatArg(
  token: Buffer | string,
  profile: CompatibilityProfile,
): number {
  const value = parseString2dToken(token, profile)
  if (value === undefined) throw errors.expectedFloat()
  return value
}

export function assertValidResultingScore(score: number) {
  if (Number.isNaN(score)) {
    throw errors.resultingScoreNaN()
  }
}

type ZsetEncodingServer = { readonly profile: CompatibilityProfile }

/**
 * Whether a sorted set of `size` members is a listpack. The mock does not
 * track the encoding: a set counts as one while it fits
 * `zset-max-listpack-entries` and every member fits
 * `zset-max-listpack-value`, as zsetTypeCreate() decides for a new one.
 */
function fitsListpack(
  size: number,
  members: Iterable<RedisSortedSetMember>,
  server: ZsetEncodingServer,
): boolean {
  const { maxEntries, maxValue } = configuredZsetListpackLimits(server)
  if (size > maxEntries) return false
  for (const entry of members) {
    if (entry.member.length > maxValue) return false
  }
  return true
}

/**
 * Store the scores just written as a listpack would (#512). From Redis 7.0
 * a listpack keeps an integral score as an integer, so -0 reads back as 0;
 * 6.2's ziplist and every skiplist keep -0 (`zset.listpack-integer-scores`).
 * The reply of the write itself (ZADD INCR, ZINCRBY) still carries -0.
 */
export function storeListpackScores(
  zset: TrackedSortedSetData,
  written: readonly Buffer[],
  server: ZsetEncodingServer,
): void {
  if (
    written.length === 0 ||
    !server.profile.has('zset.listpack-integer-scores') ||
    !fitsListpack(zset.size, membersOf(zset), server)
  ) {
    return
  }

  for (const member of written) {
    if (Object.is(zset.getMember(member)?.score, -0)) {
      zset.setScore(member, 0)
    }
  }
}

/** {@link storeListpackScores} for a set a STORE command writes whole. */
export function listpackScores(
  members: Map<string, RedisSortedSetMember>,
  server: ZsetEncodingServer,
): Map<string, RedisSortedSetMember> {
  if (
    !server.profile.has('zset.listpack-integer-scores') ||
    !fitsListpack(members.size, members.values(), server)
  ) {
    return members
  }

  for (const [hex, entry] of members) {
    if (Object.is(entry.score, -0)) {
      members.set(hex, { member: entry.member, score: 0 })
    }
  }
  return members
}

function* membersOf(
  zset: TrackedSortedSetData,
): Iterable<RedisSortedSetMember> {
  for (const [, entry] of zset.entries()) yield entry
}
