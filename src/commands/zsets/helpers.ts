import type { CompatibilityProfile } from '../../core/compatibility/profile'
import { parseString2dToken } from '../../core/double-token'
import { errors } from '../../core/redis-error'
import type {
  RedisSortedSetData,
  RedisSortedSetMember,
} from '../../state/data-types'
import type { ZsetEncodingRules } from '../../state/zset-encoding'
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

/** The server settings that decide a sorted set's encoding (#512). */
export function zsetEncodingRules(
  server: ZsetEncodingServer,
): ZsetEncodingRules {
  const { maxEntries, maxValue } = configuredZsetListpackLimits(server)
  return {
    maxEntries,
    maxValue,
    sizeHint: server.profile.has('zset.add-size-hint'),
    longestMemberHint: server.profile.has('zset.add-longest-member-hint'),
    integerScores: server.profile.has('zset.listpack-integer-scores'),
    unknownStoreLengthSkiplist:
      server.profile.has('zset.rangestore-unknown-length-skiplist') &&
      !server.profile.has('zset.rangestore-unknown-length-listpack'),
  }
}
