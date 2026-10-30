import type { CompatibilityProfile } from '../../core/compatibility/profile'
import { parseScoreRangeToken } from '../../core/double-token'
import { errors } from '../../core/redis-error'

export type ScoreBound = { value: number; exclusive: boolean }

/**
 * A ZRANGEBYSCORE / ZCOUNT / ZRANGE BYSCORE / ZREMRANGEBYSCORE bound, read
 * like `zslParseRange()`: an optional `(`, then the converter of the profile
 * (an empty rest is 0, `1e400` is infinity).
 */
export function parseScoreBoundArg(
  token: Buffer | string,
  profile: CompatibilityProfile,
): ScoreBound {
  const bytes = typeof token === 'string' ? Buffer.from(token) : token
  const exclusive = bytes[0] === 0x28 // '('
  const value = parseScoreRangeToken(
    exclusive ? bytes.subarray(1) : bytes,
    profile,
  )
  if (value === undefined) throw errors.minMaxNotFloat()
  return { value, exclusive }
}

export function scoreWithinBounds(
  score: number,
  min: ScoreBound,
  max: ScoreBound,
) {
  if (min.exclusive ? score <= min.value : score < min.value) return false
  if (max.exclusive ? score >= max.value : score > max.value) return false
  return true
}
