import { RedisValue } from '../core/redis-value'
import { RedisResult } from '../core/redis-result'
import { isIntegerToken } from '../core/command-schema'
import {
  ExpectedIntegerError,
  InvalidExpireTimeError,
  RedisCommandError,
  RedisSyntaxError,
  WrongTypeRedisError,
} from '../core/redis-error'
import type { CompatibilityProfile } from '../core/compatibility'
import type { RedisDataTypeName, RedisDatabase } from '../state'

export function ok(): RedisResult {
  return RedisResult.ok()
}

export function bulk(value: Buffer | null): RedisResult {
  return RedisResult.create(RedisValue.bulkString(value))
}

export function integer(value: number | bigint): RedisResult {
  return RedisResult.create(RedisValue.integer(value))
}

export function scoreBuffer(score: number): Buffer {
  if (score === Infinity) return Buffer.from('inf')
  if (score === -Infinity) return Buffer.from('-inf')
  return Buffer.from(score.toString())
}

// A sorted-set score reply. Protocol-aware: a bulk string on RESP2 (matching
// scoreBuffer) and a `,double` on RESP3 — the shape real Redis uses for
// ZSCORE/ZINCRBY/WITHSCORES scores. `-0` is normalized to `0` like Redis.
export function scoreValue(score: number): RedisValue {
  return RedisValue.double(Object.is(score, -0) ? 0 : score)
}

// member/score pairs for WITHSCORES-style replies: flat [m, s, ...] on RESP2,
// nested [[m, s], ...] on RESP3, with scores as RESP3 doubles.
export function scorePairs(
  members: readonly { member: Buffer; score: number }[],
): RedisValue {
  return RedisValue.flatPairs(
    members.map(entry => [
      RedisValue.bulkString(entry.member),
      scoreValue(entry.score),
    ]),
  )
}

export function simpleString(value: string): RedisResult {
  return RedisResult.create(RedisValue.simpleString(value))
}

export function array(items: RedisValue[]): RedisResult {
  return RedisResult.create(RedisValue.array(items))
}

export function ensureStringOrMissing(
  db: RedisDatabase,
  key: Buffer,
): Buffer | null {
  const type = db.getType(key)
  if (type === null) {
    return null
  }

  if (type !== 'string') {
    throw new WrongTypeRedisError()
  }

  return db.getString(key)
}

export function typeName(type: RedisDataTypeName | null): string {
  return type ?? 'none'
}

export function ttlSeconds(expiresAt: number): number {
  // Redis rounds remaining time to the nearest second ((ms+500)/1000),
  // not ceil/floor — matches EXPIRETIME and real TTL behavior.
  return Math.max(0, Math.round((expiresAt - Date.now()) / 1000))
}

export function ttlMilliseconds(expiresAt: number): number {
  return Math.max(0, expiresAt - Date.now())
}

export function parseIntegerToken(token: Buffer): number {
  const raw = token.toString()
  if (!isIntegerToken(raw)) {
    throw new ExpectedIntegerError()
  }

  const value = Number(raw)
  if (!Number.isSafeInteger(value)) {
    throw new ExpectedIntegerError()
  }

  return value
}

export const INT64_MAX = 9223372036854775807n
export const INT64_MIN = -9223372036854775808n

// Parse a stored token as a signed 64-bit integer, matching Redis' int64
// semantics for INCR/DECR. Unlike parseIntegerToken this keeps full precision
// above 2^53 and rejects values outside the int64 range.
export function parseInt64Token(token: Buffer): bigint {
  const raw = token.toString()
  if (!isIntegerToken(raw)) {
    throw new ExpectedIntegerError()
  }

  const value = BigInt(raw)
  if (value < INT64_MIN || value > INT64_MAX) {
    throw new ExpectedIntegerError()
  }

  return value
}

export function parsePositiveExpireToken(
  token: Buffer,
  commandName: string,
): number {
  const value = parseIntegerToken(token)
  if (value <= 0) {
    throw new InvalidExpireTimeError(commandName)
  }

  return value
}

/**
 * Real Redis formats the echoed subcommand with `%.128s`, so a longer name is
 * cut at 128 bytes. Verified exact against 7.0.15 and 8.0.6: at 128 the reply
 * is byte-identical to the mock's, at 129 real echoes 128 characters. Redis 6.2
 * has no truncation at all — a 300-byte name comes back whole.
 */
const SUBCOMMAND_ECHO_LIMIT = 128

/**
 * The reply real Redis sends when a container command is given a subcommand it
 * does not recognize.
 *
 * Redis 7.0 moved container commands into the command table, which changed both
 * the template and the echo model. Captured from real servers:
 *
 * ```
 * 6.2.24  CONFIG BOGUS -> Unknown subcommand or wrong number of arguments for 'BOGUS'. Try CONFIG HELP.
 * 7.0.15  CONFIG BOGUS -> unknown subcommand 'BOGUS'. Try CONFIG HELP.
 * 8.0.6   CONFIG BOGUS -> unknown subcommand 'BOGUS'. Try CONFIG HELP.
 * ```
 *
 * The subcommand is echoed with the casing the client sent. `container` is the
 * upper-case parent name as it appears in the `Try ... HELP.` suffix.
 *
 * Note this covers the *unknown subcommand* path only. On 6.2 the same template
 * also served wrong-arity replies for a known subcommand, where 7.0+ says
 * `wrong number of arguments for 'config|get' command`; that arity path is a
 * separate divergence and is not handled here (#413).
 *
 * There are 15 hand-rolled copies of this message across the command modules
 * with three wordings live at once; #413 tracks migrating them onto this
 * helper. Only CONFIG is routed through it so far.
 */
export function unknownSubcommandError(
  container: string,
  subcommand: Buffer | string,
  profile: CompatibilityProfile,
): RedisCommandError {
  const raw = Buffer.isBuffer(subcommand) ? subcommand : Buffer.from(subcommand)

  if (!profile.has('error.unknown-subcommand-wording')) {
    return new RedisCommandError(
      `Unknown subcommand or wrong number of arguments for '${raw.toString()}'. Try ${container} HELP.`,
    )
  }

  const echoed = raw.subarray(0, SUBCOMMAND_ECHO_LIMIT).toString()
  return new RedisCommandError(
    `unknown subcommand '${echoed}'. Try ${container} HELP.`,
  )
}

export function requireNextOptionValue(
  args: readonly Buffer[],
  index: number,
): Buffer {
  const value = args[index]
  if (!value) {
    throw new RedisSyntaxError()
  }

  return value
}
