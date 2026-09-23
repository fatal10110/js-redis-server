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

// Key-level TTL: Redis rounds remaining time to the nearest second
// ((ms+500)/1000), not ceil/floor — matches EXPIRETIME and real TTL behavior.
export function ttlSeconds(expiresAt: number): number {
  return Math.max(0, Math.round((expiresAt - Date.now()) / 1000))
}

// Hash-field TTL (HTTL): Redis rounds remaining time *up* to the next second
// ((ms+999)/1000), so any sub-second remainder reports 1, not 0 (#432).
export function hashFieldTtlSeconds(expiresAt: number): number {
  return Math.max(0, Math.ceil((expiresAt - Date.now()) / 1000))
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
 * Real Redis renders the echoed subcommand with `%.128s` on the 7.0+ template,
 * so a longer name is cut at 128 *bytes*. Verified exact against 7.0.15 and
 * 8.0.6: 127 and 128 come back whole, 129 and 300 are both cut to 128 — and
 * when the cut lands inside a multi-byte character the partial byte is emitted
 * raw (`'A'.repeat(127) + 'é'` replies with 128 bytes ending in a bare
 * 0xc3). That is why the cut is taken on the Buffer and the result never round
 * trips through a string. Redis 6.2 has no truncation at all — a 300-byte name
 * comes back whole.
 */
const SUBCOMMAND_ECHO_LIMIT = 128

/**
 * The reply real Redis sends when a container command is given a subcommand it
 * does not recognize — including a subcommand this server gates off on older
 * profiles, which real Redis of that vintage simply did not have.
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
 * `container` is the upper-case parent name as it appears in the
 * `Try ... HELP.` suffix; `subcommand` is echoed with the bytes the client
 * sent, casing and all.
 */
export function unknownSubcommandError(
  container: string,
  subcommand: Buffer | string,
  profile: CompatibilityProfile,
): RedisCommandError {
  return subcommandError(container, subcommand, profile, 'unknown')
}

/**
 * Real Redis' `addReplySubcommandSyntaxError` — the reply a container command
 * builds itself when a *known* subcommand is given arguments it cannot use, as
 * opposed to the dispatch-level {@link unknownSubcommandError}. Same 7.0 case
 * flip, but the wording keeps the `or wrong number of arguments` clause and
 * there is no `%.128s` truncation. Captured from real servers:
 *
 * ```
 * 6.2.24  PUBSUB CHANNELS a b -> Unknown subcommand or wrong number of arguments for 'CHANNELS'. Try PUBSUB HELP.
 * 8.0.6   PUBSUB CHANNELS a b -> unknown subcommand or wrong number of arguments for 'CHANNELS'. Try PUBSUB HELP.
 * ```
 */
export function subcommandSyntaxError(
  container: string,
  subcommand: Buffer | string,
  profile: CompatibilityProfile,
): RedisCommandError {
  return subcommandError(container, subcommand, profile, 'syntax')
}

/**
 * Both templates in one place, because on 6.2 they *are* one template: before
 * container commands entered the command table every one of these replies came
 * out of `addReplySubcommandSyntaxError`.
 *
 * The body is built as a Buffer rather than a string so the echoed name
 * survives byte for byte — real 8.0.6 answers `CONFIG \xff\xfe\xfd` with those
 * three bytes, where a UTF-8 decode would turn each into U+FFFD.
 */
function subcommandError(
  container: string,
  subcommand: Buffer | string,
  profile: CompatibilityProfile,
  kind: 'unknown' | 'syntax',
): RedisCommandError {
  const raw = Buffer.isBuffer(subcommand) ? subcommand : Buffer.from(subcommand)
  const echoed = asCString(raw)

  if (!profile.has('error.unknown-subcommand-wording')) {
    return subcommandErrorFrom(
      'Unknown subcommand or wrong number of arguments for',
      echoed,
      container,
    )
  }

  if (kind === 'syntax') {
    return subcommandErrorFrom(
      'unknown subcommand or wrong number of arguments for',
      echoed,
      container,
    )
  }

  return subcommandErrorFrom(
    'unknown subcommand',
    echoed.subarray(0, SUBCOMMAND_ECHO_LIMIT),
    container,
  )
}

/**
 * The prefix up to the first NUL. Both templates render the echoed name with a
 * `%s`-family conversion over a C string, so the echo stops at the first NUL
 * byte — and that cut happens *before* `%.128s` counts its 128. Captured from
 * real servers:
 *
 * ```
 * 8.0.6   CONFIG 'AA\0BB'              -> unknown subcommand 'AA'. Try CONFIG HELP.
 * 8.0.6   CONFIG 'A'*100 + \0 + 'A'*100 -> echoes 100, not 128
 * 8.0.6   CONFIG '\0' + 'A'*10          -> echoes nothing at all
 * 8.0.6   CONFIG 'A'*200 + \0 + 'A'*200 -> echoes 128 (NUL cut, then %.128s)
 * 6.2.24  CONFIG 'A'*200 + \0 + 'A'*200 -> echoes 200 (NUL cut, no length cut)
 * ```
 *
 * So 6.2 truncates at a NUL even though it does not truncate by length, which
 * is why this runs ahead of the profile branch rather than inside it. It also
 * keeps a raw NUL out of a `-ERR ...\r\n` simple-error frame, a body real Redis
 * has no way to produce.
 */
function asCString(value: Buffer): Buffer {
  const nul = value.indexOf(0)
  return nul === -1 ? value : value.subarray(0, nul)
}

function subcommandErrorFrom(
  lead: string,
  echoed: Buffer,
  container: string,
): RedisCommandError {
  return new RedisCommandError(
    Buffer.concat([
      Buffer.from(`${lead} '`),
      echoed,
      Buffer.from(`'. Try ${container} HELP.`),
    ]),
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
