import type {
  RedisClientSession,
  RedisExecutionContext,
} from '../core/redis-context'
import type { CompatibilityProfile } from '../core/compatibility/profile'
import {
  type BinaryFloat,
  longDoubleTimesThousand,
  parseLongDoubleMagnitude,
} from '../core/long-double-token'
import { errors } from '../core/redis-error'
import type { RedisDataValue } from '../state/data-types'

/** `setTimeout` takes at most a signed 32-bit delay. */
const MAX_TIMER_DELAY_MS = 2 ** 31 - 1

/**
 * Sessions inside {@link blockOnKeys}, with how many of their commands are:
 * the clients real Redis flags `CLIENT_BLOCKED` (`flags=b` in CLIENT LIST,
 * counted by INFO's `blocked_clients`). A client sends one command at a time,
 * so the count is 1 in practice; it is a count so nesting can never clear
 * the flag early.
 */
const blockedSessions = new WeakMap<RedisClientSession, number>()

/** Whether `session` is parked in a blocking command (BLPOP, XREAD BLOCK, ...). */
export function isClientBlocked(session: RedisClientSession): boolean {
  return (blockedSessions.get(session) ?? 0) > 0
}

function markBlocked(session: RedisClientSession): () => void {
  blockedSessions.set(session, (blockedSessions.get(session) ?? 0) + 1)
  return () => {
    const count = (blockedSessions.get(session) ?? 1) - 1
    if (count > 0) {
      blockedSessions.set(session, count)
    } else {
      blockedSessions.delete(session)
    }
  }
}

export type BlockOnKeysOptions<TResult> = {
  keys: readonly Buffer[]
  /**
   * Only a write that leaves one of `keys` holding this type wakes the waiter,
   * as in real Redis (7.2+): a key overwritten with another type (`SET k foo`
   * on a key a `BLPOP` waits on) keeps the client blocked instead of waking it
   * into a WRONGTYPE reply.
   *
   * Omit it to wake on every change to the keys: any modified-key event
   * (write, delete, expiry, flush, ...) and any `signalKeyReady` on them.
   * `XREADGROUP` does, because real Redis unblocks it when its stream is
   * overwritten with another type (the re-run replies WRONGTYPE), deleted, or
   * loses its group (the re-run replies NOGROUP). A wake that changed nothing
   * relevant just re-parks.
   */
  type?: RedisDataValue['type']
  /** `undefined` blocks forever. */
  timeoutMs: number | undefined
  /**
   * Re-run the command's non-blocking attempt; `null` means nothing is ready
   * yet and the command keeps waiting. Errors (e.g. WRONGTYPE from another of
   * the keys) propagate, the same as real Redis re-executing the command.
   */
  attempt: () => TResult | null
}

/**
 * Park a blocking command (BLPOP, BLMOVE, BZMPOP, XREAD BLOCK, ...) until
 * `attempt` yields a result or the timeout expires (`null`).
 *
 * The key subscriptions are taken once and kept until the command finishes, so
 * a wake that finds nothing ready (another waiter won, or the value was
 * deleted in the same transaction) re-parks the command at its original place
 * in each key's listener list. Real Redis likewise keeps a blocked client's
 * position; re-subscribing would move it behind every later waiter.
 *
 * The wake is reported synchronously through `onWake`, so a turn-aware park
 * handler queues the command's resume the moment the write happens — ahead of
 * any command queued after it, in the order the waiters blocked.
 */
export async function blockOnKeys<TResult>(
  ctx: RedisExecutionContext,
  options: BlockOnKeysOptions<TResult>,
): Promise<TResult | null> {
  const { keys, type, timeoutMs, attempt } = options
  const deadline = timeoutMs === undefined ? undefined : Date.now() + timeoutMs
  // Armed only while parked: writes seen while the command is resuming or
  // re-checking are already reflected in what `attempt` observes.
  let wake: (() => void) | undefined

  const db = ctx.db
  const unsubs = keys.flatMap(key =>
    type === undefined
      ? [
          db.subscribeKey(key, () => wake?.()),
          db.subscribeKeyReady(key, () => wake?.()),
        ]
      : [
          db.subscribeKey(key, event => {
            if (event.type === 'write' && event.value.type === type) wake?.()
          }),
        ],
  )

  // Blocked from here until the command finishes: a wake that finds nothing
  // ready re-parks without the client ever leaving the blocked state, and
  // the flag is cleared before the reply is written.
  const unmarkBlocked = markBlocked(ctx.session)
  try {
    while (true) {
      const remaining =
        deadline === undefined ? undefined : Math.max(0, deadline - Date.now())
      if (remaining === 0) return null
      // A longer wait is parked in timer-sized steps (Node fires a longer
      // delay at once).
      const parkMs =
        remaining === undefined
          ? undefined
          : Math.min(remaining, MAX_TIMER_DELAY_MS)

      const wakeListeners: Array<() => void> = []
      const waitFor = new Promise<true>(resolve => {
        wake = () => {
          wake = undefined
          resolve(true)
          for (const listener of wakeListeners) listener()
        }
      })

      let woken: true | null
      try {
        woken = await ctx.park({
          waitFor,
          onWake: listener => {
            wakeListeners.push(listener)
          },
          timeoutMs: parkMs,
          signal: ctx.signal,
        })
      } finally {
        wake = undefined
      }

      if (woken === null) {
        if (parkMs === remaining) return null
        continue
      }

      const result = attempt()
      if (result !== null) return result
    }
  } finally {
    unmarkBlocked()
    for (const unsub of unsubs) {
      try {
        unsub()
      } catch {
        // ignore errors from individual unsubscribers so all are attempted
      }
    }
  }
}

/**
 * A blocking command's wait once its non-blocking attempt found nothing:
 * {@link blockOnKeys}, then `timedOut()` for its timeout reply.
 *
 * A command called from a script cannot block (real Redis sets
 * `CLIENT_DENY_BLOCKING` on the script client), so there it answers
 * `timedOut()` at once, even with timeout 0, as if the timeout had expired
 * (#500). Which blocking commands a script may call at all is the command
 * table's `noscript` flag: from 7.2 every one of them, before that only
 * BLMPOP and BZMPOP. The reply is synchronous, because `redis.call` runs
 * through `executePlanSync`, which refuses a promise.
 */
export function blockOrTimeOut<TResult>(
  ctx: RedisExecutionContext,
  options: BlockOnKeysOptions<TResult>,
  timedOut: () => TResult,
): TResult | Promise<TResult> {
  if (ctx.inScript) return timedOut()
  return blockOnKeys(ctx, options).then(result => result ?? timedOut())
}

/** A parsed blocking timeout (`0` = forever) as park milliseconds. */
export function blockingTimeoutMs(timeoutMs: number): number | undefined {
  return timeoutMs === 0 ? undefined : timeoutMs
}

const LLONG_MAX = (1n << 63n) - 1n

/** `value > LLONG_MAX` for a binary float. */
function exceedsLlongMax(value: BinaryFloat): boolean {
  if (value.negative || value.mantissa === 0n) return false
  if (value.exponent >= 0) {
    return (
      value.exponent > 64 ||
      value.mantissa << BigInt(value.exponent) > LLONG_MAX
    )
  }
  return value.mantissa > LLONG_MAX << BigInt(-value.exponent)
}

/** `ceill(value)` or a truncating cast, as an exact integer. */
function wholeMilliseconds(value: BinaryFloat, roundUp: boolean): bigint {
  const { negative, mantissa, exponent } = value
  if (exponent >= 0) {
    const magnitude = mantissa << BigInt(exponent)
    return negative ? -magnitude : magnitude
  }
  const shift = BigInt(-exponent)
  const floor = mantissa >> shift
  const hasFraction = (mantissa & ((1n << shift) - 1n)) !== 0n
  if (negative) return -floor
  return roundUp && hasFraction ? floor + 1n : floor
}

/**
 * A blocking command's timeout in seconds (BLPOP, BLMOVE, BZMPOP, ...), read
 * the way `getTimeoutFromObjectOrReply` (timeout.c, UNIT_SECONDS) does, as
 * whole milliseconds (`0` = block forever) (#577).
 *
 * - The token goes through `string2ld()`: no leading or trailing space, no
 *   empty token, no NaN, hex floats welcome; else `timeout is not a float or
 *   out of range`.
 * - The milliseconds are `ftval * 1000.0` in an 80-bit `long double`
 *   ({@link longDoubleTimesThousand}), so `0.001` is a hair under 1 ms and
 *   `-0.001` a hair above -1 ms.
 * - More than LLONG_MAX ms (`inf`, `1e400`, `1e19`) is `timeout is out of
 *   range` from Redis 7.2 (`timeout.ms-overflow-check`); 6.2 / 7.0 cast it to
 *   LLONG_MIN, `timeout is negative`.
 * - The milliseconds round up from Redis 7.0.8 (`timeout.ceil-ms`) and are
 *   truncated before, so `-0.001` is no timeout everywhere and `0.001` is
 *   no timeout on 6.2; below that, `timeout is negative`.
 * - From 7.2 an end time past LLONG_MAX is out of range; 6.2 / 7.0 let it wrap
 *   into the past, a timeout that expires at once (1 ms here).
 */
export function parseBlockingTimeout(
  token: Buffer,
  profile: CompatibilityProfile,
): number {
  if (parseLongDoubleMagnitude(token) === undefined) {
    throw errors.timeoutNotFloat()
  }

  const ms = longDoubleTimesThousand(token)
  const overflowCheck = profile.has('timeout.ms-overflow-check')
  if (overflowCheck && exceedsLlongMax(ms)) {
    throw errors.timeoutOutOfRange()
  }

  const whole = wholeMilliseconds(ms, profile.has('timeout.ceil-ms'))
  // Out of the long long range, the x87 cast gives LLONG_MIN.
  if (whole < 0n || whole > LLONG_MAX) {
    throw errors.timeoutNegative()
  }
  if (whole === 0n) return 0
  if (whole > LLONG_MAX - BigInt(Date.now())) {
    if (overflowCheck) throw errors.timeoutOutOfRange()
    return 1
  }
  return Number(whole)
}
