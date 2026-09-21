import type { Unsubscribe } from './mutation-events'

export type RedisMonitorCommandEvent = {
  /**
   * Microseconds since the Unix epoch, matching the resolution real Redis
   * prints on every `MONITOR` line (`<unix-seconds>.<6 digits>`, stamped from
   * `gettimeofday()`). Produced by {@link monitorTimestampMicros} — do not use
   * `Date.now() * 1000` here, which looks the same but quantizes every line to
   * a whole millisecond (#388).
   *
   * A plain `number` is exact for this: microsecond epoch values stay below
   * `Number.MAX_SAFE_INTEGER` until the year 2255.
   */
  timestampMicros: number
  database: number
  clientId: string
  clientAddress?: string
  command: Buffer
  args: readonly Buffer[]
}

export type RedisMonitorCommandListener = (
  event: RedisMonitorCommandEvent,
) => void

export class RedisMonitorFeed {
  private readonly listeners = new Set<RedisMonitorCommandListener>()

  get subscriberCount(): number {
    return this.listeners.size
  }

  subscribe(listener: RedisMonitorCommandListener): Unsubscribe {
    this.listeners.add(listener)
    return () => {
      this.listeners.delete(listener)
    }
  }

  publish(event: RedisMonitorCommandEvent): void {
    for (const listener of Array.from(this.listeners)) {
      listener(cloneMonitorCommandEvent(event))
    }
  }
}

/**
 * Wall-clock anchor for {@link monitorTimestampMicros}.
 *
 * `Date.now()` only resolves to whole milliseconds, so it cannot produce the
 * six meaningful fractional digits real Redis prints. `process.hrtime.bigint()`
 * has nanosecond resolution but an arbitrary origin, so the two are combined:
 * the coarse wall clock fixes the epoch, the monotonic clock supplies the
 * sub-millisecond offset from it.
 */
let wallClockOriginMicros = BigInt(Date.now()) * 1000n
let monotonicOriginNanos = process.hrtime.bigint()

/**
 * How far the derived clock may drift from `Date.now()` before the anchor is
 * recaptured. The monotonic clock does not follow NTP adjustments, suspend /
 * resume, or manual clock changes, so without a resync a long-lived server
 * would slowly diverge from wall-clock time. One second is far above normal
 * drift and far below anything a caller could notice, and the wide margin keeps
 * the common case free of the millisecond-scale jitter inherent in comparing a
 * floored `Date.now()` against a precise offset.
 */
const RESYNC_THRESHOLD_MICROS = 1_000_000n

/**
 * Current time in microseconds since the Unix epoch, with genuine
 * sub-millisecond resolution (#388).
 */
export function monitorTimestampMicros(): number {
  const wallClockMicros = BigInt(Date.now()) * 1000n
  const elapsedMicros = (process.hrtime.bigint() - monotonicOriginNanos) / 1000n
  const derivedMicros = wallClockOriginMicros + elapsedMicros

  const drift =
    derivedMicros > wallClockMicros
      ? derivedMicros - wallClockMicros
      : wallClockMicros - derivedMicros

  if (drift <= RESYNC_THRESHOLD_MICROS) {
    return Number(derivedMicros)
  }

  wallClockOriginMicros = wallClockMicros
  monotonicOriginNanos = process.hrtime.bigint()
  return Number(wallClockMicros)
}

/**
 * Render a microsecond epoch timestamp the way real Redis writes it on a
 * `MONITOR` line: `<unix-seconds>.<6 digits>`. Split with integer arithmetic
 * rather than `(micros / 1e6).toFixed(6)`, which can round the last digit once
 * the value exceeds a double's ~15 significant digits.
 */
export function formatMonitorTimestamp(timestampMicros: number): string {
  const seconds = Math.floor(timestampMicros / 1_000_000)
  const microseconds = timestampMicros - seconds * 1_000_000

  return `${seconds}.${String(microseconds).padStart(6, '0')}`
}

function cloneMonitorCommandEvent(
  event: RedisMonitorCommandEvent,
): RedisMonitorCommandEvent {
  return {
    ...event,
    command: Buffer.from(event.command),
    args: event.args.map(arg => Buffer.from(arg)),
  }
}
