/**
 * Wall-clock source for `MONITOR` line timestamps.
 *
 * Real Redis stamps each line from `gettimeofday()`, so the six fractional
 * digits it prints carry genuine microsecond resolution. `Date.now()` only
 * resolves to whole milliseconds — it looks the same on the wire but leaves the
 * last three digits permanently `000` (#388). `process.hrtime.bigint()` has
 * nanosecond resolution but an arbitrary origin, so the two are combined: the
 * coarse wall clock fixes the epoch, the monotonic clock supplies the
 * sub-millisecond offset from it.
 *
 * This lives in `core/` rather than `state/` because it is a clock, not state —
 * it touches no `RedisServerState`, keyspace or feed — and because
 * `command-executor.ts` is its caller. Putting it here keeps every `core/` ->
 * `state/` import type-only, as it is on `main`.
 */

let wallClockOriginMicros = BigInt(Date.now()) * 1000n
let monotonicOriginNanos = process.hrtime.bigint()

/**
 * How far the derived clock may drift from `Date.now()` before the anchor is
 * recaptured. The monotonic clock does not follow NTP adjustments, suspend /
 * resume, or manual clock changes, so without a resync a long-lived server
 * would slowly diverge from wall-clock time. One second is far above normal
 * drift, and the wide margin keeps the common case free of the millisecond-
 * scale jitter inherent in comparing a floored `Date.now()` against a precise
 * offset.
 */
const RESYNC_THRESHOLD_MICROS = 1_000_000n

/**
 * Current time in microseconds since the Unix epoch, with genuine
 * sub-millisecond resolution.
 *
 * Monotonic **while the wall clock is stable**, which is the only case the
 * server ever sees in practice. It is deliberately *not* monotonic across a
 * wall-clock step larger than {@link RESYNC_THRESHOLD_MICROS}: the anchor is
 * recaptured and the returned value follows the new wall clock, so a backwards
 * step produces a backwards timestamp. That matches real Redis, whose
 * `gettimeofday()` does exactly the same thing — a MONITOR capture taken across
 * an NTP step is not ordered there either.
 *
 * Two known costs of the resync path, both accepted:
 *
 *  - the single line emitted at a resync is millisecond-quantized, because a
 *    freshly floored `Date.now()` is all the precision available at that
 *    instant. Resolution returns on the next call;
 *  - between resyncs the value may sit up to a second away from true
 *    wall-clock time, where real Redis is always exact.
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
 *
 * The input is coerced to a finite integer because this is a published `/core`
 * entry point. A fractional value would otherwise produce a string with two
 * decimal points (`1695000000000000.5` -> `"1695000000.0000.5"`), and `NaN` or
 * `Infinity` would produce `"NaN.000NaN"` / `"Infinity.000NaN"` — neither is a
 * timestamp at all. {@link monitorTimestampMicros} never returns any of them.
 */
export function formatMonitorTimestamp(timestampMicros: number): string {
  const micros = Number.isFinite(timestampMicros)
    ? Math.trunc(timestampMicros)
    : 0
  const seconds = Math.floor(micros / 1_000_000)
  const microseconds = micros - seconds * 1_000_000

  return `${seconds}.${String(microseconds).padStart(6, '0')}`
}
