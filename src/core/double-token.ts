/**
 * Operand parsing for the `double`-typed arguments (#534, #513).
 *
 * Redis converts a double operand with one of three C routines, and they do
 * not accept the same tokens:
 *
 *  - `string2d()` (util.c), behind `getDoubleFromObject()`: ZADD and ZINCRBY
 *    scores, ZUNION / ZINTER weights, GEOADD and every GEO* longitude,
 *    latitude, radius, width and height. The whole token must be the number:
 *    an empty token, one starting with whitespace, trailing bytes, NaN, an
 *    overflow (`1e400`) and a nonzero value that underflows to zero
 *    (`1e-400`) are refused. Hex floats (`0x10`, `0x1p3`) are accepted through
 *    C `strtod` everywhere except Valkey 8.1+, whose `valkey_strtod()` is
 *    fast_float alone (`double.valkey-strtod`). Redis 8.0 also tries
 *    fast_float first, but falls back to `strtod` for what it cannot read.
 *  - `zslParseRange()` (t_zset.c), the ZRANGEBYSCORE / ZCOUNT / ZRANGE BYSCORE
 *    / ZREMRANGEBYSCORE bounds (after an optional `(`), and
 *  - sort.c, the numeric SORT element or BY weight.
 *
 * The last two call the converter directly and only check that it stopped at
 * the C string's end (`eptr[0] == '\0'`): an empty token is 0, and a NUL byte
 * ends the token. Up to Redis 7.4 / Valkey 8.0 that converter is C `strtod`,
 * which also skips leading whitespace and reads hex floats; Redis 8.0 and
 * Valkey 8.1 switched to fast_float, which does neither
 * (`double.fast-float`). The range parser never checks `errno`, so `1e400`
 * is infinity and `1e-400` is 0. SORT does:
 *
 *  - glibc `strtod` (Redis <= 7.4, Valkey 8.0) sets `ERANGE` on overflow and
 *    on any inexact result below `DBL_MIN` (`1e-310`, `1e-400`), which SORT
 *    refuses;
 *  - Redis 8.0's fast_float never sets `errno`, so SORT takes `1e400` as
 *    infinity and `1e-400` as 0;
 *  - Valkey 8.1+'s `valkey_strtod()` resets `errno` and reports `ERANGE` for
 *    an overflow or an underflow to zero, and `EINVAL` when nothing was read,
 *    so SORT also refuses an empty element there (`double.valkey-strtod`).
 *
 * Known limits: real Redis <= 7.4 and Valkey 8.0 never reset `errno` before
 * SORT's check, so a stale `ERANGE` from an earlier command can fail a valid
 * SORT; that is not modelled. glibc decides "tiny" after rounding, so an
 * exact value just under `DBL_MIN` that rounds up to it with a 53-bit
 * mantissa (`0x1.fffffffffffffp-1023`) is flagged there and not here.
 */

import type { CompatibilityProfile } from './compatibility/profile'
import { binaryToDouble, clampExponent } from './long-double-token'

/** Smallest normal double, `DBL_MIN`. */
const DBL_MIN = 2 ** -1022

const INFINITY_PATTERN = /^([+-]?)inf(?:inity)?$/i
const DECIMAL_PATTERN = /^[+-]?(\d*)(?:\.(\d*))?(?:[eE]([+-]?\d+))?$/
const HEX_PATTERN =
  /^([+-]?)0[xX]([0-9a-fA-F]*)(?:\.([0-9a-fA-F]*))?(?:[pP]([+-]?\d+))?$/
/** C `isspace()` in the C locale. */
const LEADING_SPACE = /^[ \t\n\v\f\r]+/

/**
 * How a converted value left the normal range, where it did:
 * `overflow` (a finite literal beyond `DBL_MAX`), `zero` (a nonzero literal
 * that rounded to zero) or `tiny` (an inexact subnormal result).
 */
export type DoubleRangeError = 'overflow' | 'zero' | 'tiny'

export interface ScannedDouble {
  value: number
  range?: DoubleRangeError
}

/**
 * Convert the whole of `text` as C `strtod` (with `hex`) or fast_float
 * (without) would, returning `undefined` when the conversion would stop
 * before the end of `text` or read NaN. Leading whitespace and the empty
 * token are the caller's business.
 */
export function scanDoubleToken(
  text: string,
  hex: boolean,
): ScannedDouble | undefined {
  const infinity = INFINITY_PATTERN.exec(text)
  if (infinity) {
    return { value: infinity[1] === '-' ? -Infinity : Infinity }
  }

  if (hex) {
    const match = HEX_PATTERN.exec(text)
    if (match) {
      return scanHex(match[1] === '-', match[2]!, match[3] ?? '', match[4])
    }
  }

  const decimal = DECIMAL_PATTERN.exec(text)
  if (decimal) {
    return scanDecimal(text, decimal[1]!, decimal[2] ?? '', decimal[3])
  }
  return undefined
}

/**
 * `string2d()`: the double operands of ZADD, ZINCRBY, ZUNION/ZINTER WEIGHTS
 * and the GEO commands. `undefined` means the caller's "not a valid float"
 * (or "need numeric radius", ...) error.
 */
export function parseString2dToken(
  raw: Buffer | string,
  profile: CompatibilityProfile,
): number | undefined {
  const text = toLatin1(raw)
  if (text.length === 0 || LEADING_SPACE.test(text)) {
    return undefined
  }
  const scanned = scanDoubleToken(text, !profile.has('double.valkey-strtod'))
  if (!scanned || scanned.range === 'overflow' || scanned.range === 'zero') {
    return undefined
  }
  return scanned.value
}

/**
 * `zslParseRange()`'s conversion of a score bound, the `(` already removed.
 * `undefined` means "min or max is not a float".
 */
export function parseScoreRangeToken(
  raw: Buffer | string,
  profile: CompatibilityProfile,
): number | undefined {
  const fastFloat = profile.has('double.fast-float')
  const text = cStringOperand(toLatin1(raw), fastFloat)
  if (text === undefined) return undefined
  if (text.length === 0) return 0
  return scanDoubleToken(text, !fastFloat)?.value
}

/**
 * sort.c's conversion of a numeric SORT element or BY weight. `undefined`
 * means "One or more scores can't be converted into double".
 */
export function parseSortScoreToken(
  raw: Buffer | string,
  profile: CompatibilityProfile,
): number | undefined {
  const fastFloat = profile.has('double.fast-float')
  const valkeyStrtod = profile.has('double.valkey-strtod')
  const text = cStringOperand(toLatin1(raw), fastFloat)
  if (text === undefined) return undefined
  if (text.length === 0) return valkeyStrtod ? undefined : 0

  const scanned = scanDoubleToken(text, !fastFloat)
  if (!scanned) return undefined
  if (scanned.range !== undefined) {
    // glibc strtod flags every range case, valkey_strtod overflow and
    // underflow to zero; Redis 8.0's fast_float leaves errno alone.
    if (!fastFloat) return undefined
    if (valkeyStrtod && scanned.range !== 'tiny') return undefined
  }
  return scanned.value
}

/** `|value| / 2^-1074` for a subnormal `value` (2^1074 itself overflows). */
function subnormalUnits(value: number): bigint {
  return BigInt(Math.abs(value) * 2 ** 1000 * 2 ** 74)
}

function toLatin1(raw: Buffer | string): string {
  return typeof raw === 'string' ? raw : raw.toString('latin1')
}

/**
 * The part of `text` a C converter reading up to `eptr[0] == '\0'` sees:
 * everything before the first NUL, minus the leading whitespace `strtod`
 * skips. `undefined` when only whitespace is left: `strtod` then converts
 * nothing and leaves `eptr` on that whitespace.
 */
function cStringOperand(text: string, fastFloat: boolean): string | undefined {
  const nul = text.indexOf('\0')
  const cString = nul === -1 ? text : text.slice(0, nul)
  if (fastFloat) return cString
  const stripped = cString.replace(LEADING_SPACE, '')
  if (stripped.length === 0 && cString.length > 0) return undefined
  return stripped
}

function scanDecimal(
  text: string,
  intDigits: string,
  fracDigits: string,
  exponentText: string | undefined,
): ScannedDouble | undefined {
  if (intDigits.length === 0 && fracDigits.length === 0) {
    return undefined
  }

  // V8's Number() rounds a decimal string of any length correctly.
  const value = Number(text)
  if (!Number.isFinite(value)) {
    return { value, range: 'overflow' }
  }
  if (Math.abs(value) >= DBL_MIN) {
    return { value }
  }

  const digits = (intDigits + fracDigits).replace(/^0+/, '')
  if (digits.length === 0) {
    return { value }
  }
  if (value === 0) {
    return { value, range: 'zero' }
  }

  // A subnormal result is exact only if digits * 10^exponent equals
  // units * 2^-1074, i.e. digits * 2^1074 == units * 10^-exponent (the
  // exponent is negative this far down).
  const exponent = clampExponent(exponentText) - fracDigits.length
  const units = subnormalUnits(value)
  const exact =
    exponent < 0 && BigInt(digits) << 1074n === units * 10n ** BigInt(-exponent)
  return exact ? { value } : { value, range: 'tiny' }
}

function scanHex(
  negative: boolean,
  intDigits: string,
  fracDigits: string,
  exponentText: string | undefined,
): ScannedDouble | undefined {
  if (intDigits.length === 0 && fracDigits.length === 0) {
    return undefined
  }

  const sign = negative ? -1 : 1
  const mantissa = BigInt(`0x${intDigits}${fracDigits}`)
  if (mantissa === 0n) {
    return { value: sign * 0 }
  }

  // value = mantissa * 2^exponent
  const exponent = clampExponent(exponentText) - 4 * fracDigits.length
  const bits = mantissa.toString(2).length
  // Far below the smallest subnormal: zero, without the BigInt shifts below.
  if (bits - 1 + exponent < -1100) {
    return { value: sign * 0, range: 'zero' }
  }
  const value = sign * binaryToDouble(mantissa, exponent, bits)
  if (!Number.isFinite(value)) {
    return { value, range: 'overflow' }
  }
  if (value === 0) {
    return { value, range: 'zero' }
  }
  if (Math.abs(value) >= DBL_MIN) {
    return { value }
  }

  // Exact iff mantissa * 2^exponent == units * 2^-1074.
  const units = subnormalUnits(value)
  const shift = exponent + 1074
  const exact =
    shift >= 0
      ? mantissa << BigInt(shift) === units
      : mantissa === units << BigInt(-shift)
  return exact ? { value } : { value, range: 'tiny' }
}
