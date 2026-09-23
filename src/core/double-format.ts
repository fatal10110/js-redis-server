import {
  resolveCompatibilityProfile,
  type CompatibilityProfile,
} from './compatibility/profile'

/**
 * The profile half {@link formatRedisDouble} reads: only whether the
 * `reply.double-fpconv` gate is open.
 */
export type DoubleFormatProfile = Pick<CompatibilityProfile, 'has'>

const DEFAULT_PROFILE = resolveCompatibilityProfile()

/**
 * The text of a `double` reply — a RESP3 `,` double, and the bulk string RESP2
 * sends in its place (`addReplyDouble()`), which is therefore also what a
 * client reads back off a RESP2 connection and what `redis.call` hands a
 * script. The encoder, the socketless decoders and the Lua bridge all share
 * this, so the paths cannot drift.
 *
 * Two spellings, by profile (#451):
 *  - Redis 6.2 / 7.0: `%.17g` — `0.1` is `0.10000000000000001`.
 *  - Redis 7.2+ and every Valkey (`reply.double-fpconv`): `d2string()` —
 *    exact integer digits within ±2^62, `fpconv_dtoa` (Grisu2) otherwise.
 *
 * `inf` / `-inf` / `nan` and `-0` are spelled the same by both. Without a
 * profile, the default profile's spelling is used.
 */
export function formatRedisDouble(
  value: number,
  profile: DoubleFormatProfile = DEFAULT_PROFILE,
): string {
  if (Number.isNaN(value)) {
    return 'nan'
  }

  if (value === Infinity) {
    return 'inf'
  }

  if (value === -Infinity) {
    return '-inf'
  }

  if (value === 0) {
    return Object.is(value, -0) ? '-0' : '0'
  }

  return profile.has('reply.double-fpconv')
    ? formatD2string(value)
    : formatPercentG17(value)
}

// ---------------------------------------------------------------------------
// Redis 6.2 / 7.0: printf("%.17g")
// ---------------------------------------------------------------------------

const G17_PRECISION = 17

/**
 * `snprintf("%.17g", value)` for a finite, non-zero double, as glibc prints
 * it: correctly rounded, ties to even.
 *
 * `toExponential(16)` is correctly rounded too, but breaks a tie *up*
 * (`1234567890123456.25` → `…456.3`, where glibc prints `…456.2`). A tie needs
 * the exact decimal expansion to be exactly 18 significant digits long, which
 * {@link g17TiePossible} rules out cheaply for almost every double; only the
 * rest pay for the exact BigInt expansion.
 */
function formatPercentG17(value: number): string {
  const sign = value < 0 ? '-' : ''
  const abs = Math.abs(value)
  let digits: string
  let exponent: number

  if (!g17TiePossible(abs)) {
    const [mantissa, exponentText] = abs
      .toExponential(G17_PRECISION - 1)
      .split('e')
    digits = mantissa.replace('.', '').replace(/0+$/, '')
    exponent = Number(exponentText)
  } else {
    ;({ digits, exponent } = exactDecimal(abs))
    if (digits.length > G17_PRECISION) {
      const kept = digits.slice(0, G17_PRECISION)
      const dropped = digits.slice(G17_PRECISION)
      const roundUp =
        dropped[0] > '5' ||
        (dropped[0] === '5' &&
          (/[1-9]/.test(dropped.slice(1)) || Number(kept.at(-1)) % 2 === 1))
      digits = roundUp ? (BigInt(kept) + 1n).toString() : kept
      if (digits.length > G17_PRECISION) {
        // 99…9 carried into a new leading digit.
        digits = digits.slice(0, G17_PRECISION)
        exponent++
      }
    }
  }

  // %g: exponent form when X < -4 or X >= P, then drop trailing zeros.
  if (exponent < -4 || exponent >= G17_PRECISION) {
    const mantissa = stripFraction(`${digits[0]}.${digits.slice(1)}`)
    const expSign = exponent < 0 ? '-' : '+'
    const expDigits = String(Math.abs(exponent)).padStart(2, '0')
    return `${sign}${mantissa}e${expSign}${expDigits}`
  }

  if (exponent < 0) {
    return stripFraction(`${sign}0.${'0'.repeat(-exponent - 1)}${digits}`)
  }

  const padded = digits.padEnd(exponent + 1, '0')
  return stripFraction(
    `${sign}${padded.slice(0, exponent + 1)}.${padded.slice(exponent + 1)}`,
  )
}

/**
 * Whether rounding a positive double to 17 significant digits can hit an
 * exact tie. With the mantissa's trailing zero bits stripped the value is
 * `m · 2^-k` for odd `m`, i.e. `m · 5^k / 10^k`: its exact expansion is the
 * digits of `m · 5^k`, which end in 5 and number at least
 * `floor(k · log10 5) + 1` — more than 18 once `k > 25`, so no tie. An integer
 * below 2^53 has at most 16 digits and is never rounded at all.
 */
function g17TiePossible(abs: number): boolean {
  let { frac, exp } = buildFp(abs)
  if (exp >= 0) {
    return abs >= 2 ** 53
  }
  while ((frac & 1n) === 0n && exp < 0) {
    frac >>= 1n
    exp++
  }
  return exp < 0 && -exp <= 25
}

/**
 * A positive finite double as an exact `integer / 10^scale`. Every double is
 * `m · 2^e`, and for `e < 0` that is `m · 5^-e / 10^-e`.
 */
function exactScaled(value: number): { integer: bigint; scale: number } {
  const { frac, exp } = buildFp(value)
  if (exp >= 0) {
    return { integer: frac << BigInt(exp), scale: 0 }
  }
  return { integer: frac * 5n ** BigInt(-exp), scale: -exp }
}

/**
 * The exact decimal value of a positive finite double: its significant digits
 * (no leading or trailing zeros) and the decimal exponent of the first one.
 */
function exactDecimal(value: number): { digits: string; exponent: number } {
  const { integer, scale } = exactScaled(value)
  const text = integer.toString()
  return {
    digits: text.replace(/0+$/, ''),
    exponent: text.length - 1 - scale,
  }
}

// ---------------------------------------------------------------------------
// GEO coordinates: addReplyHumanLongDouble() before Redis 8.0
// ---------------------------------------------------------------------------

const HUMAN_LONG_DOUBLE_DECIMALS = 17

/**
 * The text of a GEOPOS / `WITHCOORD` coordinate. Redis 8.0 replies with
 * `addReplyDouble()`, i.e. {@link formatRedisDouble}'s `d2string()`; Redis
 * 6.2–7.4 and every Valkey use `addReplyHumanLongDouble()`, which is
 * `ld2string(…, LD_STR_HUMAN)`: `%.17Lf` with trailing zeros (and a bare `.`)
 * dropped, `-0` shown as `0`. Both are a `,` double on RESP3.
 */
export function formatGeoCoordinate(
  value: number,
  profile: DoubleFormatProfile = DEFAULT_PROFILE,
): string {
  if (profile.has('geo.coord-d2string')) {
    return formatRedisDouble(value, profile)
  }
  return formatHumanLongDouble(value)
}

/**
 * `snprintf("%.17Lf")` of a double promoted (exactly) to `long double`, then
 * trimmed the way `LD_STR_HUMAN` does. glibc rounds the exact value, ties to
 * even.
 */
function formatHumanLongDouble(value: number): string {
  if (!Number.isFinite(value)) {
    return formatRedisDouble(value)
  }
  if (value === 0) {
    return '0'
  }

  const sign = value < 0 ? '-' : ''
  let { integer, scale } = exactScaled(Math.abs(value))
  if (scale > HUMAN_LONG_DOUBLE_DECIMALS) {
    const divisor = 10n ** BigInt(scale - HUMAN_LONG_DOUBLE_DECIMALS)
    const quotient = integer / divisor
    const twice = (integer - quotient * divisor) * 2n
    const roundUp =
      twice > divisor || (twice === divisor && (quotient & 1n) === 1n)
    integer = roundUp ? quotient + 1n : quotient
    scale = HUMAN_LONG_DOUBLE_DECIMALS
  }

  const text = integer.toString().padStart(scale + 1, '0')
  const whole = text.slice(0, text.length - scale)
  const fraction = text.slice(text.length - scale).replace(/0+$/, '')
  const out = fraction ? `${whole}.${fraction}` : whole
  return out === '0' ? '0' : sign + out
}

function stripFraction(text: string): string {
  if (!text.includes('.')) {
    return text
  }
  return text.replace(/0+$/, '').replace(/\.$/, '')
}

// ---------------------------------------------------------------------------
// Redis 7.2+ / Valkey: d2string() → ll2string() or fpconv_dtoa()
// ---------------------------------------------------------------------------

/**
 * `d2string()` from `util.c`: `double2ll()` accepts an integer-valued double
 * within ±(LLONG_MAX / 2), i.e. ±2^62, and prints every digit (2^62 is
 * `4611686018427387904`, where JS `toString()` rounds the tail to `…388000`).
 * Anything else goes to `fpconv_dtoa`.
 */
function formatD2string(value: number): string {
  if (Number.isInteger(value) && Math.abs(value) <= 2 ** 62) {
    return BigInt(value).toString()
  }
  return fpconvDtoa(value)
}

// A port of Redis's vendored `deps/fpconv/fpconv_dtoa.c` (night-shift/fpconv,
// Grisu2), 64-bit unsigned arithmetic carried in BigInt. Grisu2 does not always
// find the shortest round-trip digits JS `toString()` would — 4.8911660955712037e-5
// stays 17 digits where JS prints 4.891166095571204e-5 — so the algorithm is
// ported rather than approximated.

const U64 = (1n << 64n) - 1n
const FRAC_MASK = 0x000fffffffffffffn
const EXP_MASK = 0x7ff0000000000000n
const HIDDEN_BIT = 0x0010000000000000n
const EXP_BIAS = 1023 + 52

const NPOWERS = 87
const STEP_POWERS = 8
const FIRST_POWER = -348
const EXP_MAX = -32
const EXP_MIN = -60

type Fp = { frac: bigint; exp: number }

/**
 * `powers_ten[]` from `fpconv_powers.h`: 10^(-348 + 8i) as a normalized 64-bit
 * significand (rounded to nearest) and binary exponent. Computed exactly
 * rather than transcribed.
 */
const POWERS_TEN: readonly Fp[] = Array.from({ length: NPOWERS }, (_, i) =>
  cachedPower(FIRST_POWER + i * STEP_POWERS),
)

function cachedPower(k: number): Fp {
  // 10^k = num / den exactly; find exp with 2^63 <= 10^k / 2^exp < 2^64.
  const num = k >= 0 ? 10n ** BigInt(k) : 1n
  const den = k >= 0 ? 1n : 10n ** BigInt(-k)
  let exp = Math.floor(k * Math.log2(10)) - 63
  for (;;) {
    const [n, d] =
      exp >= 0 ? [num, den << BigInt(exp)] : [num << BigInt(-exp), den]
    const quotient = n / d
    if (quotient < 1n << 63n) {
      exp--
      continue
    }
    if (quotient >= 1n << 64n) {
      exp++
      continue
    }
    const remainder = n - quotient * d
    const frac = remainder * 2n >= d ? quotient + 1n : quotient
    return frac === 1n << 64n
      ? { frac: 1n << 63n, exp: exp + 1 }
      : { frac, exp }
  }
}

const TENS: readonly bigint[] = Array.from(
  { length: 20 },
  (_, i) => 10n ** BigInt(19 - i),
)

const bitsView = new DataView(new ArrayBuffer(8))

function doubleBits(value: number): bigint {
  bitsView.setFloat64(0, value)
  return bitsView.getBigUint64(0)
}

function buildFp(value: number): Fp {
  const bits = doubleBits(value)
  const frac = bits & FRAC_MASK
  const exp = Number((bits & EXP_MASK) >> 52n)
  if (exp) {
    return { frac: frac + HIDDEN_BIT, exp: exp - EXP_BIAS }
  }
  return { frac, exp: -EXP_BIAS + 1 }
}

function normalize(fp: Fp): Fp {
  let { frac, exp } = fp
  while ((frac & HIDDEN_BIT) === 0n) {
    frac <<= 1n
    exp--
  }
  const shift = 64 - 52 - 1
  return { frac: (frac << BigInt(shift)) & U64, exp: exp - shift }
}

function normalizedBoundaries(fp: Fp): { lower: Fp; upper: Fp } {
  let upperFrac = (fp.frac << 1n) + 1n
  let upperExp = fp.exp - 1
  while ((upperFrac & (HIDDEN_BIT << 1n)) === 0n) {
    upperFrac <<= 1n
    upperExp--
  }
  const uShift = 64 - 52 - 2
  upperFrac = (upperFrac << BigInt(uShift)) & U64
  upperExp -= uShift

  const lShift = fp.frac === HIDDEN_BIT ? 2 : 1
  let lowerFrac = (fp.frac << BigInt(lShift)) - 1n
  const lowerExp = fp.exp - lShift
  lowerFrac = (lowerFrac << BigInt(lowerExp - upperExp)) & U64

  return {
    lower: { frac: lowerFrac, exp: upperExp },
    upper: { frac: upperFrac, exp: upperExp },
  }
}

function multiply(a: Fp, b: Fp): Fp {
  const LO = 0xffffffffn
  const ahBl = (a.frac >> 32n) * (b.frac & LO)
  const alBh = (a.frac & LO) * (b.frac >> 32n)
  const alBl = (a.frac & LO) * (b.frac & LO)
  const ahBh = (a.frac >> 32n) * (b.frac >> 32n)

  let tmp = (ahBl & LO) + (alBh & LO) + (alBl >> 32n)
  tmp += 1n << 31n // round up

  return {
    frac: (ahBh + (ahBl >> 32n) + (alBh >> 32n) + (tmp >> 32n)) & U64,
    exp: a.exp + b.exp + 64,
  }
}

function findCachedPow10(exp: number): { power: Fp; k: number } {
  const ONE_LOG_TEN = 0.30102999566398114
  const approx = Math.trunc(-(exp + NPOWERS) * ONE_LOG_TEN)
  let idx = Math.trunc((approx - FIRST_POWER) / STEP_POWERS)
  for (;;) {
    const current = exp + POWERS_TEN[idx].exp + 64
    if (current < EXP_MIN) {
      idx++
      continue
    }
    if (current > EXP_MAX) {
      idx--
      continue
    }
    return { power: POWERS_TEN[idx], k: FIRST_POWER + idx * STEP_POWERS }
  }
}

function roundDigit(
  digits: number[],
  delta: bigint,
  rem: bigint,
  kappa: bigint,
  frac: bigint,
): void {
  while (
    rem < frac &&
    delta - rem >= kappa &&
    (((rem + kappa) & U64) < frac || frac - rem > ((rem + kappa - frac) & U64))
  ) {
    digits[digits.length - 1]--
    rem = (rem + kappa) & U64
  }
}

function generateDigits(
  fp: Fp,
  upper: Fp,
  lower: Fp,
  K: number,
): { digits: number[]; K: number } {
  const wfrac = (upper.frac - fp.frac) & U64
  let delta = (upper.frac - lower.frac) & U64
  const shift = BigInt(-upper.exp)
  const one = 1n << shift

  let part1 = upper.frac >> shift
  let part2 = upper.frac & (one - 1n)
  const digits: number[] = []

  // 1000000000
  let kappa = 10
  for (let divIndex = 10; kappa > 0; divIndex++) {
    const div = TENS[divIndex]
    const digit = part1 / div
    if (digit || digits.length) {
      digits.push(Number(digit))
    }
    part1 -= digit * div
    kappa--

    const tmp = ((part1 << shift) & U64) + part2
    if (tmp <= delta) {
      roundDigit(digits, delta, tmp, (div << shift) & U64, wfrac)
      return { digits, K: K + kappa }
    }
  }

  // 10
  let unitIndex = 18
  for (;;) {
    part2 = (part2 * 10n) & U64
    delta = (delta * 10n) & U64
    kappa--

    const digit = part2 >> shift
    if (digit || digits.length) {
      digits.push(Number(digit))
    }
    part2 &= one - 1n
    if (part2 < delta) {
      roundDigit(digits, delta, part2, one, (wfrac * TENS[unitIndex]) & U64)
      return { digits, K: K + kappa }
    }
    unitIndex--
  }
}

function grisu2(value: number): { digits: number[]; K: number } {
  let w = buildFp(value)
  let { lower, upper } = normalizedBoundaries(w)
  w = normalize(w)

  const { power, k } = findCachedPow10(upper.exp)
  w = multiply(w, power)
  upper = multiply(upper, power)
  lower = multiply(lower, power)
  lower = { frac: (lower.frac + 1n) & U64, exp: lower.exp }
  upper = { frac: (upper.frac - 1n) & U64, exp: upper.exp }

  return generateDigits(w, upper, lower, -k)
}

function emitDigits(digits: string, K: number, neg: boolean): string {
  let ndigits = digits.length
  let exp = Math.abs(K + ndigits - 1)

  // write plain integer
  if (K >= 0 && exp < ndigits + 7) {
    return digits + '0'.repeat(K)
  }

  // write decimal w/o scientific notation
  if (K < 0 && (K > -7 || exp < 4)) {
    const offset = ndigits - Math.abs(K)
    if (offset <= 0) {
      return `0.${'0'.repeat(-offset)}${digits}`
    }
    return `${digits.slice(0, offset)}.${digits.slice(offset)}`
  }

  // write decimal w/ scientific notation
  ndigits = Math.min(ndigits, 18 - (neg ? 1 : 0))
  let out = digits[0]
  if (ndigits > 1) {
    out += `.${digits.slice(1, ndigits)}`
  }
  out += K + ndigits - 1 < 0 ? 'e-' : 'e+'

  let cent = 0
  if (exp > 99) {
    cent = Math.trunc(exp / 100)
    out += String(cent)
    exp -= cent * 100
  }
  if (exp > 9) {
    const dec = Math.trunc(exp / 10)
    out += String(dec)
    exp -= dec * 10
  } else if (cent) {
    out += '0'
  }
  return out + String(exp % 10)
}

/** `fpconv_dtoa()` for a finite, non-zero double. */
function fpconvDtoa(value: number): string {
  const neg = value < 0
  const { digits, K } = grisu2(value)
  return (neg ? '-' : '') + emitDigits(digits.join(''), K, neg)
}
