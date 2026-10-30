/**
 * Operand parsing for the `long double` commands, INCRBYFLOAT and
 * HINCRBYFLOAT (#234).
 *
 * Redis parses both their increment and the value already stored with
 * `string2ld()` (util.c), a thin wrapper around C `strtold`. That is a
 * different grammar from the decimal-only float tokens the rest of the server
 * accepts, and it is the same on every Redis and Valkey release this server
 * models (6.2 through 8.0, Valkey 7.2 through 9.0), so it is not
 * profile-gated. `string2ld()` refuses:
 *
 *  - an empty token, or one of 5120 bytes or more (`MAX_LONG_DOUBLE_CHARS`);
 *  - a token starting with whitespace, or with anything left after the
 *    number (including a NUL byte): the whole token must be the number;
 *  - NaN, in any spelling `strtold` knows (`nan`, `-nan`, `nan(1)`);
 *  - a value that overflows `long double` (`1e5000`, `0x1p16384`), or a
 *    nonzero one that underflows to zero (`1e-5000`, `0x1p-16446`).
 *
 * What it accepts on top of plain decimals: C99 hex floats (`0x10`, `0X1.8p3`,
 * `-0x.8`, `0x1e5` — `e` is a hex digit there), and the infinity literals
 * `inf` / `infinity` in any case with an optional sign. Binary and octal
 * prefixes (`0b11`, `0o7`) are not C syntax and stay invalid; a leading zero
 * (`010`) is just a decimal ten.
 *
 * The limits are those of the x86-64 80-bit `long double` Redis is built with
 * on amd64: its smallest subnormal is 2^-16445, so under round-to-nearest-even
 * a nonzero value of at most 2^-16446 becomes zero and is refused.
 *
 * Known limit: the arithmetic here is JavaScript `double`, not `long double`
 * (#512). A value inside the `long double` range but beyond the `double` one
 * (`1e400`, `0x1p1024`), which real Redis accepts, cannot be represented and
 * is refused as an invalid float instead.
 */

/** `MAX_LONG_DOUBLE_CHARS` in Redis' util.h: a token must be shorter. */
const MAX_LONG_DOUBLE_CHARS = 5 * 1024

/**
 * A nonzero value at or below 2^LONG_DOUBLE_ZERO_EXPONENT rounds to zero in an
 * 80-bit `long double` (half the smallest subnormal, 2^-16445, ties to even).
 */
const LONG_DOUBLE_ZERO_EXPONENT = -16446

const INFINITY_PATTERN = /^([+-]?)inf(?:inity)?$/i
const DECIMAL_PATTERN = /^([+-]?)(\d*)(?:\.(\d*))?(?:[eE]([+-]?\d+))?$/
const HEX_PATTERN =
  /^([+-]?)0[xX]([0-9a-fA-F]*)(?:\.([0-9a-fA-F]*))?(?:[pP]([+-]?\d+))?$/

/**
 * Parse `raw` the way Redis' `string2ld()` does. Returns `undefined` for a
 * token Redis refuses ("value is not a valid float" / "hash value is not a
 * float" at the call site), `+/-Infinity` for an infinity literal (a valid
 * operand that the caller's own NaN/Infinity check then rejects), and the
 * number otherwise.
 */
export function parseLongDoubleToken(raw: Buffer | string): number | undefined {
  const text = typeof raw === 'string' ? raw : raw.toString('latin1')
  const length = typeof raw === 'string' ? Buffer.byteLength(raw) : raw.length
  if (length === 0 || length >= MAX_LONG_DOUBLE_CHARS) {
    return undefined
  }

  const infinity = INFINITY_PATTERN.exec(text)
  if (infinity) {
    return infinity[1] === '-' ? -Infinity : Infinity
  }

  const hex = HEX_PATTERN.exec(text)
  if (hex) {
    return parseHexFloat(hex[1] === '-', hex[2]!, hex[3] ?? '', hex[4])
  }

  const decimal = DECIMAL_PATTERN.exec(text)
  if (decimal) {
    return parseDecimalFloat(text, decimal[2]!, decimal[3] ?? '', decimal[4])
  }

  return undefined
}

function parseDecimalFloat(
  text: string,
  intDigits: string,
  fracDigits: string,
  exponentText: string | undefined,
): number | undefined {
  if (intDigits.length === 0 && fracDigits.length === 0) {
    return undefined
  }

  // V8's Number() rounds a decimal string of any length correctly.
  const value = Number(text)
  if (!Number.isFinite(value)) {
    return undefined
  }
  if (value !== 0) {
    return value
  }

  // Zero in a double: either a genuine zero, or a tiny value that a
  // `long double` may or may not still hold.
  const digits = (intDigits + fracDigits).replace(/^0+/, '')
  if (digits.length === 0) {
    return value
  }

  // value = digits * 10^exponent
  const exponent = clampExponent(exponentText) - fracDigits.length
  // 10^(order) <= value < 10^(order + 1)
  const order = digits.length - 1 + exponent
  // 2^-16446 is about 1.82e-4951.
  if (order < -4951) {
    return undefined
  }
  if (order > -4951) {
    return value
  }

  // Exact comparison: value <= 2^-16446  <=>  digits * 2^16446 <= 10^-exponent.
  const scaled = BigInt(digits) << BigInt(-LONG_DOUBLE_ZERO_EXPONENT)
  return scaled <= 10n ** BigInt(-exponent) ? undefined : value
}

function parseHexFloat(
  negative: boolean,
  intDigits: string,
  fracDigits: string,
  exponentText: string | undefined,
): number | undefined {
  if (intDigits.length === 0 && fracDigits.length === 0) {
    return undefined
  }

  const sign = negative ? -1 : 1
  const mantissa = BigInt(`0x${intDigits}${fracDigits}`)
  if (mantissa === 0n) {
    return sign * 0
  }

  // value = mantissa * 2^exponent
  const exponent = clampExponent(exponentText) - 4 * fracDigits.length
  const bits = mantissa.toString(2).length
  const top = bits - 1 + exponent
  const isPowerOfTwo = (mantissa & (mantissa - 1n)) === 0n
  if (
    top < LONG_DOUBLE_ZERO_EXPONENT ||
    (top === LONG_DOUBLE_ZERO_EXPONENT && isPowerOfTwo)
  ) {
    return undefined
  }

  const value = sign * binaryToDouble(mantissa, exponent, bits)
  return Number.isFinite(value) ? value : undefined
}

/**
 * The double nearest `mantissa * 2^exponent` (ties to even), for a positive
 * `mantissa` of `bits` bits: round the mantissa to the precision the result's
 * binade allows (53 bits, fewer in the subnormal range), then scale it by
 * powers of two, which is exact.
 */
export function binaryToDouble(
  mantissa: bigint,
  exponent: number,
  bits: number,
): number {
  const top = bits - 1 + exponent
  if (top > 1023) {
    return Infinity
  }

  // Significant bits available at this magnitude: 53 for a normal double, one
  // fewer per binade below 2^-1022, none once below 2^-1075.
  const precision = top >= -1022 ? 53 : top + 1075
  const shift = bits - precision
  let rounded = mantissa
  let scale = exponent
  if (shift > 0) {
    const dropped = BigInt(shift)
    rounded = mantissa >> dropped
    const remainder = mantissa - (rounded << dropped)
    const half = 1n << (dropped - 1n)
    if (remainder > half || (remainder === half && (rounded & 1n) === 1n)) {
      rounded += 1n
    }
    scale += shift
  }

  return scaleByPowerOfTwo(Number(rounded), scale)
}

/**
 * `value * 2^exponent` in steps small enough that every intermediate is a
 * normal double. The caller guarantees the result is exactly representable
 * (or overflows), so no step rounds.
 */
function scaleByPowerOfTwo(value: number, exponent: number): number {
  let result = value
  let remaining = exponent
  while (remaining > 1000) {
    result *= 2 ** 1000
    remaining -= 1000
  }
  while (remaining < -1000) {
    result *= 2 ** -1000
    remaining += 1000
  }
  return result * 2 ** remaining
}

/**
 * The exponent's value, clamped far outside any range that matters so an
 * absurd `1e99999999999999999999` stays a plain number: the token is at most
 * 5119 bytes, so its digits shift the value by fewer than 5119 decimal (or
 * 20476 binary) places, well inside the clamp.
 */
export function clampExponent(exponentText: string | undefined): number {
  if (exponentText === undefined) {
    return 0
  }
  const value = Number(exponentText)
  return Math.max(-1e6, Math.min(1e6, value))
}

/**
 * The smallest value an 80-bit `long double` rounds to infinity:
 * (2^65 - 1) * 2^16319, halfway between the largest finite one,
 * (2^64 - 1) * 2^16320, and 2^16384. `strtold` reports it as a range error,
 * which `string2ld()` refuses.
 */
const LONG_DOUBLE_OVERFLOW_MANTISSA = (1n << 65n) - 1n
const LONG_DOUBLE_OVERFLOW_EXPONENT = 16319

/**
 * {@link parseLongDoubleToken}, except that a finite `long double` beyond the
 * `double` range (`1e400`, `-0x1p1024`) comes back as `+/-Infinity` instead of
 * being refused. For callers that only compare the value against a bound:
 * blocking timeouts (#577), where real Redis takes `1e400` and then answers
 * that it is out of range (or negative, before 7.2).
 */
export function parseLongDoubleMagnitude(
  raw: Buffer | string,
): number | undefined {
  const value = parseLongDoubleToken(raw)
  if (value !== undefined) {
    return value
  }

  const text = typeof raw === 'string' ? raw : raw.toString('latin1')
  const length = typeof raw === 'string' ? Buffer.byteLength(raw) : raw.length
  if (length === 0 || length >= MAX_LONG_DOUBLE_CHARS) {
    return undefined
  }

  const hex = HEX_PATTERN.exec(text)
  if (hex) {
    const fracDigits = hex[3] ?? ''
    const digits = hex[2]! + fracDigits
    if (digits.length === 0) {
      return undefined
    }
    const mantissa = BigInt(`0x${digits}`)
    if (mantissa === 0n) {
      return undefined
    }
    const exponent = clampExponent(hex[4]) - 4 * fracDigits.length
    const bits = mantissa.toString(2).length
    // Finite as a double: refused for underflow, not overflow.
    if (Number.isFinite(binaryToDouble(mantissa, exponent, bits))) {
      return undefined
    }
    const top = bits - 1 + exponent
    const overflows =
      top > LONG_DOUBLE_OVERFLOW_EXPONENT + 64 ||
      (top === LONG_DOUBLE_OVERFLOW_EXPONENT + 64 &&
        mantissa << BigInt(exponent - LONG_DOUBLE_OVERFLOW_EXPONENT) >=
          LONG_DOUBLE_OVERFLOW_MANTISSA)
    if (overflows) {
      return undefined
    }
    return hex[1] === '-' ? -Infinity : Infinity
  }

  const decimal = DECIMAL_PATTERN.exec(text)
  if (!decimal) {
    return undefined
  }
  const fracDigits = decimal[3] ?? ''
  const digits = (decimal[2]! + fracDigits).replace(/^0+/, '')
  // Finite as a double (or no digits at all): refused for another reason.
  if (digits.length === 0 || Number.isFinite(Number(text))) {
    return undefined
  }
  // value = digits * 10^exponent, and 10^order <= value < 10^(order + 1)
  const exponent = clampExponent(decimal[4]) - fracDigits.length
  const order = digits.length - 1 + exponent
  // The overflow bound is about 1.19e4932.
  if (order > 4932) {
    return undefined
  }
  if (order === 4932) {
    const scaledValue =
      BigInt(digits) * (exponent > 0 ? 10n ** BigInt(exponent) : 1n)
    const scaledBound =
      (LONG_DOUBLE_OVERFLOW_MANTISSA << BigInt(LONG_DOUBLE_OVERFLOW_EXPONENT)) *
      (exponent < 0 ? 10n ** BigInt(-exponent) : 1n)
    if (scaledValue >= scaledBound) {
      return undefined
    }
  }
  return decimal[1] === '-' ? -Infinity : Infinity
}

/** A binary float: `(negative ? -1 : 1) * mantissa * 2^exponent`. */
export interface BinaryFloat {
  negative: boolean
  mantissa: bigint
  exponent: number
}

/**
 * Stand-ins far outside the range a timeout cares about: any value this
 * small times 1000 is strictly between 0 and 1, any value this large times
 * 1000 is past LLONG_MAX. They keep absurd exponents out of BigInt powers.
 */
const TINY_EXPONENT = -400
const HUGE_EXPONENT = 1000

/** Round the positive `num / den * 2^exp2` to a 64-bit significand, ties to even. */
function roundToLongDouble(
  num: bigint,
  den: bigint,
  exp2: number,
): { mantissa: bigint; exponent: number } {
  let exponent = num.toString(2).length - den.toString(2).length - 64 + exp2
  for (;;) {
    const shift = exp2 - exponent
    const scaledNum = shift > 0 ? num << BigInt(shift) : num
    const scaledDen = shift < 0 ? den << BigInt(-shift) : den
    let mantissa = scaledNum / scaledDen
    if (mantissa >= 1n << 64n) {
      exponent += 1
      continue
    }
    if (mantissa < 1n << 63n) {
      exponent -= 1
      continue
    }
    const twiceRest = (scaledNum % scaledDen) * 2n
    if (
      twiceRest > scaledDen ||
      (twiceRest === scaledDen && (mantissa & 1n) === 1n)
    ) {
      mantissa += 1n
      if (mantissa === 1n << 64n) {
        return { mantissa: 1n << 63n, exponent: exponent + 1 }
      }
    }
    return { mantissa, exponent }
  }
}

/**
 * `strtold(raw) * 1000.0` the way an x86 80-bit `long double` computes it:
 * the token rounds to a 64-bit significand, then the product rounds again.
 * This is how `getTimeoutFromObjectOrReply` turns seconds into milliseconds,
 * so `0.001` comes out a hair under 1 (`ceill` 1, a cast 0) and `-0.001` a
 * hair above -1 (0 either way) (#577).
 *
 * Call it only on a token {@link parseLongDoubleMagnitude} accepts. Values
 * whose product is far below 1 or far past LLONG_MAX (infinity included)
 * come back as stand-ins of the same sign that compare the same way.
 */
export function longDoubleTimesThousand(raw: Buffer | string): BinaryFloat {
  const text = typeof raw === 'string' ? raw : raw.toString('latin1')
  const infinity = INFINITY_PATTERN.exec(text)
  if (infinity) {
    return {
      negative: infinity[1] === '-',
      mantissa: 1n,
      exponent: HUGE_EXPONENT,
    }
  }

  let negative: boolean
  let value: { mantissa: bigint; exponent: number }
  const hex = HEX_PATTERN.exec(text)
  const decimal = hex ? null : DECIMAL_PATTERN.exec(text)
  if (hex) {
    negative = hex[1] === '-'
    const fracDigits = hex[3] ?? ''
    const mantissa = BigInt(`0x${hex[2]! + fracDigits || '0'}`)
    if (mantissa === 0n) {
      return { negative, mantissa: 0n, exponent: 0 }
    }
    const exponent = clampExponent(hex[4]) - 4 * fracDigits.length
    const top = mantissa.toString(2).length - 1 + exponent
    if (top < TINY_EXPONENT || top > HUGE_EXPONENT) {
      return {
        negative,
        mantissa: 1n,
        exponent: top < TINY_EXPONENT ? TINY_EXPONENT : HUGE_EXPONENT,
      }
    }
    value = roundToLongDouble(mantissa, 1n, exponent)
  } else if (decimal) {
    negative = decimal[1] === '-'
    const fracDigits = decimal[3] ?? ''
    const digits = (decimal[2]! + fracDigits).replace(/^0+/, '')
    if (digits.length === 0) {
      return { negative, mantissa: 0n, exponent: 0 }
    }
    const exponent = clampExponent(decimal[4]) - fracDigits.length
    const order = digits.length - 1 + exponent
    if (order < -100 || order > 100) {
      return {
        negative,
        mantissa: 1n,
        exponent: order < -100 ? TINY_EXPONENT : HUGE_EXPONENT,
      }
    }
    value = roundToLongDouble(
      BigInt(digits) * 10n ** BigInt(Math.max(exponent, 0)),
      10n ** BigInt(Math.max(-exponent, 0)),
      0,
    )
  } else {
    throw new TypeError(`not a long double token: ${text}`)
  }

  const product = roundToLongDouble(value.mantissa * 1000n, 1n, value.exponent)
  return { negative, ...product }
}
