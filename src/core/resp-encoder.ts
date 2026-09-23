import { RedisResult } from './redis-result'
import { RedisValue } from './redis-value'

export type RespVersion = 2 | 3

export type RespEncodeOptions = {
  version?: RespVersion
}

export function encodeRedisResult(
  result: RedisResult,
  options?: RespEncodeOptions,
): Buffer {
  if (result.encoded) {
    return Buffer.from(result.encoded)
  }

  return encodeRedisValue(result.value, options)
}

export function encodeRedisValue(
  value: RedisValue,
  options?: RespEncodeOptions,
): Buffer {
  const version = options?.version ?? 2
  if (version === 3) {
    return encodeResp3(value)
  }

  return encodeResp2(value)
}

function encodeResp2(value: RedisValue): Buffer {
  switch (value.kind) {
    case 'simple-string':
      return Buffer.from(`+${value.value}\r\n`)
    case 'bulk-string':
      return encodeBulkString(value.value)
    case 'integer':
      return Buffer.from(`:${value.value.toString()}\r\n`)
    case 'double':
      return encodeBulkString(Buffer.from(formatRedisDouble(value.value)))
    case 'boolean':
      return Buffer.from(`:${value.value ? 1 : 0}\r\n`)
    case 'big-number':
      return encodeBulkString(Buffer.from(value.value.toString()))
    case 'verbatim':
      return encodeBulkString(value.value)
    case 'array':
      return encodeArray(value.items)
    case 'set':
      return encodeArray(value.items)
    case 'map':
      return encodeArray(
        value.entries.flatMap(([key, entryValue]) => [key, entryValue]),
      )
    case 'map-pairs':
      return encodeArray(
        value.entries.map(([key, entryValue]) =>
          RedisValue.array([key, entryValue]),
        ),
      )
    case 'flat-pairs':
      return encodeArray(
        value.entries.flatMap(([key, entryValue]) => [key, entryValue]),
      )
    case 'push':
      return encodeArray([
        { kind: 'bulk-string', value: Buffer.from(value.name) },
        ...value.items,
      ])
    case 'null':
      return encodeBulkString(null)
    case 'null-array':
      return Buffer.from('*-1\r\n')
    case 'error':
      return Buffer.from(`-${formatError(value)}\r\n`)
  }
}

function encodeResp3(value: RedisValue): Buffer {
  switch (value.kind) {
    case 'simple-string':
      return Buffer.from(`+${value.value}\r\n`)
    case 'bulk-string':
      return encodeResp3BlobString(value.value)
    case 'integer':
      return Buffer.from(`:${value.value.toString()}\r\n`)
    case 'double':
      return Buffer.from(`,${formatRedisDouble(value.value)}\r\n`)
    case 'boolean':
      return Buffer.from(value.value ? '#t\r\n' : '#f\r\n')
    case 'big-number':
      return Buffer.from(`(${value.value.toString()}\r\n`)
    case 'verbatim':
      return encodeResp3VerbatimString(value.format, value.value)
    case 'array':
      return encodeResp3Array(value.items)
    case 'set':
      return encodeResp3Set(value.items)
    case 'map':
      return encodeResp3Map(value.entries)
    case 'map-pairs':
      return encodeResp3Map(value.entries)
    case 'flat-pairs':
      return encodeResp3Array(
        value.entries.map(([key, entryValue]) =>
          RedisValue.array([key, entryValue]),
        ),
      )
    case 'push':
      return encodeResp3Push(value.name, value.items)
    case 'null':
    case 'null-array':
      return Buffer.from('_\r\n')
    case 'error':
      return Buffer.from(`-${formatError(value)}\r\n`)
  }
}

function encodeArray(items: readonly RedisValue[]): Buffer {
  return Buffer.concat([
    Buffer.from(`*${items.length}\r\n`),
    ...items.map(item => encodeResp2(item)),
  ])
}

function encodeResp3Array(items: readonly RedisValue[]): Buffer {
  return Buffer.concat([
    Buffer.from(`*${items.length}\r\n`),
    ...items.map(item => encodeResp3(item)),
  ])
}

function encodeResp3Set(items: readonly RedisValue[]): Buffer {
  return Buffer.concat([
    Buffer.from(`~${items.length}\r\n`),
    ...items.map(item => encodeResp3(item)),
  ])
}

function encodeResp3Map(entries: readonly [RedisValue, RedisValue][]): Buffer {
  const frames: Buffer[] = [Buffer.from(`%${entries.length}\r\n`)]
  for (const [key, value] of entries) {
    frames.push(encodeResp3(key), encodeResp3(value))
  }
  return Buffer.concat(frames)
}

function encodeResp3Push(name: string, items: readonly RedisValue[]): Buffer {
  return Buffer.concat([
    Buffer.from(`>${items.length + 1}\r\n`),
    encodeResp3BlobString(Buffer.from(name)),
    ...items.map(item => encodeResp3(item)),
  ])
}

function encodeBulkString(value: Buffer | null): Buffer {
  if (value === null) {
    return Buffer.from('$-1\r\n')
  }

  return Buffer.concat([
    Buffer.from(`$${value.length}\r\n`),
    value,
    Buffer.from('\r\n'),
  ])
}

function encodeResp3BlobString(value: Buffer | null): Buffer {
  if (value === null) {
    return Buffer.from('_\r\n')
  }

  return encodeBulkString(value)
}

function encodeResp3VerbatimString(format: string, value: Buffer): Buffer {
  if (Buffer.byteLength(format) !== 3) {
    throw new Error(
      `RESP3 verbatim string format must be exactly 3 bytes, got ${JSON.stringify(format)}`,
    )
  }

  const payload = Buffer.concat([Buffer.from(`${format}:`), value])
  return Buffer.concat([
    Buffer.from(`=${payload.length}\r\n`),
    payload,
    Buffer.from('\r\n'),
  ])
}

function formatError(value: Extract<RedisValue, { kind: 'error' }>): string {
  const message = sanitizeErrorText(value.message)
  return value.code ? `${sanitizeErrorText(value.code)} ${message}` : message
}

/**
 * Largest magnitude Redis prints through its integer path: `double2ll()` in
 * `util.c` rejects anything past `LLONG_MAX / 2` as a double, which is 2^62.
 */
const REDIS_DOUBLE_INTEGER_LIMIT = 2 ** 62

/**
 * Render a `double` the way Redis 7.2+ spells it (`d2string()` in `util.c`),
 * which is the text of both a RESP3 `,` double and the RESP2 bulk string a
 * client reads back — `decodeRedisValue` shares this so the two cannot drift.
 *
 *  - `nan` / `inf` / `-inf`, and `-0` kept distinct from `0`.
 *  - An integer-valued double within ±2^62 prints every digit, exactly
 *    (`ll2string`): 2^62 is `4611686018427387904`, where JS's `toString` would
 *    round the tail to `…388000`.
 *  - Anything else goes through `fpconv_dtoa`: the shortest round-trip digits,
 *    which JS computes too, laid out by fpconv's own rules rather than JS's.
 *    Plain digits while the trailing zeros number fewer than 8 (`5e18` is
 *    `5e+18`, `1.2345678901234568e22` stays plain); a plain decimal while the
 *    last digit sits fewer than 7 places after the point, or the magnitude is
 *    below 10^4 (`0.000123`, but `1.23e-5`); exponent form otherwise.
 *
 * fpconv is Grisu2-based, which can in rare cases emit a longer digit string
 * than the shortest one JS picks; those values are not matched here. Nor is
 * the `redis-6.2` profile: Redis 6.2 prints `%.17g` (`0.1` is
 * `0.10000000000000001` there), and the encoder is not profile-aware.
 * (`redis-7.0` has not been checked.)
 */
export function formatRedisDouble(value: number): string {
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

  if (
    Number.isInteger(value) &&
    Math.abs(value) <= REDIS_DOUBLE_INTEGER_LIMIT
  ) {
    return BigInt(value).toString()
  }

  return formatFpconv(value)
}

/**
 * `emit_digits()` from Redis's vendored `fpconv_dtoa.c`, fed the shortest
 * round-trip digits that `toExponential()` yields.
 */
function formatFpconv(value: number): string {
  const exponential = value.toExponential()
  const [mantissa, exponentText] = exponential.split('e')
  const sign = value < 0 ? '-' : ''
  const digits = mantissa.replace('-', '').replace('.', '')
  // Decimal exponent of the *last* digit, as fpconv's `K`.
  const lastDigitExponent = Number(exponentText) - (digits.length - 1)
  const magnitude = Math.abs(Number(exponentText))

  if (lastDigitExponent >= 0) {
    if (magnitude < digits.length + 7) {
      return sign + digits + '0'.repeat(lastDigitExponent)
    }
    return exponential
  }

  if (lastDigitExponent > -7 || magnitude < 4) {
    const pointAt = digits.length + lastDigitExponent
    if (pointAt <= 0) {
      return `${sign}0.${'0'.repeat(-pointAt)}${digits}`
    }
    return `${sign}${digits.slice(0, pointAt)}.${digits.slice(pointAt)}`
  }

  return exponential
}

/**
 * Replace the two bytes that would end an error frame early. Real Redis uses
 * `sdsmapchars(s, "\r\n", "  ", 2)`, a 1:1 character map — so a `\r\n` run
 * becomes *two* spaces, not one. Collapsing runs is protocol-safe but changes
 * the byte count of every error reply that carries a newline (#388).
 */
function sanitizeErrorText(value: string): string {
  return value.replace(/[\r\n]/g, ' ')
}
