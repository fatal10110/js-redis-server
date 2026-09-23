import { RedisResult } from './redis-result'
import { RedisValue } from './redis-value'
import { formatRedisDouble, type DoubleFormatProfile } from './double-format'

export type RespVersion = 2 | 3

export type RespEncodeOptions = {
  version?: RespVersion
  /**
   * The server's compatibility profile. Decides how a `double` is spelled
   * (`%.17g` before Redis 7.2, `d2string()` after — see
   * {@link formatRedisDouble}); the default profile's spelling without it.
   */
  profile?: DoubleFormatProfile
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
    return encodeResp3(value, options?.profile)
  }

  return encodeResp2(value, options?.profile)
}

function encodeResp2(
  value: RedisValue,
  profile: DoubleFormatProfile | undefined,
): Buffer {
  switch (value.kind) {
    case 'simple-string':
      return Buffer.from(`+${value.value}\r\n`)
    case 'bulk-string':
      return encodeBulkString(value.value)
    case 'integer':
      return Buffer.from(`:${value.value.toString()}\r\n`)
    case 'double':
      return encodeBulkString(
        Buffer.from(value.text ?? formatRedisDouble(value.value, profile)),
      )
    case 'boolean':
      return Buffer.from(`:${value.value ? 1 : 0}\r\n`)
    case 'big-number':
      return encodeBulkString(Buffer.from(value.value.toString()))
    case 'verbatim':
      return encodeBulkString(value.value)
    case 'array':
      return encodeArray(value.items, profile)
    case 'set':
      return encodeArray(value.items, profile)
    case 'map':
      return encodeArray(
        value.entries.flatMap(([key, entryValue]) => [key, entryValue]),
        profile,
      )
    case 'map-pairs':
      return encodeArray(
        value.entries.map(([key, entryValue]) =>
          RedisValue.array([key, entryValue]),
        ),
        profile,
      )
    case 'flat-pairs':
      return encodeArray(
        value.entries.flatMap(([key, entryValue]) => [key, entryValue]),
        profile,
      )
    case 'push':
      return encodeArray(
        [
          { kind: 'bulk-string', value: Buffer.from(value.name) },
          ...value.items,
        ],
        profile,
      )
    case 'null':
      return encodeBulkString(null)
    case 'null-array':
      return Buffer.from('*-1\r\n')
    case 'error':
      return encodeError(value)
  }
}

function encodeResp3(
  value: RedisValue,
  profile: DoubleFormatProfile | undefined,
): Buffer {
  switch (value.kind) {
    case 'simple-string':
      return Buffer.from(`+${value.value}\r\n`)
    case 'bulk-string':
      return encodeResp3BlobString(value.value)
    case 'integer':
      return Buffer.from(`:${value.value.toString()}\r\n`)
    case 'double':
      return Buffer.from(
        `,${value.text ?? formatRedisDouble(value.value, profile)}\r\n`,
      )
    case 'boolean':
      return Buffer.from(value.value ? '#t\r\n' : '#f\r\n')
    case 'big-number':
      return Buffer.from(`(${value.value.toString()}\r\n`)
    case 'verbatim':
      return encodeResp3VerbatimString(value.format, value.value)
    case 'array':
      return encodeResp3Array(value.items, profile)
    case 'set':
      return encodeResp3Set(value.items, profile)
    case 'map':
      return encodeResp3Map(value.entries, profile)
    case 'map-pairs':
      return encodeResp3Map(value.entries, profile)
    case 'flat-pairs':
      return encodeResp3Array(
        value.entries.map(([key, entryValue]) =>
          RedisValue.array([key, entryValue]),
        ),
        profile,
      )
    case 'push':
      return encodeResp3Push(value.name, value.items, profile)
    case 'null':
    case 'null-array':
      return Buffer.from('_\r\n')
    case 'error':
      return encodeError(value)
  }
}

function encodeArray(
  items: readonly RedisValue[],
  profile: DoubleFormatProfile | undefined,
): Buffer {
  return Buffer.concat([
    Buffer.from(`*${items.length}\r\n`),
    ...items.map(item => encodeResp2(item, profile)),
  ])
}

function encodeResp3Array(
  items: readonly RedisValue[],
  profile: DoubleFormatProfile | undefined,
): Buffer {
  return Buffer.concat([
    Buffer.from(`*${items.length}\r\n`),
    ...items.map(item => encodeResp3(item, profile)),
  ])
}

function encodeResp3Set(
  items: readonly RedisValue[],
  profile: DoubleFormatProfile | undefined,
): Buffer {
  return Buffer.concat([
    Buffer.from(`~${items.length}\r\n`),
    ...items.map(item => encodeResp3(item, profile)),
  ])
}

function encodeResp3Map(
  entries: readonly [RedisValue, RedisValue][],
  profile: DoubleFormatProfile | undefined,
): Buffer {
  const frames: Buffer[] = [Buffer.from(`%${entries.length}\r\n`)]
  for (const [key, value] of entries) {
    frames.push(encodeResp3(key, profile), encodeResp3(value, profile))
  }
  return Buffer.concat(frames)
}

function encodeResp3Push(
  name: string,
  items: readonly RedisValue[],
  profile: DoubleFormatProfile | undefined,
): Buffer {
  return Buffer.concat([
    Buffer.from(`>${items.length + 1}\r\n`),
    encodeResp3BlobString(Buffer.from(name)),
    ...items.map(item => encodeResp3(item, profile)),
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

/**
 * Build a `-ERR ...` frame. The body is assembled as *bytes* rather than a
 * string so an error that echoes a token the client sent (an unknown
 * subcommand, say) reaches the wire byte for byte — real Redis echoes whatever
 * the client wrote, which need not be valid UTF-8, and a `toString()` round
 * trip would replace those bytes with U+FFFD.
 */
function encodeError(value: Extract<RedisValue, { kind: 'error' }>): Buffer {
  const body = value.messageBytes ?? Buffer.from(value.message)
  return Buffer.concat([
    Buffer.from('-'),
    sanitizeErrorBytes(
      value.code ? Buffer.concat([Buffer.from(`${value.code} `), body]) : body,
    ),
    Buffer.from('\r\n'),
  ])
}

/**
 * Replace the two bytes that would end an error frame early. Real Redis uses
 * `sdsmapchars(s, "\r\n", "  ", 2)`, a 1:1 character map — so a `\r\n` run
 * becomes *two* spaces, not one. Collapsing runs is protocol-safe but changes
 * the byte count of every error reply that carries a newline (#388).
 *
 * Done on bytes rather than characters: `\r` and `\n` are single-byte, so the
 * result is the same for UTF-8 text, and it also works on a body that is not
 * valid UTF-8 at all.
 */
function sanitizeErrorBytes(value: Buffer): Buffer {
  const CR = 0x0d
  const LF = 0x0a
  const SPACE = 0x20
  let sanitized: Buffer | null = null
  for (let i = 0; i < value.length; i++) {
    if (value[i] !== CR && value[i] !== LF) {
      continue
    }
    sanitized ??= Buffer.from(value)
    sanitized[i] = SPACE
  }

  return sanitized ?? value
}
