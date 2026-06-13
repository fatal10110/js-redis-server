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
      return encodeBulkString(Buffer.from(formatNumber(value.value)))
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
      return Buffer.from(`,${formatNumber(value.value)}\r\n`)
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

function formatNumber(value: number): string {
  if (Number.isNaN(value)) {
    return 'nan'
  }

  if (value === Infinity) {
    return 'inf'
  }

  if (value === -Infinity) {
    return '-inf'
  }

  if (Object.is(value, -0)) {
    return '-0'
  }

  return value.toString()
}

function sanitizeErrorText(value: string): string {
  return value.replace(/[\r\n]+/g, ' ')
}
