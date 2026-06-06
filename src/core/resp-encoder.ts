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
  if (version !== 2) {
    throw new Error('RESP3 encoding is not implemented yet')
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

function encodeArray(items: readonly RedisValue[]): Buffer {
  return Buffer.concat([
    Buffer.from(`*${items.length}\r\n`),
    ...items.map(item => encodeResp2(item)),
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
