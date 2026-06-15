import { RedisValue } from '../core/redis-value'
import { RedisResult } from '../core/redis-result'
import { isIntegerToken } from '../core/command-schema'
import {
  ExpectedIntegerError,
  InvalidExpireTimeError,
  RedisSyntaxError,
  WrongTypeRedisError,
} from '../core/redis-error'
import type { RedisDataTypeName, RedisDatabase } from '../state'

export function ok(): RedisResult {
  return RedisResult.ok()
}

export function bulk(value: Buffer | null): RedisResult {
  return RedisResult.create(RedisValue.bulkString(value))
}

export function integer(value: number | bigint): RedisResult {
  return RedisResult.create(RedisValue.integer(value))
}

export function scoreBuffer(score: number): Buffer {
  if (score === Infinity) return Buffer.from('inf')
  if (score === -Infinity) return Buffer.from('-inf')
  return Buffer.from(score.toString())
}

export function simpleString(value: string): RedisResult {
  return RedisResult.create(RedisValue.simpleString(value))
}

export function array(items: RedisValue[]): RedisResult {
  return RedisResult.create(RedisValue.array(items))
}

export function ensureStringOrMissing(
  db: RedisDatabase,
  key: Buffer,
): Buffer | null {
  const type = db.getType(key)
  if (type === null) {
    return null
  }

  if (type !== 'string') {
    throw new WrongTypeRedisError()
  }

  return db.getString(key)
}

export function typeName(type: RedisDataTypeName | null): string {
  return type ?? 'none'
}

export function ttlSeconds(expiresAt: number): number {
  // Redis rounds remaining time to the nearest second ((ms+500)/1000),
  // not ceil/floor — matches EXPIRETIME and real TTL behavior.
  return Math.max(0, Math.round((expiresAt - Date.now()) / 1000))
}

export function ttlMilliseconds(expiresAt: number): number {
  return Math.max(0, expiresAt - Date.now())
}

export function parseIntegerToken(token: Buffer): number {
  const raw = token.toString()
  if (!isIntegerToken(raw)) {
    throw new ExpectedIntegerError()
  }

  const value = Number(raw)
  if (!Number.isSafeInteger(value)) {
    throw new ExpectedIntegerError()
  }

  return value
}

export function parsePositiveExpireToken(
  token: Buffer,
  commandName: string,
): number {
  const value = parseIntegerToken(token)
  if (value <= 0) {
    throw new InvalidExpireTimeError(commandName)
  }

  return value
}

export function requireNextOptionValue(
  args: readonly Buffer[],
  index: number,
): Buffer {
  const value = args[index]
  if (!value) {
    throw new RedisSyntaxError()
  }

  return value
}
