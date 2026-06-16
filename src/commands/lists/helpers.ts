import {
  PositiveCountError,
  RedisSyntaxError,
  TimeoutNegativeError,
  TimeoutNotFloatError,
} from '../../core/redis-error'
import type { RedisExecutionContext } from '../../core/redis-context'
import { RedisResult } from '../../core/redis-result'
import { RedisValue } from '../../core/redis-value'
import { array, bulk } from '../helpers'

export function resolveIndex(index: number, len: number): number {
  return index < 0 ? len + index : index
}

export function listRemove(
  values: Buffer[],
  count: number,
  element: Buffer,
): number {
  const target = element.toString('binary')
  let removed = 0

  if (count === 0) {
    for (let i = values.length - 1; i >= 0; i--) {
      if (values[i].toString('binary') === target) {
        values.splice(i, 1)
        removed++
      }
    }
  } else if (count > 0) {
    for (let i = 0; i < values.length && removed < count; i++) {
      if (values[i].toString('binary') === target) {
        values.splice(i, 1)
        removed++
        i--
      }
    }
  } else {
    const absCount = Math.abs(count)
    for (let i = values.length - 1; i >= 0 && removed < absCount; i--) {
      if (values[i].toString('binary') === target) {
        values.splice(i, 1)
        removed++
      }
    }
  }
  return removed
}

export function popList(
  args: { key: Buffer; count?: number },
  ctx: RedisExecutionContext,
  side: 'left' | 'right',
): RedisResult {
  const count = args.count
  if (count !== undefined && count < 0) {
    throw new PositiveCountError()
  }

  const list = ctx.db.getList(args.key)
  if (!list || list.values.length === 0) {
    return count === undefined
      ? bulk(null)
      : RedisResult.create(RedisValue.nullArray())
  }

  if (count === undefined) {
    const result = ctx.db.updateList(args.key, list => {
      const value = side === 'left' ? list.values.shift() : list.values.pop()
      return { value: value ?? null, empty: list.values.length === 0 }
    })
    if (result.empty) ctx.db.delete(args.key)
    return bulk(result.value)
  }

  if (count === 0) {
    return array([])
  }

  const result = ctx.db.updateList(args.key, list => {
    const values =
      side === 'left'
        ? list.values.splice(0, count)
        : list.values.splice(Math.max(0, list.values.length - count))
    return { values, empty: list.values.length === 0 }
  })
  if (result.empty) ctx.db.delete(args.key)

  return array(
    result.values.map((value: Buffer) => RedisValue.bulkString(value)),
  )
}

export function parseMoveDirection(
  token: Buffer | undefined,
): 'left' | 'right' {
  if (!token) throw new RedisSyntaxError()
  const direction = token.toString().toUpperCase()
  if (direction === 'LEFT') return 'left'
  if (direction === 'RIGHT') return 'right'
  throw new RedisSyntaxError()
}

export function parseTimeout(token: Buffer): number {
  const value = Number(token.toString())
  if (isNaN(value)) throw new TimeoutNotFloatError()
  if (value < 0) throw new TimeoutNegativeError()
  return value
}
