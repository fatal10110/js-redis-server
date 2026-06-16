import {
  SchemaMismatchError,
  t,
  type ParseContext,
} from '../../core/command-schema'
import {
  ExpectedIntegerError,
  RedisSyntaxError,
  StreamLimitNegativeError,
  StreamLimitRequiresApproxError,
  WrongNumberOfArgumentsError,
} from '../../core/redis-error'
import type { RedisStreamData, StreamId } from '../../state/data-types'
import { compareStreamId, parseExactId, parseUint64 } from './ids'
import { updateMaxDeletedId } from './groups'

// Trim specification shared by XADD and XTRIM.
export type TrimSpec =
  | {
      strategy: 'maxlen'
      count: bigint
      approximate: boolean
      limit: bigint | null
    }
  | {
      strategy: 'minid'
      minId: StreamId
      approximate: boolean
      limit: bigint | null
    }

function parseTrimLimit(raw: string): bigint {
  if (/^-\d+$/.test(raw)) {
    throw new StreamLimitNegativeError()
  }

  const value = parseUint64(raw)
  if (value === null) {
    throw new ExpectedIntegerError()
  }

  return value
}

export function createTrimSpecSchema() {
  return t.custom<TrimSpec>(
    (input: readonly Buffer[], index: number, ctx: ParseContext) => {
      if (index >= input.length) throw new SchemaMismatchError()

      const keyword = input[index].toString().toUpperCase()
      if (keyword !== 'MAXLEN' && keyword !== 'MINID')
        throw new SchemaMismatchError()

      let cursor = index + 1
      let approximate = false
      if (cursor < input.length && input[cursor].toString() === '~') {
        approximate = true
        cursor++
      }

      const rawValue = input[cursor]?.toString()
      if (rawValue === undefined)
        throw new WrongNumberOfArgumentsError(ctx.commandName)
      cursor++

      let trim: TrimSpec
      if (keyword === 'MAXLEN') {
        const count = parseUint64(rawValue)
        if (count === null) throw new RedisSyntaxError()
        trim = { strategy: 'maxlen', count, approximate, limit: null }
      } else {
        const minId = parseExactId(rawValue)
        trim = { strategy: 'minid', minId, approximate, limit: null }
      }

      while (cursor < input.length) {
        if (input[cursor]!.toString().toUpperCase() !== 'LIMIT') {
          break
        }

        if (!approximate) {
          throw new StreamLimitRequiresApproxError()
        }

        const rawLimit = input[cursor + 1]?.toString()
        if (rawLimit === undefined) {
          throw new RedisSyntaxError()
        }

        trim.limit = parseTrimLimit(rawLimit)
        cursor += 2
      }

      return { value: trim, nextIndex: cursor }
    },
  )
}

export function applyTrim(stream: RedisStreamData, spec: TrimSpec): number {
  if (spec.strategy === 'maxlen') {
    const targetLength = spec.approximate ? spec.count + 1n : spec.count
    const removeCountBigint = BigInt(stream.entries.length) - targetLength
    const removeCount = removeCountBigint > 0n ? Number(removeCountBigint) : 0
    if (removeCount <= 0) return 0
    for (const entry of stream.entries.slice(0, removeCount)) {
      updateMaxDeletedId(stream, entry.id)
    }
    stream.entries.splice(0, removeCount)
    return removeCount
  } else {
    let i = 0
    while (
      i < stream.entries.length &&
      compareStreamId(stream.entries[i].id, spec.minId) < 0
    ) {
      i++
    }
    if (i === 0) return 0
    if (spec.approximate) {
      i--
      if (i === 0) return 0
    }
    for (const entry of stream.entries.slice(0, i)) {
      updateMaxDeletedId(stream, entry.id)
    }
    stream.entries.splice(0, i)
    return i
  }
}
