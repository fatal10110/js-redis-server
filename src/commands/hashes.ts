import { defineCommand } from '../core/command-definition'
import {
  isIntegerToken,
  parseFiniteFloatToken,
  t,
  type ParseContext,
} from '../core/command-schema'
import {
  HashValueNotFloatError,
  HashValueNotIntegerError,
  RedisCommandError,
  RedisSyntaxError,
  WrongNumberOfArgumentsError,
} from '../core/redis-error'
import { RedisResult } from '../core/redis-result'
import { RedisValue } from '../core/redis-value'
import { bulk, integer, ok, array, parseIntegerToken } from './helpers'

type FieldValuePair = { field: Buffer; value: Buffer }
type HrandfieldArgs = {
  key: Buffer
  count: number | undefined
  withValues: boolean
}
type HgetdelArgs = {
  key: Buffer
  fields: Buffer[]
}

const LONG_MAX = 9223372036854775807n

function createFieldValuePairsSchema() {
  return t.custom<FieldValuePair[]>(
    (input: readonly Buffer[], index: number, ctx: ParseContext) => {
      const pairs: FieldValuePair[] = []
      let cursor = index
      if (cursor >= input.length) {
        throw new WrongNumberOfArgumentsError(ctx.commandName)
      }
      while (cursor < input.length) {
        const field = input[cursor]
        const value = input[cursor + 1]
        if (!field || !value) {
          throw new WrongNumberOfArgumentsError(ctx.commandName)
        }
        pairs.push({ field, value })
        cursor += 2
      }
      if (pairs.length === 0) {
        throw new WrongNumberOfArgumentsError(ctx.commandName)
      }
      return { value: pairs, nextIndex: cursor }
    },
  )
}

function createHrandfieldSchema() {
  return t.custom<HrandfieldArgs>(
    (input: readonly Buffer[], index: number, ctx: ParseContext) => {
      const key = input[index]
      if (!key) {
        throw new WrongNumberOfArgumentsError(ctx.commandName)
      }

      let cursor = index + 1
      if (cursor >= input.length) {
        return {
          value: { key, count: undefined, withValues: false },
          nextIndex: cursor,
        }
      }

      const count = parseIntegerToken(input[cursor])
      cursor++

      let withValues = false
      if (cursor < input.length) {
        if (input[cursor].toString().toUpperCase() !== 'WITHVALUES') {
          throw new RedisSyntaxError()
        }

        withValues = true
        cursor++
      }

      if (cursor !== input.length) {
        throw new RedisSyntaxError()
      }

      return { value: { key, count, withValues }, nextIndex: cursor }
    },
  )
}

function createHgetdelSchema() {
  return t.custom<HgetdelArgs>(
    (input: readonly Buffer[], index: number, ctx: ParseContext) => {
      const key = input[index]
      if (!key || input.length - index < 4) {
        throw new WrongNumberOfArgumentsError(ctx.commandName)
      }

      const fieldsToken = input[index + 1]
      if (fieldsToken.toString().toUpperCase() !== 'FIELDS') {
        throw new RedisCommandError(
          'Mandatory argument FIELDS is missing or not at the right position',
        )
      }

      const fieldCount = parsePositiveFieldCount(input[index + 2])
      const fields = input.slice(index + 3)
      if (fieldCount !== BigInt(fields.length)) {
        throw new RedisCommandError(
          'The `numfields` parameter must match the number of arguments',
        )
      }

      return {
        value: { key, fields },
        nextIndex: input.length,
      }
    },
  )
}

function parsePositiveFieldCount(token: Buffer): bigint {
  const raw = token.toString()
  if (!isIntegerToken(raw)) {
    throw new RedisCommandError('Number of fields must be a positive integer')
  }

  const value = BigInt(raw)
  if (value < 1n || value > LONG_MAX) {
    throw new RedisCommandError('Number of fields must be a positive integer')
  }

  return value
}

function randomHashEntries<TValue>(entries: TValue[], count: number): TValue[] {
  if (count === 0) return []

  if (count > 0) {
    const limit = Math.min(count, entries.length)
    const pool = entries.slice()
    const result: TValue[] = []

    for (let i = 0; i < limit; i++) {
      const j = i + Math.floor(Math.random() * (pool.length - i))
      ;[pool[i], pool[j]] = [pool[j], pool[i]]
      result.push(pool[i])
    }

    return result
  }

  const result: TValue[] = []
  for (let i = 0; i < Math.abs(count); i++) {
    result.push(entries[Math.floor(Math.random() * entries.length)])
  }

  return result
}

function hashEntriesReply(
  entries: FieldValuePair[],
  withValues: boolean,
): RedisValue[] {
  if (!withValues) {
    return entries.map(({ field }) => RedisValue.bulkString(field))
  }

  return entries.flatMap(({ field, value }) => [
    RedisValue.bulkString(field),
    RedisValue.bulkString(value),
  ])
}

export const hsetCommand = defineCommand({
  name: 'hset',
  schema: t.object({ key: t.key(), pairs: createFieldValuePairsSchema() }),
  flags: ['write', 'denyoom', 'fast'],
  keys: args => [args.key],
  execute: (args, ctx) => {
    const added = ctx.db.updateHash(args.key, hash => {
      let count = 0
      for (const { field, value } of args.pairs) {
        if (hash.setField(field, value, { forceDirty: true }).added) count++
      }
      return count
    })
    return integer(added)
  },
})

export const hsetnxCommand = defineCommand({
  name: 'hsetnx',
  schema: t.object({ key: t.key(), field: t.key(), value: t.key() }),
  flags: ['write', 'denyoom', 'fast'],
  keys: args => [args.key],
  execute: (args, ctx) => {
    const set = ctx.db.updateHash(args.key, hash => {
      return hash.setFieldIfAbsent(args.field, args.value)
    })
    return integer(set ? 1 : 0)
  },
})

export const hgetCommand = defineCommand({
  name: 'hget',
  schema: t.object({ key: t.key(), field: t.key() }),
  flags: ['readonly', 'fast'],
  keys: args => [args.key],
  execute: (args, ctx) => {
    const hash = ctx.db.getHash(args.key)
    if (!hash) return bulk(null)
    const entry = hash.fields.get(args.field.toString('hex'))
    return bulk(entry?.value ?? null)
  },
})

export const hdelCommand = defineCommand({
  name: 'hdel',
  schema: t.object({ key: t.key(), fields: t.variadic(t.key(), { min: 1 }) }),
  flags: ['write', 'fast'],
  keys: args => [args.key],
  execute: (args, ctx) => {
    const existingHash = ctx.db.getHash(args.key)
    if (!existingHash) return integer(0)

    let deleted = 0
    const remaining = ctx.db.updateHash(args.key, hash => {
      for (const field of args.fields) {
        if (hash.deleteField(field)) deleted++
      }
      return hash.size
    })
    if (remaining === 0) {
      ctx.db.delete(args.key)
    }
    return integer(deleted)
  },
})

export const hgetdelCommand = defineCommand({
  name: 'hgetdel',
  schema: createHgetdelSchema(),
  flags: ['write', 'fast'],
  keys: args => [args.key],
  execute: (args, ctx) => {
    if (!ctx.db.getHash(args.key)) {
      return array(args.fields.map(() => RedisValue.bulkString(null)))
    }

    let remaining = 0
    const values = ctx.db.updateHash(args.key, hash => {
      const replies: RedisValue[] = []
      for (const field of args.fields) {
        const entry = hash.getField(field)
        replies.push(RedisValue.bulkString(entry?.value ?? null))
        if (entry) hash.deleteField(field)
      }
      remaining = hash.size
      return replies
    })

    if (remaining === 0) {
      ctx.db.delete(args.key)
    }
    return array(values)
  },
})

export const hmsetCommand = defineCommand({
  name: 'hmset',
  schema: t.object({ key: t.key(), pairs: createFieldValuePairsSchema() }),
  flags: ['write', 'denyoom', 'fast'],
  keys: args => [args.key],
  execute: (args, ctx) => {
    ctx.db.updateHash(args.key, hash => {
      for (const { field, value } of args.pairs) {
        hash.setField(field, value, { forceDirty: true })
      }
    })
    return ok()
  },
})

export const hmgetCommand = defineCommand({
  name: 'hmget',
  schema: t.object({ key: t.key(), fields: t.variadic(t.key(), { min: 1 }) }),
  flags: ['readonly', 'fast'],
  keys: args => [args.key],
  execute: (args, ctx) => {
    const hash = ctx.db.getHash(args.key)
    return array(
      args.fields.map(field => {
        const entry = hash?.fields.get(field.toString('hex'))
        return RedisValue.bulkString(entry?.value ?? null)
      }),
    )
  },
})

export const hgetallCommand = defineCommand({
  name: 'hgetall',
  schema: t.object({ key: t.key() }),
  flags: ['readonly', 'fast'],
  keys: args => [args.key],
  execute: (args, ctx) => {
    const hash = ctx.db.getHash(args.key)
    const entries: [RedisValue, RedisValue][] = []
    for (const { field, value } of hash?.fields.values() ?? []) {
      entries.push([RedisValue.bulkString(field), RedisValue.bulkString(value)])
    }
    return RedisResult.create(RedisValue.map(entries))
  },
})

export const hkeysCommand = defineCommand({
  name: 'hkeys',
  schema: t.object({ key: t.key() }),
  flags: ['readonly', 'fast'],
  keys: args => [args.key],
  execute: (args, ctx) => {
    const hash = ctx.db.getHash(args.key)
    if (!hash) return array([])
    return array(
      Array.from(hash.fields.values()).map(({ field }) =>
        RedisValue.bulkString(field),
      ),
    )
  },
})

export const hvalsCommand = defineCommand({
  name: 'hvals',
  schema: t.object({ key: t.key() }),
  flags: ['readonly', 'fast'],
  keys: args => [args.key],
  execute: (args, ctx) => {
    const hash = ctx.db.getHash(args.key)
    if (!hash) return array([])
    return array(
      Array.from(hash.fields.values()).map(({ value }) =>
        RedisValue.bulkString(value),
      ),
    )
  },
})

export const hrandfieldCommand = defineCommand({
  name: 'hrandfield',
  schema: createHrandfieldSchema(),
  flags: ['readonly', 'random', 'noscript'],
  keys: args => [args.key],
  execute: (args, ctx) => {
    const hash = ctx.db.getHash(args.key)
    if (!hash || hash.fields.size === 0) {
      return args.count === undefined ? bulk(null) : array([])
    }

    const entries = Array.from(hash.fields.values())
    if (args.count === undefined) {
      const entry = entries[Math.floor(Math.random() * entries.length)]
      return bulk(entry.field)
    }

    return array(
      hashEntriesReply(randomHashEntries(entries, args.count), args.withValues),
    )
  },
})

export const hlenCommand = defineCommand({
  name: 'hlen',
  schema: t.object({ key: t.key() }),
  flags: ['readonly', 'fast'],
  keys: args => [args.key],
  execute: (args, ctx) => {
    const hash = ctx.db.getHash(args.key)
    return integer(hash?.fields.size ?? 0)
  },
})

export const hexistsCommand = defineCommand({
  name: 'hexists',
  schema: t.object({ key: t.key(), field: t.key() }),
  flags: ['readonly', 'fast'],
  keys: args => [args.key],
  execute: (args, ctx) => {
    const hash = ctx.db.getHash(args.key)
    if (!hash) return integer(0)
    return integer(hash.fields.has(args.field.toString('hex')) ? 1 : 0)
  },
})

export const hincrbyCommand = defineCommand({
  name: 'hincrby',
  schema: t.object({ key: t.key(), field: t.key(), increment: t.integer() }),
  flags: ['write', 'fast'],
  keys: args => [args.key],
  execute: (args, ctx) => {
    const result = ctx.db.updateHash(args.key, hash => {
      const entry = hash.getField(args.field)
      let current = 0
      if (entry) {
        const raw = entry.value.toString()
        if (!isIntegerToken(raw) || !Number.isSafeInteger(Number(raw))) {
          throw new HashValueNotIntegerError()
        }
        current = Number(raw)
      }
      const next = current + args.increment
      const valueBuf = Buffer.from(String(next))
      hash.setField(args.field, valueBuf, { forceDirty: true })
      return next
    })
    return integer(result)
  },
})

export const hincrbyfloatCommand = defineCommand({
  name: 'hincrbyfloat',
  schema: t.object({ key: t.key(), field: t.key(), increment: t.float() }),
  flags: ['write', 'fast'],
  keys: args => [args.key],
  execute: (args, ctx) => {
    const result = ctx.db.updateHash(args.key, hash => {
      const entry = hash.getField(args.field)
      let current = 0
      if (entry) {
        const raw = entry.value.toString()
        const parsed = parseFiniteFloatToken(raw)
        if (parsed === undefined) {
          throw new HashValueNotFloatError()
        }
        current = parsed
      }
      const next = current + args.increment
      if (isNaN(next) || !isFinite(next)) {
        throw new HashValueNotFloatError()
      }
      // Format like Redis: strip trailing zeros, use fixed notation for small values
      let formatted = String(next)
      if (formatted.includes('e') || formatted.includes('E')) {
        formatted = next.toFixed(17).replace(/\.?0+$/, '')
      }
      const valueBuf = Buffer.from(formatted)
      hash.setField(args.field, valueBuf, { forceDirty: true })
      return valueBuf
    })
    return bulk(result)
  },
})

export const hstrlenCommand = defineCommand({
  name: 'hstrlen',
  schema: t.object({ key: t.key(), field: t.key() }),
  flags: ['readonly', 'fast'],
  keys: args => [args.key],
  execute: (args, ctx) => {
    const hash = ctx.db.getHash(args.key)
    if (!hash) return integer(0)
    const entry = hash.fields.get(args.field.toString('hex'))
    return integer(entry?.value.byteLength ?? 0)
  },
})

export const hashesCommands = [
  hsetCommand,
  hsetnxCommand,
  hgetCommand,
  hdelCommand,
  hgetdelCommand,
  hmsetCommand,
  hmgetCommand,
  hgetallCommand,
  hkeysCommand,
  hvalsCommand,
  hrandfieldCommand,
  hlenCommand,
  hexistsCommand,
  hincrbyCommand,
  hincrbyfloatCommand,
  hstrlenCommand,
]
