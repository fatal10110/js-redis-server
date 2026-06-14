import { defineCommand } from '../core/command-definition'
import { t, type ParseContext } from '../core/command-schema'
import {
  HashValueNotFloatError,
  HashValueNotIntegerError,
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
        const hex = field.toString('hex')
        if (!hash.fields.has(hex)) count++
        hash.fields.set(hex, { field, value })
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
      const hex = args.field.toString('hex')
      if (hash.fields.has(hex)) return false
      hash.fields.set(hex, { field: args.field, value: args.value })
      return true
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
        if (hash.fields.delete(field.toString('hex'))) deleted++
      }
      return hash.fields.size
    })
    if (remaining === 0) {
      ctx.db.delete(args.key)
    }
    return integer(deleted)
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
        hash.fields.set(field.toString('hex'), { field, value })
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
      const hex = args.field.toString('hex')
      const entry = hash.fields.get(hex)
      let current = 0
      if (entry) {
        const raw = entry.value.toString()
        if (!/^-?\d+$/.test(raw) || !Number.isSafeInteger(Number(raw))) {
          throw new HashValueNotIntegerError()
        }
        current = Number(raw)
      }
      const next = current + args.increment
      const valueBuf = Buffer.from(String(next))
      hash.fields.set(hex, { field: args.field, value: valueBuf })
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
      const hex = args.field.toString('hex')
      const entry = hash.fields.get(hex)
      let current = 0
      if (entry) {
        const raw = entry.value.toString()
        const parsed = parseFloat(raw)
        if (isNaN(parsed)) {
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
      hash.fields.set(hex, { field: args.field, value: valueBuf })
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
