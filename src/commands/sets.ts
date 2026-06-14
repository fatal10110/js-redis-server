import { defineCommand } from '../core/command-definition'
import { t } from '../core/command-schema'
import { integer, bulk, array } from './helpers'
import { RedisValue } from '../core/redis-value'
import {
  LimitCantBeNegativeError,
  NumKeysGreaterThanZeroError,
  PositiveCountError,
  RedisSyntaxError,
  WrongNumberOfArgumentsError,
  WrongNumberOfKeysError,
  WrongTypeRedisError,
} from '../core/redis-error'
import type { RedisDatabase } from '../state'

// ---------------------------------------------------------------------------
// Internal helpers
// ---------------------------------------------------------------------------

function getSetMembers(db: RedisDatabase, key: Buffer): Set<string> {
  const setData = db.getSet(key) // throws WrongTypeRedisError if wrong type
  if (!setData) return new Set()
  return new Set(setData.members.keys())
}

function getSetBufferMap(db: RedisDatabase, key: Buffer): Map<string, Buffer> {
  const setData = db.getSet(key)
  if (!setData) return new Map()
  return new Map(setData.members)
}

function computeDiff(sets: Set<string>[]): Set<string> {
  if (sets.length === 0) return new Set()
  const [first, ...rest] = sets
  const result = new Set(first)
  for (const s of rest) {
    for (const m of s) result.delete(m)
  }
  return result
}

function computeInter(sets: Set<string>[]): Set<string> {
  if (sets.length === 0) return new Set()
  const result = new Set(sets[0])
  for (let i = 1; i < sets.length; i++) {
    for (const m of result) {
      if (!sets[i].has(m)) result.delete(m)
    }
  }
  return result
}

function computeUnion(sets: Set<string>[]): Set<string> {
  const result = new Set<string>()
  for (const s of sets) for (const m of s) result.add(m)
  return result
}

function storeSetResult(
  db: RedisDatabase,
  destKey: Buffer,
  hexSet: Set<string>,
  bufferMap: Map<string, Buffer>,
): number {
  if (hexSet.size === 0) {
    db.delete(destKey)
    return 0
  }
  db.updateSet(destKey, set => {
    set.members.clear()
    for (const hex of hexSet) {
      const buf = bufferMap.get(hex)!
      set.members.set(hex, buf)
    }
  })
  return hexSet.size
}

function parseSintercardCount(token: Buffer): number {
  const count = Number(token.toString())
  if (!Number.isSafeInteger(count) || count <= 0) {
    throw new NumKeysGreaterThanZeroError()
  }

  return count
}

function parseSintercardLimit(token: Buffer): number {
  const limit = Number(token.toString())
  if (!Number.isSafeInteger(limit) || limit < 0) {
    throw new LimitCantBeNegativeError()
  }

  return limit
}

const sintercardSchema = t.custom<{
  keys: Buffer[]
  limit: number
}>((input, _index, ctx) => {
  if (input.length < 2) {
    throw new WrongNumberOfArgumentsError(ctx.commandName)
  }

  const keyCount = parseSintercardCount(input[0])
  if (keyCount > input.length - 1) {
    throw new WrongNumberOfKeysError()
  }

  const keys = input.slice(1, 1 + keyCount)
  let cursor = 1 + keyCount
  let limit = 0

  if (cursor >= input.length) {
    return { value: { keys, limit }, nextIndex: cursor }
  }

  const option = input[cursor].toString().toUpperCase()
  if (option !== 'LIMIT') {
    throw new RedisSyntaxError()
  }

  cursor++
  if (cursor >= input.length) {
    throw new RedisSyntaxError()
  }

  limit = parseSintercardLimit(input[cursor])
  cursor++

  if (cursor !== input.length) {
    throw new RedisSyntaxError()
  }

  return { value: { keys, limit }, nextIndex: cursor }
})

// ---------------------------------------------------------------------------
// SADD key member [member ...]
// ---------------------------------------------------------------------------

export const saddCommand = defineCommand({
  name: 'sadd',
  schema: t.object({ key: t.key(), members: t.variadic(t.key(), { min: 1 }) }),
  flags: ['write', 'denyoom', 'fast'],
  keys: args => [args.key],
  execute: (args, ctx) => {
    const added = ctx.db.updateSet(args.key, set => {
      let count = 0
      for (const member of args.members) {
        const hex = member.toString('hex')
        if (!set.members.has(hex)) {
          count++
          set.members.set(hex, member)
        }
      }
      return count
    })
    return integer(added)
  },
})

// ---------------------------------------------------------------------------
// SREM key member [member ...]
// ---------------------------------------------------------------------------

export const sremCommand = defineCommand({
  name: 'srem',
  schema: t.object({ key: t.key(), members: t.variadic(t.key(), { min: 1 }) }),
  flags: ['write', 'fast'],
  keys: args => [args.key],
  execute: (args, ctx) => {
    if (ctx.db.getType(args.key) === null) return integer(0)
    let removed = 0
    ctx.db.updateSet(args.key, set => {
      for (const member of args.members) {
        if (set.members.delete(member.toString('hex'))) removed++
      }
    })
    if (removed > 0 && (ctx.db.getSet(args.key)?.members.size ?? 0) === 0) {
      ctx.db.delete(args.key)
    }
    return integer(removed)
  },
})

// ---------------------------------------------------------------------------
// SCARD key
// ---------------------------------------------------------------------------

export const scardCommand = defineCommand({
  name: 'scard',
  schema: t.object({ key: t.key() }),
  flags: ['readonly', 'fast'],
  keys: args => [args.key],
  execute: (args, ctx) => {
    const set = ctx.db.getSet(args.key)
    return integer(set?.members.size ?? 0)
  },
})

// ---------------------------------------------------------------------------
// SMEMBERS key
// ---------------------------------------------------------------------------

export const smembersCommand = defineCommand({
  name: 'smembers',
  schema: t.object({ key: t.key() }),
  flags: ['readonly'],
  keys: args => [args.key],
  execute: (args, ctx) => {
    const set = ctx.db.getSet(args.key)
    if (!set) return array([])
    return array(
      Array.from(set.members.values()).map(m => RedisValue.bulkString(m)),
    )
  },
})

// ---------------------------------------------------------------------------
// SISMEMBER key member
// ---------------------------------------------------------------------------

export const sismemberCommand = defineCommand({
  name: 'sismember',
  schema: t.object({ key: t.key(), member: t.key() }),
  flags: ['readonly', 'fast'],
  keys: args => [args.key],
  execute: (args, ctx) => {
    const set = ctx.db.getSet(args.key)
    if (!set) return integer(0)
    return integer(set.members.has(args.member.toString('hex')) ? 1 : 0)
  },
})

// ---------------------------------------------------------------------------
// SMISMEMBER key member [member ...]
// ---------------------------------------------------------------------------

export const smismemberCommand = defineCommand({
  name: 'smismember',
  schema: t.object({ key: t.key(), members: t.variadic(t.key(), { min: 1 }) }),
  flags: ['readonly', 'fast'],
  keys: args => [args.key],
  execute: (args, ctx) => {
    const set = ctx.db.getSet(args.key)

    return array(
      args.members.map(member =>
        RedisValue.integer(set?.members.has(member.toString('hex')) ? 1 : 0),
      ),
    )
  },
})

// ---------------------------------------------------------------------------
// SPOP key [count]
// ---------------------------------------------------------------------------

export const spopCommand = defineCommand({
  name: 'spop',
  schema: t.object({ key: t.key(), count: t.optional(t.integer()) }),
  flags: ['write', 'random', 'fast', 'noscript'],
  keys: args => [args.key],
  execute: (args, ctx) => {
    if (args.count !== undefined && args.count < 0) {
      throw new PositiveCountError()
    }

    const type = ctx.db.getType(args.key)
    if (type === null) return args.count === undefined ? bulk(null) : array([])
    if (type !== 'set') throw new WrongTypeRedisError()

    if (args.count === undefined) {
      const result = ctx.db.updateSet(args.key, set => {
        if (set.members.size === 0)
          return { member: null as Buffer | null, empty: false }
        const entries = Array.from(set.members.entries())
        const [hex, member] =
          entries[Math.floor(Math.random() * entries.length)]
        set.members.delete(hex)
        return { member, empty: set.members.size === 0 }
      })
      if (result.empty) ctx.db.delete(args.key)
      return bulk(result.member)
    }

    if (args.count === 0) return array([])

    const result = ctx.db.updateSet(args.key, set => {
      if (set.members.size === 0)
        return { members: [] as Buffer[], empty: true }
      const entries = Array.from(set.members.entries())
      const size = Math.min(args.count!, entries.length)
      const members: Buffer[] = []

      for (let i = 0; i < size; i++) {
        const j = i + Math.floor(Math.random() * (entries.length - i))
        ;[entries[i], entries[j]] = [entries[j], entries[i]]
        const [hex, member] = entries[i]
        set.members.delete(hex)
        members.push(member)
      }

      return { members, empty: set.members.size === 0 }
    })
    if (result.empty) ctx.db.delete(args.key)
    return array(result.members.map(member => RedisValue.bulkString(member)))
  },
})

// ---------------------------------------------------------------------------
// SRANDMEMBER key [count]
// ---------------------------------------------------------------------------

export const srandmemberCommand = defineCommand({
  name: 'srandmember',
  schema: t.object({ key: t.key(), count: t.optional(t.integer()) }),
  flags: ['readonly', 'random', 'noscript'],
  keys: args => [args.key],
  execute: (args, ctx) => {
    const set = ctx.db.getSet(args.key)

    if (args.count === undefined) {
      if (!set || set.members.size === 0) return bulk(null)
      const values = Array.from(set.members.values())
      return bulk(values[Math.floor(Math.random() * values.length)])
    }

    if (!set || set.members.size === 0) return array([])

    const values = Array.from(set.members.values())
    const count = args.count

    if (count >= 0) {
      // unique members, up to count
      const size = Math.min(count, values.length)
      // Fisher-Yates partial shuffle
      const pool = values.slice()
      const result: Buffer[] = []
      for (let i = 0; i < size; i++) {
        const j = i + Math.floor(Math.random() * (pool.length - i))
        ;[pool[i], pool[j]] = [pool[j], pool[i]]
        result.push(pool[i])
      }
      return array(result.map(m => RedisValue.bulkString(m)))
    } else {
      // with repetition, |count| members
      const n = -count
      const result: Buffer[] = []
      for (let i = 0; i < n; i++) {
        result.push(values[Math.floor(Math.random() * values.length)])
      }
      return array(result.map(m => RedisValue.bulkString(m)))
    }
  },
})

// ---------------------------------------------------------------------------
// SDIFF key [key ...]
// ---------------------------------------------------------------------------

export const sdiffCommand = defineCommand({
  name: 'sdiff',
  schema: t.object({ keys: t.variadic(t.key(), { min: 1 }) }),
  flags: ['readonly'],
  keys: args => args.keys,
  execute: (args, ctx) => {
    const sets = args.keys.map(k => getSetMembers(ctx.db, k))
    const diff = computeDiff(sets)
    if (diff.size === 0) return array([])
    // collect buffers from first set (all diff members come from it)
    const firstMap = getSetBufferMap(ctx.db, args.keys[0])
    return array(
      Array.from(diff).map(hex => RedisValue.bulkString(firstMap.get(hex)!)),
    )
  },
})

// ---------------------------------------------------------------------------
// SINTER key [key ...]
// ---------------------------------------------------------------------------

export const sinterCommand = defineCommand({
  name: 'sinter',
  schema: t.object({ keys: t.variadic(t.key(), { min: 1 }) }),
  flags: ['readonly'],
  keys: args => args.keys,
  execute: (args, ctx) => {
    const sets = args.keys.map(k => getSetMembers(ctx.db, k))
    const inter = computeInter(sets)
    if (inter.size === 0) return array([])
    const firstMap = getSetBufferMap(ctx.db, args.keys[0])
    return array(
      Array.from(inter).map(hex => RedisValue.bulkString(firstMap.get(hex)!)),
    )
  },
})

// ---------------------------------------------------------------------------
// SINTERCARD numkeys key [key ...] [LIMIT limit]
// ---------------------------------------------------------------------------

export const sintercardCommand = defineCommand({
  name: 'sintercard',
  schema: sintercardSchema,
  flags: ['readonly'],
  keys: args => args.keys,
  execute: (args, ctx) => {
    const sets = args.keys.map(k => getSetMembers(ctx.db, k))
    const count = computeInter(sets).size
    if (args.limit > 0) return integer(Math.min(count, args.limit))
    return integer(count)
  },
})

// ---------------------------------------------------------------------------
// SUNION key [key ...]
// ---------------------------------------------------------------------------

export const sunionCommand = defineCommand({
  name: 'sunion',
  schema: t.object({ keys: t.variadic(t.key(), { min: 1 }) }),
  flags: ['readonly'],
  keys: args => args.keys,
  execute: (args, ctx) => {
    const bufferMaps = args.keys.map(k => getSetBufferMap(ctx.db, k))
    const union = computeUnion(bufferMaps.map(m => new Set(m.keys())))
    if (union.size === 0) return array([])
    // build combined buffer map
    const combined = new Map<string, Buffer>()
    for (const m of bufferMaps) {
      for (const [hex, buf] of m) combined.set(hex, buf)
    }
    return array(
      Array.from(union).map(hex => RedisValue.bulkString(combined.get(hex)!)),
    )
  },
})

// ---------------------------------------------------------------------------
// SMOVE source destination member
// ---------------------------------------------------------------------------

export const smoveCommand = defineCommand({
  name: 'smove',
  schema: t.object({ source: t.key(), destination: t.key(), member: t.key() }),
  flags: ['write', 'fast'],
  keys: args => [args.source, args.destination],
  execute: (args, ctx) => {
    const hex = args.member.toString('hex')

    const sourceType = ctx.db.getType(args.source)
    if (sourceType === null) return integer(0)
    if (sourceType !== 'set') throw new WrongTypeRedisError()

    // validate destination type before mutating source
    const destType = ctx.db.getType(args.destination)
    if (destType !== null && destType !== 'set') throw new WrongTypeRedisError()

    let moved = false
    ctx.db.updateSet(args.source, set => {
      if (set.members.has(hex)) {
        set.members.delete(hex)
        moved = true
      }
    })

    if (!moved) return integer(0)

    if ((ctx.db.getSet(args.source)?.members.size ?? 0) === 0) {
      ctx.db.delete(args.source)
    }

    ctx.db.updateSet(args.destination, set => {
      set.members.set(hex, args.member)
    })

    return integer(1)
  },
})

// ---------------------------------------------------------------------------
// SDIFFSTORE destination key [key ...]
// ---------------------------------------------------------------------------

export const sdiffstoreCommand = defineCommand({
  name: 'sdiffstore',
  schema: t.object({
    destination: t.key(),
    keys: t.variadic(t.key(), { min: 1 }),
  }),
  flags: ['write', 'denyoom'],
  keys: args => [args.destination, ...args.keys],
  execute: (args, ctx) => {
    const bufferMaps = args.keys.map(k => getSetBufferMap(ctx.db, k))
    const sets = bufferMaps.map(m => new Set(m.keys()))
    const diff = computeDiff(sets)
    const combined = new Map<string, Buffer>()
    for (const m of bufferMaps) {
      for (const [hex, buf] of m) combined.set(hex, buf)
    }
    return integer(storeSetResult(ctx.db, args.destination, diff, combined))
  },
})

// ---------------------------------------------------------------------------
// SINTERSTORE destination key [key ...]
// ---------------------------------------------------------------------------

export const sinterstoreCommand = defineCommand({
  name: 'sinterstore',
  schema: t.object({
    destination: t.key(),
    keys: t.variadic(t.key(), { min: 1 }),
  }),
  flags: ['write', 'denyoom'],
  keys: args => [args.destination, ...args.keys],
  execute: (args, ctx) => {
    const bufferMaps = args.keys.map(k => getSetBufferMap(ctx.db, k))
    const sets = bufferMaps.map(m => new Set(m.keys()))
    const inter = computeInter(sets)
    const combined = new Map<string, Buffer>()
    for (const m of bufferMaps) {
      for (const [hex, buf] of m) combined.set(hex, buf)
    }
    return integer(storeSetResult(ctx.db, args.destination, inter, combined))
  },
})

// ---------------------------------------------------------------------------
// SUNIONSTORE destination key [key ...]
// ---------------------------------------------------------------------------

export const sunionstoreCommand = defineCommand({
  name: 'sunionstore',
  schema: t.object({
    destination: t.key(),
    keys: t.variadic(t.key(), { min: 1 }),
  }),
  flags: ['write', 'denyoom'],
  keys: args => [args.destination, ...args.keys],
  execute: (args, ctx) => {
    const bufferMaps = args.keys.map(k => getSetBufferMap(ctx.db, k))
    const sets = bufferMaps.map(m => new Set(m.keys()))
    const union = computeUnion(sets)
    const combined = new Map<string, Buffer>()
    for (const m of bufferMaps) {
      for (const [hex, buf] of m) combined.set(hex, buf)
    }
    return integer(storeSetResult(ctx.db, args.destination, union, combined))
  },
})

// ---------------------------------------------------------------------------
// Export
// ---------------------------------------------------------------------------

export const setsCommands = [
  saddCommand,
  sremCommand,
  scardCommand,
  smembersCommand,
  sismemberCommand,
  smismemberCommand,
  spopCommand,
  srandmemberCommand,
  sdiffCommand,
  sinterCommand,
  sintercardCommand,
  sunionCommand,
  smoveCommand,
  sdiffstoreCommand,
  sinterstoreCommand,
  sunionstoreCommand,
]
