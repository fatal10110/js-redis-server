import { defineCommand } from '../core/command-definition'
import { t, type ParseContext } from '../core/command-schema'
import {
  DbIndexOutOfRangeError,
  ExpireGtLtConflictError,
  ExpireNxXxGtLtConflictError,
  ExpectedIntegerError,
  NoSuchKeyError,
  RedisSyntaxError,
  SameObjectError,
  SortScoreNotDoubleError,
  UnsupportedOptionError,
  WrongNumberOfArgumentsError,
  WrongTypeRedisError,
} from '../core/redis-error'
import { RedisValue } from '../core/redis-value'
import type { ExpirationState, RedisDatabase } from '../state'
import {
  array,
  bulk,
  integer,
  ok,
  parseIntegerToken,
  simpleString,
  ttlMilliseconds,
  ttlSeconds,
  typeName,
} from './helpers'

export const delCommand = defineCommand({
  name: 'del',
  schema: t.object({
    keys: t.variadic(t.key(), { min: 1 }),
  }),
  flags: ['write'],
  keys: args => args.keys,
  execute: (args, ctx) => {
    let count = 0
    for (const key of args.keys) {
      if (ctx.db.delete(key)) {
        count += 1
      }
    }

    return integer(count)
  },
})

export const unlinkCommand = defineCommand({
  ...delCommand,
  name: 'unlink',
})

export const existsCommand = defineCommand({
  name: 'exists',
  schema: t.object({
    keys: t.variadic(t.key(), { min: 1 }),
  }),
  flags: ['readonly', 'fast'],
  keys: args => args.keys,
  execute: (args, ctx) => {
    let count = 0
    for (const key of args.keys) {
      if (ctx.db.getType(key) !== null) {
        count += 1
      }
    }

    return integer(count)
  },
})

export const touchCommand = defineCommand({
  name: 'touch',
  schema: t.object({
    keys: t.variadic(t.key(), { min: 1 }),
  }),
  flags: ['readonly', 'fast'],
  keys: args => args.keys,
  execute: (args, ctx) => {
    let count = 0
    for (const key of args.keys) {
      if (ctx.db.getType(key) !== null) {
        count += 1
      }
    }

    return integer(count)
  },
})

export const typeCommand = defineCommand({
  name: 'type',
  schema: t.object({
    key: t.key(),
  }),
  flags: ['readonly', 'fast'],
  keys: args => [args.key],
  execute: (args, ctx) => simpleString(typeName(ctx.db.getType(args.key))),
})

export const dbsizeCommand = defineCommand({
  name: 'dbsize',
  schema: t.object({}),
  flags: ['readonly', 'fast'],
  keys: () => [],
  execute: (_args, ctx) => integer(ctx.db.size()),
})

export const randomkeyCommand = defineCommand({
  name: 'randomkey',
  schema: t.object({}),
  flags: ['readonly', 'random', 'fast'],
  keys: () => [],
  execute: (_args, ctx) => {
    const entries = ctx.db.entriesSnapshot()
    if (entries.length === 0) {
      return bulk(null)
    }

    const index = Math.floor(Math.random() * entries.length)
    return bulk(entries[index].key)
  },
})

export const ttlCommand = defineCommand({
  name: 'ttl',
  schema: t.object({
    key: t.key(),
  }),
  flags: ['readonly', 'fast'],
  keys: args => [args.key],
  execute: (args, ctx) => {
    const expiration = ctx.db.getExpiration(args.key)
    if (expiration.kind === 'missing') {
      return integer(-2)
    }

    if (expiration.kind === 'persistent') {
      return integer(-1)
    }

    return integer(ttlSeconds(expiration.expiresAt))
  },
})

export const pttlCommand = defineCommand({
  name: 'pttl',
  schema: t.object({
    key: t.key(),
  }),
  flags: ['readonly', 'fast'],
  keys: args => [args.key],
  execute: (args, ctx) => {
    const expiration = ctx.db.getExpiration(args.key)
    if (expiration.kind === 'missing') {
      return integer(-2)
    }

    if (expiration.kind === 'persistent') {
      return integer(-1)
    }

    return integer(ttlMilliseconds(expiration.expiresAt))
  },
})

export const expiretimeCommand = defineCommand({
  name: 'expiretime',
  schema: t.object({
    key: t.key(),
  }),
  flags: ['readonly', 'fast'],
  keys: args => [args.key],
  execute: (args, ctx) => {
    const expiration = ctx.db.getExpiration(args.key)
    if (expiration.kind === 'missing') {
      return integer(-2)
    }

    if (expiration.kind === 'persistent') {
      return integer(-1)
    }

    // Redis rounds to the nearest second ((ms+500)/1000), not floor.
    return integer(Math.round(expiration.expiresAt / 1000))
  },
})

export const pexpiretimeCommand = defineCommand({
  name: 'pexpiretime',
  schema: t.object({
    key: t.key(),
  }),
  flags: ['readonly', 'fast'],
  keys: args => [args.key],
  execute: (args, ctx) => {
    const expiration = ctx.db.getExpiration(args.key)
    if (expiration.kind === 'missing') {
      return integer(-2)
    }

    if (expiration.kind === 'persistent') {
      return integer(-1)
    }

    return integer(expiration.expiresAt)
  },
})

type ExpireCondition = 'NX' | 'XX'
type ExpireComparison = 'GT' | 'LT'
type ExpireOptions = {
  condition?: ExpireCondition
  comparison?: ExpireComparison
}

const expireOptionsSchema = t.custom<ExpireOptions>((input, index) => {
  const options: ExpireOptions = {}
  let cursor = index

  while (cursor < input.length) {
    const token = input[cursor]!.toString()
    const option = token.toUpperCase()

    if (option === 'NX') {
      if (options.condition === 'XX' || options.comparison !== undefined) {
        throw new ExpireNxXxGtLtConflictError()
      }
      options.condition = 'NX'
      cursor += 1
      continue
    }

    if (option === 'XX') {
      if (options.condition === 'NX') {
        throw new ExpireNxXxGtLtConflictError()
      }
      options.condition = 'XX'
      cursor += 1
      continue
    }

    if (option === 'GT') {
      if (options.condition === 'NX') {
        throw new ExpireNxXxGtLtConflictError()
      }
      if (options.comparison === 'LT') {
        throw new ExpireGtLtConflictError()
      }
      options.comparison = 'GT'
      cursor += 1
      continue
    }

    if (option === 'LT') {
      if (options.condition === 'NX') {
        throw new ExpireNxXxGtLtConflictError()
      }
      if (options.comparison === 'GT') {
        throw new ExpireGtLtConflictError()
      }
      options.comparison = 'LT'
      cursor += 1
      continue
    }

    throw new UnsupportedOptionError(token)
  }

  return { value: options, nextIndex: cursor }
})

export const expireCommand = defineCommand({
  name: 'expire',
  schema: t.object({
    key: t.key(),
    seconds: t.integer(),
    options: expireOptionsSchema,
  }),
  flags: ['write', 'fast'],
  keys: args => [args.key],
  execute: (args, ctx) =>
    expireKey(ctx.db, args.key, args.seconds, 1000, args.options),
})

export const pexpireCommand = defineCommand({
  name: 'pexpire',
  schema: t.object({
    key: t.key(),
    milliseconds: t.integer(),
    options: expireOptionsSchema,
  }),
  flags: ['write', 'fast'],
  keys: args => [args.key],
  execute: (args, ctx) =>
    expireKey(ctx.db, args.key, args.milliseconds, 1, args.options),
})

export const persistCommand = defineCommand({
  name: 'persist',
  schema: t.object({
    key: t.key(),
  }),
  flags: ['write', 'fast'],
  keys: args => [args.key],
  execute: (args, ctx) => integer(ctx.db.persist(args.key) ? 1 : 0),
})

// FLUSHDB/FLUSHALL accept an optional ASYNC|SYNC modifier (Redis 4.0+). The
// keyspace is in-memory, so the flush is synchronous regardless — the keyword is
// parsed for compatibility and otherwise ignored. Anything other than a single
// ASYNC|SYNC token is a syntax error (matching real Redis), so the whole tail is
// validated here rather than relying on the generic leftover-arg check, which
// would surface a wrong-number-of-arguments error instead.
const flushModeSchema = t.custom<'async' | 'sync' | undefined>(
  (input, index) => {
    const remaining = input.length - index
    if (remaining === 0) {
      return { value: undefined, nextIndex: index }
    }

    if (remaining === 1) {
      const token = input[index].toString().toLowerCase()
      if (token === 'async' || token === 'sync') {
        return { value: token, nextIndex: index + 1 }
      }
    }

    throw new RedisSyntaxError()
  },
)

export const flushdbCommand = defineCommand({
  name: 'flushdb',
  schema: t.object({ mode: flushModeSchema }),
  flags: ['write'],
  keys: () => [],
  execute: (_args, ctx) => {
    ctx.db.flush()
    return ok()
  },
})

export const flushallCommand = defineCommand({
  name: 'flushall',
  schema: t.object({ mode: flushModeSchema }),
  flags: ['write'],
  keys: () => [],
  execute: (_args, ctx) => {
    ctx.server.flushAllDatabases()
    return ok()
  },
})

export const expireatCommand = defineCommand({
  name: 'expireat',
  schema: t.object({
    key: t.key(),
    timestamp: t.integer(),
    options: expireOptionsSchema,
  }),
  flags: ['write', 'fast'],
  keys: args => [args.key],
  execute: (args, ctx) => {
    return expireAtKey(ctx.db, args.key, args.timestamp * 1000, args.options)
  },
})

export const pexpireatCommand = defineCommand({
  name: 'pexpireat',
  schema: t.object({
    key: t.key(),
    timestamp: t.integer(),
    options: expireOptionsSchema,
  }),
  flags: ['write', 'fast'],
  keys: args => [args.key],
  execute: (args, ctx) => {
    return expireAtKey(ctx.db, args.key, args.timestamp, args.options)
  },
})

export const renameCommand = defineCommand({
  name: 'rename',
  schema: t.object({ key: t.key(), newKey: t.key() }),
  flags: ['write'],
  keys: args => [args.key, args.newKey],
  execute: (args, ctx) => {
    const value = ctx.db.get(args.key)
    if (!value) throw new NoSuchKeyError()

    // Renaming a key to itself is a true no-op in Redis: it replies +OK
    // without touching the keyspace, so it must not emit a mutation event
    // that would invalidate a WATCH on the key.
    if (args.key.equals(args.newKey)) return ok()

    const expiration = ctx.db.getExpiration(args.key)
    const expiresAt =
      expiration.kind === 'expires' ? expiration.expiresAt : undefined

    ctx.db.delete(args.key)
    ctx.db.set(
      args.newKey,
      value,
      expiresAt !== undefined ? { expiresAt } : undefined,
    )

    return ok()
  },
})

export const renamenxCommand = defineCommand({
  name: 'renamenx',
  schema: t.object({ key: t.key(), newKey: t.key() }),
  flags: ['write', 'fast'],
  keys: args => [args.key, args.newKey],
  execute: (args, ctx) => {
    const value = ctx.db.get(args.key)
    if (!value) throw new NoSuchKeyError()

    if (ctx.db.getType(args.newKey) !== null) return integer(0)

    const expiration = ctx.db.getExpiration(args.key)
    const expiresAt =
      expiration.kind === 'expires' ? expiration.expiresAt : undefined

    ctx.db.delete(args.key)
    ctx.db.set(
      args.newKey,
      value,
      expiresAt !== undefined ? { expiresAt } : undefined,
    )

    return integer(1)
  },
})

export const moveCommand = defineCommand({
  name: 'move',
  schema: t.object({ key: t.key(), database: t.integer() }),
  flags: ['write'],
  keys: args => [args.key],
  execute: (args, ctx) => {
    const targetDb = ctx.server.databases[args.database]
    if (!targetDb) {
      throw new DbIndexOutOfRangeError()
    }

    if (targetDb.id === ctx.db.id) {
      throw new SameObjectError()
    }

    const value = ctx.db.get(args.key)
    if (!value) return integer(0)

    if (targetDb.getType(args.key) !== null) {
      return integer(0)
    }

    const expiration = ctx.db.getExpiration(args.key)
    const expiresAt =
      expiration.kind === 'expires' ? expiration.expiresAt : undefined

    ctx.db.delete(args.key)
    targetDb.set(
      args.key,
      value,
      expiresAt !== undefined ? { expiresAt } : undefined,
    )

    return integer(1)
  },
})

type CopyOptions = { db?: number; replace: boolean }

/**
 * Parses the trailing `[DB destination-db] [REPLACE]` options of COPY. Like
 * real Redis the options may appear in any order and repeat (last DB wins).
 */
const copyOptionsSchema = t.custom<CopyOptions>((input, index) => {
  let cursor = index
  let db: number | undefined
  let replace = false

  while (cursor < input.length) {
    const token = input[cursor].toString().toUpperCase()

    if (token === 'REPLACE') {
      replace = true
      cursor += 1
      continue
    }

    if (token === 'DB') {
      const raw = input[cursor + 1]
      if (!raw) throw new RedisSyntaxError()

      const text = raw.toString()
      if (!/^-?\d+$/.test(text)) throw new ExpectedIntegerError()

      const value = Number(text)
      if (!Number.isSafeInteger(value)) throw new ExpectedIntegerError()

      db = value
      cursor += 2
      continue
    }

    throw new RedisSyntaxError()
  }

  return { value: { db, replace }, nextIndex: cursor }
})

export const copyCommand = defineCommand({
  name: 'copy',
  schema: t.object({
    source: t.key(),
    destination: t.key(),
    options: copyOptionsSchema,
  }),
  flags: ['write'],
  keys: args => [args.source, args.destination],
  execute: (args, ctx) => {
    const { source, destination, options } = args

    let targetDb = ctx.db
    if (options.db !== undefined) {
      if (options.db < 0 || options.db >= ctx.server.databases.length) {
        throw new DbIndexOutOfRangeError()
      }
      targetDb = ctx.server.getDatabase(options.db)
    }

    if (targetDb.id === ctx.db.id && source.equals(destination)) {
      throw new SameObjectError()
    }

    const value = ctx.db.get(source)
    if (!value) return integer(0)

    if (!options.replace && targetDb.getType(destination) !== null) {
      return integer(0)
    }

    const expiration = ctx.db.getExpiration(source)
    const expiresAt =
      expiration.kind === 'expires' ? expiration.expiresAt : undefined

    targetDb.set(
      destination,
      value,
      expiresAt !== undefined ? { expiresAt } : undefined,
    )

    return integer(1)
  },
})

// SORT key [LIMIT offset count] [ASC | DESC] [ALPHA] [STORE destination]
// Sorts the elements of a list, set, or zset (zset is sorted by member value,
// not score). Numeric by default — every element must parse as a double, or
// the command errors; ALPHA switches to a byte-wise lexicographic sort. STORE
// writes the sorted result to a destination list and replies with its length.
// BY/GET external-key pattern dereference is intentionally not implemented yet
// (tracked as a follow-up); passing either yields a syntax error.
type SortArgs = {
  key: Buffer
  desc: boolean
  alpha: boolean
  limit?: { offset: number; count: number }
  store?: Buffer
}

function parseSort(
  input: readonly Buffer[],
  index: number,
  ctx: ParseContext,
  allowStore: boolean,
): SortArgs {
  const key = input[index]
  if (!key) throw new WrongNumberOfArgumentsError(ctx.commandName)

  let cursor = index + 1
  let desc = false
  let alpha = false
  let limit: { offset: number; count: number } | undefined
  let store: Buffer | undefined

  while (cursor < input.length) {
    const option = input[cursor]!.toString().toUpperCase()

    if (option === 'ASC') {
      desc = false
      cursor++
      continue
    }

    if (option === 'DESC') {
      desc = true
      cursor++
      continue
    }

    if (option === 'ALPHA') {
      alpha = true
      cursor++
      continue
    }

    if (option === 'LIMIT') {
      const offsetToken = input[cursor + 1]
      const countToken = input[cursor + 2]
      if (!offsetToken || !countToken) throw new RedisSyntaxError()
      limit = {
        offset: parseIntegerToken(offsetToken),
        count: parseIntegerToken(countToken),
      }
      cursor += 3
      continue
    }

    if (allowStore && option === 'STORE') {
      const destination = input[cursor + 1]
      if (!destination) throw new RedisSyntaxError()
      store = destination
      cursor += 2
      continue
    }

    throw new RedisSyntaxError()
  }

  return { key, desc, alpha, limit, store }
}

function sortSchema(allowStore: boolean) {
  return t.custom<SortArgs>((input, index, ctx) => ({
    value: parseSort(input, index, ctx, allowStore),
    nextIndex: input.length,
  }))
}

function readSortSource(db: RedisDatabase, key: Buffer): Buffer[] {
  const type = db.getType(key)
  if (type === null) return []
  if (type === 'list') return db.getList(key)!.values
  if (type === 'set') return Array.from(db.getSet(key)!.members.values())
  if (type === 'zset') {
    return Array.from(db.getSortedSet(key)!.members.values(), m => m.member)
  }
  throw new WrongTypeRedisError()
}

function sortNumericScore(element: Buffer): number {
  // Redis converts every element with strtod; an empty string becomes 0, and
  // anything that does not parse as a double aborts the whole command.
  const value = Number(element.toString())
  if (Number.isNaN(value)) throw new SortScoreNotDoubleError()
  return value
}

function sortElements(elements: readonly Buffer[], args: SortArgs): Buffer[] {
  if (args.alpha) {
    const sorted = [...elements].sort((a, b) => Buffer.compare(a, b))
    if (args.desc) sorted.reverse()
    return sorted
  }

  const scored = elements.map(element => ({
    element,
    score: sortNumericScore(element),
  }))
  scored.sort((a, b) => a.score - b.score)
  if (args.desc) scored.reverse()
  return scored.map(entry => entry.element)
}

function applySortLimit(
  elements: Buffer[],
  limit: SortArgs['limit'],
): Buffer[] {
  if (!limit) return elements
  // Redis clamps a negative offset to 0; a negative count means "all remaining".
  const start = Math.max(0, limit.offset)
  if (start >= elements.length) return []
  if (limit.count < 0) return elements.slice(start)
  return elements.slice(start, start + limit.count)
}

function runSort(args: SortArgs, db: RedisDatabase) {
  const source = readSortSource(db, args.key)
  const sorted = applySortLimit(sortElements(source, args), args.limit)

  if (args.store) {
    db.delete(args.store)
    if (sorted.length > 0) {
      db.updateList(args.store, list => list.pushRight(sorted))
    }
    return integer(sorted.length)
  }

  return array(sorted.map(element => RedisValue.bulkString(element)))
}

export const sortCommand = defineCommand({
  name: 'sort',
  schema: sortSchema(true),
  flags: ['write', 'denyoom'],
  keys: args => (args.store ? [args.key, args.store] : [args.key]),
  execute: (args, ctx) => runSort(args, ctx.db),
})

export const sortRoCommand = defineCommand({
  name: 'sort_ro',
  schema: sortSchema(false),
  flags: ['readonly'],
  keys: args => [args.key],
  execute: (args, ctx) => runSort(args, ctx.db),
})

export const keysCommands = [
  delCommand,
  unlinkCommand,
  existsCommand,
  touchCommand,
  typeCommand,
  dbsizeCommand,
  randomkeyCommand,
  ttlCommand,
  pttlCommand,
  expiretimeCommand,
  pexpiretimeCommand,
  expireCommand,
  pexpireCommand,
  persistCommand,
  flushdbCommand,
  flushallCommand,
  expireatCommand,
  pexpireatCommand,
  renameCommand,
  renamenxCommand,
  moveCommand,
  copyCommand,
  sortCommand,
  sortRoCommand,
]

function expireKey(
  db: RedisDatabase,
  key: Buffer,
  duration: number,
  multiplier: number,
  options: ExpireOptions,
) {
  const now = Date.now()
  return expireAtKey(db, key, now + duration * multiplier, options, now)
}

function expireAtKey(
  db: RedisDatabase,
  key: Buffer,
  expiresAt: number,
  options: ExpireOptions,
  now = Date.now(),
) {
  const expiration = db.getExpiration(key)
  if (expiration.kind === 'missing') {
    return integer(0)
  }

  if (!shouldApplyExpireOptions(expiration, expiresAt, options)) {
    return integer(0)
  }

  if (expiresAt <= now) {
    return integer(db.delete(key) ? 1 : 0)
  }

  return integer(db.expire(key, expiresAt) ? 1 : 0)
}

function shouldApplyExpireOptions(
  expiration: Exclude<ExpirationState, { kind: 'missing' }>,
  expiresAt: number,
  options: ExpireOptions,
): boolean {
  if (options.condition === 'NX') {
    return expiration.kind === 'persistent'
  }

  if (options.condition === 'XX' && expiration.kind !== 'expires') {
    return false
  }

  if (options.comparison === 'GT') {
    return expiration.kind === 'expires' && expiresAt > expiration.expiresAt
  }

  if (options.comparison !== 'LT') {
    return true
  }

  if (expiration.kind === 'persistent') {
    return true
  }

  return expiresAt < expiration.expiresAt
}
