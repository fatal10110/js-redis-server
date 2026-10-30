import { asciiLowerCase, asciiUpperCase } from '../core/ascii-case'
import {
  defineCommand,
  type CommandIntrospection,
} from '../core/command-definition'
import { t } from '../core/command-schema'
import type { CompatibilityProfile } from '../core/compatibility/profile'
import { parseSortScoreToken } from '../core/double-token'
import {
  WrongNumberOfArgumentsError,
  WrongTypeRedisError,
  errors,
} from '../core/redis-error'
import { RedisValue } from '../core/redis-value'
import type { RedisExecutionContext } from '../core/redis-context'
import {
  isConstantSortPattern,
  isSelfSortPattern,
  sortPatternWildcardIndex,
} from '../core/sort-patterns'
import { sortGetKeys, sortRoGetKeys } from '../core/key-specs'
import { assertSortPatternAllowed } from '../core/sort-cluster-guard'
import type { ExpirationState, RedisDatabase } from '../state'
import {
  array,
  bulk,
  deleteForPastDeadline,
  integer,
  keyTtlSeconds,
  ok,
  parseInt64Token,
  simpleString,
  keyExpireDeadlineMs,
  ttlMilliseconds,
  typeName,
} from './helpers'
import { getSortedMembers } from './zsets/helpers'
import { dbIndexSchema, parseDbIndexToken } from './db-index'

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

    return integer(keyTtlSeconds(expiration.expiresAt))
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
  since: { redis: '7.0.0', valkey: '7.2.0' },
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
  since: { redis: '7.0.0', valkey: '7.2.0' },
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

const expireOptionsSchema = t.custom<ExpireOptions>((input, index, ctx) => {
  const options: ExpireOptions = {}
  let cursor = index

  // Before 7.0 the family takes exactly `key time`: any extra token, option
  // or not, is an arity error.
  if (cursor < input.length && !ctx.profile.has('expire.conditions')) {
    throw new WrongNumberOfArgumentsError(ctx.commandName)
  }

  while (cursor < input.length) {
    const token = input[cursor]!.toString()
    const option = asciiUpperCase(token)

    if (option === 'NX') {
      if (options.condition === 'XX' || options.comparison !== undefined) {
        throw errors.expireNxXxGtLtConflict()
      }
      options.condition = 'NX'
      cursor += 1
      continue
    }

    if (option === 'XX') {
      if (options.condition === 'NX') {
        throw errors.expireNxXxGtLtConflict()
      }
      options.condition = 'XX'
      cursor += 1
      continue
    }

    if (option === 'GT') {
      if (options.condition === 'NX') {
        throw errors.expireNxXxGtLtConflict()
      }
      if (options.comparison === 'LT') {
        throw errors.expireGtLtConflict()
      }
      options.comparison = 'GT'
      cursor += 1
      continue
    }

    if (option === 'LT') {
      if (options.condition === 'NX') {
        throw errors.expireNxXxGtLtConflict()
      }
      if (options.comparison === 'GT') {
        throw errors.expireGtLtConflict()
      }
      options.comparison = 'LT'
      cursor += 1
      continue
    }

    throw errors.unsupportedOption(token)
  }

  return { value: options, nextIndex: cursor }
})

// The NX/XX/GT/LT options arrived in 7.0. Before that the parser above
// refuses anything past `key time`, and Redis reported the family's arity as 3.
const expireIntrospection: CommandIntrospection = {
  arity: profile => (profile.has('expire.conditions') ? -3 : 3),
}

export const expireCommand = defineCommand({
  name: 'expire',
  schema: t.object({
    key: t.key(),
    seconds: t.bulk(),
    options: expireOptionsSchema,
  }),
  introspection: expireIntrospection,
  flags: ['write', 'fast'],
  keys: args => [args.key],
  execute: (args, ctx) =>
    expireKey(ctx, args.key, args.seconds, true, 'expire', args.options),
})

export const pexpireCommand = defineCommand({
  name: 'pexpire',
  schema: t.object({
    key: t.key(),
    milliseconds: t.bulk(),
    options: expireOptionsSchema,
  }),
  introspection: expireIntrospection,
  flags: ['write', 'fast'],
  keys: args => [args.key],
  execute: (args, ctx) =>
    expireKey(ctx, args.key, args.milliseconds, false, 'pexpire', args.options),
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
      const token = asciiLowerCase(input[index].toString())
      if (token === 'async' || token === 'sync') {
        return { value: token, nextIndex: index + 1 }
      }
    }

    throw errors.syntax()
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
    timestamp: t.bulk(),
    options: expireOptionsSchema,
  }),
  introspection: expireIntrospection,
  flags: ['write', 'fast'],
  keys: args => [args.key],
  execute: (args, ctx) => {
    const expiresAt = keyExpireDeadlineMs(
      args.timestamp,
      true,
      0,
      'expireat',
      ctx.server.profile,
    )
    return expireAtKey(ctx, args.key, expiresAt, args.options)
  },
})

export const pexpireatCommand = defineCommand({
  name: 'pexpireat',
  schema: t.object({
    key: t.key(),
    timestamp: t.bulk(),
    options: expireOptionsSchema,
  }),
  introspection: expireIntrospection,
  flags: ['write', 'fast'],
  keys: args => [args.key],
  execute: (args, ctx) => {
    const expiresAt = keyExpireDeadlineMs(
      args.timestamp,
      false,
      0,
      'pexpireat',
      ctx.server.profile,
    )
    return expireAtKey(ctx, args.key, expiresAt, args.options)
  },
})

export const renameCommand = defineCommand({
  name: 'rename',
  schema: t.object({ key: t.key(), newKey: t.key() }),
  flags: ['write'],
  keys: args => [args.key, args.newKey],
  execute: (args, ctx) => {
    const value = ctx.db.get(args.key)
    if (!value) throw errors.noSuchKey()

    // Renaming a key to itself is a true no-op in Redis: it replies +OK
    // without touching the keyspace, so it must not emit a mutation event
    // that would invalidate a WATCH on the key.
    if (args.key.equals(args.newKey)) return ok()

    const expiration = ctx.db.getExpiration(args.key)
    const expiresAt =
      expiration.kind === 'expires' ? expiration.expiresAt : undefined

    // Real Redis adds the destination (`new`) before deleting the source
    // (`rename_from`), then announces `rename_to`; an existing destination
    // is deleted and added again, so it is `new` too.
    ctx.db.announceNew(args.newKey)
    ctx.db.delete(args.key)
    ctx.db.set(args.newKey, value, { expiresAt, announceNew: false })

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
    if (!value) throw errors.noSuchKey()

    if (ctx.db.getType(args.newKey) !== null) return integer(0)

    const expiration = ctx.db.getExpiration(args.key)
    const expiresAt =
      expiration.kind === 'expires' ? expiration.expiresAt : undefined

    // Real Redis adds the destination (`new`) before deleting the source
    // (`rename_from`), then announces `rename_to`; an existing destination
    // is deleted and added again, so it is `new` too.
    ctx.db.announceNew(args.newKey)
    ctx.db.delete(args.key)
    ctx.db.set(args.newKey, value, { expiresAt, announceNew: false })

    return integer(1)
  },
})

export const moveCommand = defineCommand({
  name: 'move',
  schema: t.object({ key: t.key(), database: dbIndexSchema }),
  flags: ['write'],
  capabilities: { clusterMode: 'multiDbOnly' },
  keys: args => [args.key],
  execute: (args, ctx) => {
    const targetDb = ctx.server.databases[args.database]
    if (!targetDb) {
      throw errors.dbIndexOutOfRange()
    }

    if (targetDb.id === ctx.db.id) {
      throw errors.sameObject()
    }

    const value = ctx.db.get(args.key)
    if (!value) return integer(0)

    if (targetDb.getType(args.key) !== null) {
      return integer(0)
    }

    const expiration = ctx.db.getExpiration(args.key)
    const expiresAt =
      expiration.kind === 'expires' ? expiration.expiresAt : undefined

    // `new` on the target, move_from on the source, then move_to on the
    // target (#445).
    const target = targetDb.withOrigin('move')
    target.announceNew(args.key)
    ctx.db.delete(args.key)
    target.set(args.key, value, { expiresAt, announceNew: false })

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
    const token = asciiUpperCase(input[cursor].toString())

    if (token === 'REPLACE') {
      replace = true
      cursor += 1
      continue
    }

    if (token === 'DB') {
      const raw = input[cursor + 1]
      if (!raw) throw errors.syntax()

      db = parseDbIndexToken(raw)
      cursor += 2
      continue
    }

    throw errors.syntax()
  }

  return { value: { db, replace }, nextIndex: cursor }
})

export const copyCommand = defineCommand({
  name: 'copy',
  since: { redis: '6.2.0', valkey: '7.2.0' },
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
        throw errors.dbIndexOutOfRange()
      }
      // The executor tags only the selected database with the command's
      // name; tag the target too so its write is published as copy_to (#445).
      targetDb = ctx.server.getDatabase(options.db).withOrigin('copy')
    }

    if (targetDb.id === ctx.db.id && source.equals(destination)) {
      throw errors.sameObject()
    }

    const value = ctx.db.get(source)
    if (!value) return integer(0)

    if (!options.replace && targetDb.getType(destination) !== null) {
      return integer(0)
    }

    const expiration = ctx.db.getExpiration(source)
    const expiresAt =
      expiration.kind === 'expires' ? expiration.expiresAt : undefined

    // REPLACE deletes an existing destination and adds it again, so the
    // destination is announced `new` either way.
    targetDb.set(destination, value, { expiresAt, announceNew: true })

    return integer(1)
  },
})

// SORT key [BY pattern] [LIMIT offset count] [GET pattern ...] [ASC | DESC]
//      [ALPHA] [STORE destination]
// Sorts the elements of a list, set, or zset (zset is sorted by member value,
// not score). Numeric by default — every element must parse as a double, or
// the command errors; ALPHA switches to a byte-wise lexicographic sort. STORE
// writes the sorted result to a destination list and replies with its length.
//
// Only the source key is parsed when the command is planned. The options are
// scanned when it runs, left to right, exactly as sortCommand() does (#417):
// the scan stops at the first offending token — a syntax error, a bad LIMIT
// integer, or a BY/GET pattern the cluster guard refuses — so whichever comes
// first is the error reported, and inside MULTI every one of them queues and
// surfaces in EXEC instead of aborting the transaction.
type SortArgs = {
  key: Buffer
  options: Buffer[]
}

type SortOptions = {
  desc: boolean
  alpha: boolean
  limit?: { offset: number; count: number }
  /**
   * The last BY given. sortCommand() keeps overwriting it, but a constant BY
   * sets `dontsort` for good, so once any BY was constant the weights are
   * never looked up and `by` is irrelevant (#443). While `dontsort` is false,
   * every BY seen was a glob.
   */
  by?: Buffer
  dontsort: boolean
  get: Buffer[]
  store?: Buffer
}

function sortSchema() {
  return t.custom<SortArgs>({ min: 1, keys: [0] }, (input, index, ctx) => {
    const key = input[index]
    if (!key) throw new WrongNumberOfArgumentsError(ctx.commandName)
    return {
      value: { key, options: input.slice(index + 1) },
      nextIndex: input.length,
    }
  })
}

function scanSortOptions(
  args: SortArgs,
  allowStore: boolean,
  ctx: RedisExecutionContext,
): SortOptions {
  const { key, options } = args
  const scanned: SortOptions = {
    desc: false,
    alpha: false,
    dontsort: false,
    get: [],
  }

  for (let j = 0; j < options.length; j++) {
    const option = asciiUpperCase(options[j]!.toString())
    const remaining = options.length - j - 1

    if (option === 'ASC') {
      scanned.desc = false
      continue
    }

    if (option === 'DESC') {
      scanned.desc = true
      continue
    }

    if (option === 'ALPHA') {
      scanned.alpha = true
      continue
    }

    if (option === 'LIMIT' && remaining >= 2) {
      scanned.limit = parseSortLimit(options[j + 1]!, options[j + 2]!)
      j += 2
      continue
    }

    if (allowStore && option === 'STORE' && remaining >= 1) {
      scanned.store = options[++j]
      continue
    }

    if (option === 'BY' && remaining >= 1) {
      const pattern = options[++j]!
      scanned.by = pattern
      if (isConstantSortPattern(pattern)) {
        scanned.dontsort = true
      } else {
        assertSortPatternAllowed(ctx, 'BY', pattern, key)
      }
      continue
    }

    if (option === 'GET' && remaining >= 1) {
      const pattern = options[++j]!
      assertSortPatternAllowed(ctx, 'GET', pattern, key)
      scanned.get.push(pattern)
      continue
    }

    throw errors.syntax()
  }

  return scanned
}

// The source's type travels with its elements so the rest of SORT never looks
// the key up again — a second lookup is a second clock read, and the key could
// expire in between.
type SortSource = {
  type: 'list' | 'set' | 'zset' | null
  elements: Buffer[]
}

function readSortSource(db: RedisDatabase, key: Buffer): SortSource {
  // One lookup for both the type and the contents (#443).
  const value = db.get(key)
  if (!value) return { type: null, elements: [] }
  if (value.type === 'list') return { type: 'list', elements: value.values }
  if (value.type === 'set') {
    // setTypeIterator walks the set in storage order, which the members Map
    // keeps: ascending by value for an intset, insertion order otherwise (#504).
    return { type: 'set', elements: Array.from(value.members.values()) }
  }
  if (value.type === 'zset') {
    // sortCommand() converts the zset to a skiplist in place, for good
    // (`zsetConvert`, redis@7.2.4 sort.c:320) — SORT_RO too (#512) — and
    // walks it, so a zset source is read in rank order — the same order
    // ZRANGE reports, not insertion order (#418).
    db.updateSortedSet(key, zset => zset.convertToSkiplist())
    const members = getSortedMembers(value)
    return { type: 'zset', elements: members.map(entry => entry.member) }
  }
  throw new WrongTypeRedisError()
}

function sortNumericScore(
  weight: Buffer | null,
  profile: CompatibilityProfile,
): number {
  // A missing weight leaves the score at 0. Otherwise sort.c converts it with
  // the profile's strtod / fast_float (see parseSortScoreToken), and anything
  // that does not convert aborts the whole command.
  if (!weight) return 0
  const value = parseSortScoreToken(weight, profile)
  if (value === undefined) throw errors.sortScoreNotDouble()
  return value
}

/**
 * `LIMIT offset count`: both are full signed 64-bit integers
 * (`getLongFromObjectOrReply`). sortCommand() clamps them to the element
 * count, so a negative offset is 0 and a negative count means "the rest";
 * clamping to the safe-integer range here keeps that meaning.
 */
function parseSortLimit(
  offsetToken: Buffer,
  countToken: Buffer,
): { offset: number; count: number } {
  const offset = parseInt64Token(offsetToken)
  const count = parseInt64Token(countToken)
  const max = BigInt(Number.MAX_SAFE_INTEGER)
  return {
    offset: offset < 0n ? 0 : Number(offset > max ? max : offset),
    count: count < 0n ? -1 : Number(count > max ? max : count),
  }
}

/**
 * sortCompare()'s ALPHA branch: a missing BY weight (NULL `cmpobj`) orders
 * before any present one — even an empty string — and two missing weights tie.
 */
function compareSortAlphaWeights(a: Buffer | null, b: Buffer | null): number {
  if (a === null || b === null) {
    if (a === b) return 0
    return a === null ? -1 : 1
  }
  return Buffer.compare(a, b)
}

function sortElements(
  { type, elements }: SortSource,
  options: SortOptions,
  db: RedisDatabase,
  profile: CompatibilityProfile,
  announceMiss: boolean,
): Buffer[] {
  if (options.dontsort) {
    // A constant BY does not simply skip the sort: for DESC, sortCommand()'s
    // dontsort branch walks a list and a zset's skiplist from the tail toward
    // the head, and applies LIMIT to that reversed walk. A set has no such
    // branch, so there DESC really is a no-op (#426).
    const unsorted = [...elements]
    if (options.desc && type !== 'set') unsorted.reverse()
    return unsorted
  }

  // sortCompare() negates its result for DESC instead of reversing the sorted
  // vector, so elements that compare equal keep their load order in *both*
  // directions (#443) — which a stable sort on the negated comparison gives.
  const direction = options.desc ? -1 : 1
  const by = options.by

  if (options.alpha) {
    // Known differences from real Redis, both about elements that tie:
    // - With BY plus a LIMIT that does not cover the whole vector, real Redis
    //   uses its own partial quicksort (pqsort), which is not stable beyond
    //   six elements; the mock stays stable.
    // - A zset is loaded from its dict, whose hash seed is random per
    //   process: for `ZADD zr 3 c 2 b 1 a`, `SORT zr BY missing_* ALPHA` gave
    //   `b c a`, `c a b` and `a c b` across three starts of 8.0.6. The mock
    //   loads rank order. That order is undefined, so it is left untested.
    const weighted = elements.map(element => ({
      element,
      weight: by ? readSortPattern(db, by, element, announceMiss) : element,
    }))
    weighted.sort(
      (a, b) => direction * compareSortAlphaWeights(a.weight, b.weight),
    )
    return weighted.map(entry => entry.element)
  }

  const scored = elements.map(element => ({
    element,
    score: sortNumericScore(
      by ? readSortPattern(db, by, element, announceMiss) : element,
      profile,
    ),
  }))
  // sortCompare() falls through to compareStringObjects() on equal scores, so
  // ties are broken lexicographically rather than left in insertion order.
  scored.sort(
    (a, b) =>
      direction *
      (compareNumbers(a.score, b.score) ||
        Buffer.compare(a.element, b.element)),
  )
  return scored.map(entry => entry.element)
}

function compareNumbers(a: number, b: number): number {
  if (a > b) return 1
  if (a < b) return -1
  return 0
}

function applySortLimit(
  elements: Buffer[],
  limit: SortOptions['limit'],
): Buffer[] {
  if (!limit) return elements
  // Redis clamps a negative offset to 0; a negative count means "all remaining".
  const start = Math.max(0, limit.offset)
  if (start >= elements.length) return []
  if (limit.count < 0) return elements.slice(start)
  return elements.slice(start, start + limit.count)
}

/**
 * Mirrors `sortCommand()`'s determinism override: a constant `BY` normally
 * means "do not sort", but an unordered SET source whose output has to be
 * reproducible — it is written by STORE, or returned to a script — is
 * force-sorted ALPHA with the `BY` dropped. Lists have a defined order, so
 * only sets are overridden. Real Redis leaves zsets alone for the same reason:
 * `sortCommand()` walks the skiplist, so a zset source already arrives in rank
 * order — which `readSortSource` now reproduces (#418).
 */
function forceDeterministicSetOrder(
  options: SortOptions,
  type: SortSource['type'],
  ctx: RedisExecutionContext,
): SortOptions {
  if (!options.dontsort) {
    return options
  }
  if (!options.store && !ctx.inScript) {
    return options
  }
  if (type !== 'set') {
    return options
  }

  return { ...options, dontsort: false, by: undefined, alpha: true }
}

function runSort(
  args: SortArgs,
  ctx: RedisExecutionContext,
  allowStore: boolean,
) {
  const options = scanSortOptions(args, allowStore, ctx)
  const db = ctx.db
  const source = readSortSource(db, args.key)
  const effective = forceDeterministicSetOrder(options, source.type, ctx)
  // The BY / GET keys are read lookups (a missing one publishes `keymiss`),
  // except under STORE on 6.2, which looks them up for writing (6.2.14
  // sort.c:464).
  const announceMiss =
    !options.store ||
    ctx.server.profile.has('notify.keyspace.keymiss-store-sources')
  const sorted = applySortLimit(
    sortElements(source, effective, db, ctx.server.profile, announceMiss),
    options.limit,
  )
  const output = projectSortOutput(sorted, options.get, db, announceMiss)

  if (options.store) {
    // An empty result deletes the destination (`del`); otherwise the list
    // replaces whatever was there in one write, published as `sortstore` —
    // not `del` followed by the command name.
    if (output.length === 0) {
      db.delete(options.store)
    } else {
      db.withOrigin('sortstore').set(options.store, {
        type: 'list',
        values: output.map(value => value ?? Buffer.alloc(0)),
      })
    }
    return integer(output.length)
  }

  return array(output.map(element => RedisValue.bulkString(element)))
}

function projectSortOutput(
  elements: readonly Buffer[],
  get: readonly Buffer[],
  db: RedisDatabase,
  announceMiss: boolean,
): Array<Buffer | null> {
  if (get.length === 0) {
    return elements.map(element => Buffer.from(element))
  }

  const output: Array<Buffer | null> = []
  for (const element of elements) {
    for (const pattern of get) {
      output.push(
        isSelfSortPattern(pattern)
          ? Buffer.from(element)
          : readSortPattern(db, pattern, element, announceMiss),
      )
    }
  }
  return output
}

/**
 * Mirrors `lookupKeyByPattern()`: a pattern with no `*` resolves to nothing,
 * and a key holding anything other than a string resolves to nothing either —
 * real Redis never turns that into a WRONGTYPE for the whole SORT. A
 * `key*->field` pattern reads `field` of the hash `key<element>` instead. The
 * key is looked up for reading, so a missing one publishes `keymiss` when
 * `announceMiss` is set.
 */
function readSortPattern(
  db: RedisDatabase,
  pattern: Buffer,
  element: Buffer,
  announceMiss: boolean,
): Buffer | null {
  const wildcard = sortPatternWildcardIndex(pattern)
  if (wildcard === -1) {
    return null
  }

  const { key, field } = expandSortPattern(pattern, wildcard, element)
  const type = db.getType(key)
  if (type === null) {
    if (announceMiss) db.announceKeyMiss(key)
    return null
  }
  if (field) {
    if (type !== 'hash') return null
    return db.updateHash(key, hash => hash.getField(field)?.value ?? null)
  }
  if (type !== 'string') {
    return null
  }
  return db.getString(key)
}

/**
 * Substitutes `element` for the `*`. Like `lookupKeyByPattern()`, a `->`
 * after the `*` (found with `strstr`, so not past a NUL) followed by at least
 * one byte splits off a hash field.
 */
function expandSortPattern(
  pattern: Buffer,
  wildcard: number,
  element: Buffer,
): { key: Buffer; field: Buffer | null } {
  const tail = pattern.subarray(wildcard + 1)
  const nul = tail.indexOf(0)
  const arrow = tail.indexOf('->')
  const hasField =
    arrow !== -1 &&
    (nul === -1 || arrow + 1 < nul) &&
    arrow + 2 < tail.length &&
    tail[arrow + 2] !== 0
  const postfix = hasField ? tail.subarray(0, arrow) : tail
  return {
    key: Buffer.concat([pattern.subarray(0, wildcard), element, postfix]),
    field: hasField ? Buffer.from(tail.subarray(arrow + 2)) : null,
  }
}

/**
 * Port of `sortGetKeys()`: the source key and, when present, the STORE
 * destination — never the BY/GET patterns, whose cluster safety is checked by
 * SORT's own option scan. The options are not parsed until the command runs,
 * so this does its own light scan the way Redis does: skip LIMIT's two
 * arguments and BY's or GET's one, so a `STORE` in value position is not
 * mistaken for the option, and let the last STORE win.
 */
function sortRoutingKeys(args: SortArgs): Buffer[] {
  const { options } = args
  let store: Buffer | undefined

  for (let i = 0; i < options.length; i++) {
    const option = asciiUpperCase(options[i]!.toString())
    if (option === 'LIMIT') {
      i += 2
      continue
    }
    if (option === 'GET' || option === 'BY') {
      i += 1
      continue
    }
    if (option === 'STORE' && i + 1 < options.length) {
      store = options[i + 1]
    }
  }

  return store ? [args.key, store] : [args.key]
}

export const sortCommand = defineCommand({
  name: 'sort',
  rawKeys: sortGetKeys,
  schema: sortSchema(),
  flags: ['write', 'denyoom'],
  keys: sortRoutingKeys,
  execute: (args, ctx) => runSort(args, ctx, true),
})

export const sortRoCommand = defineCommand({
  name: 'sort_ro',
  since: { redis: '7.0.0', valkey: '7.2.0' },
  rawKeys: sortRoGetKeys,
  schema: sortSchema(),
  flags: ['readonly'],
  // sortROGetKeys() reports the source key only: SORT_RO has no STORE.
  keys: args => [args.key],
  execute: (args, ctx) => runSort(args, ctx, false),
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
  ctx: RedisExecutionContext,
  key: Buffer,
  duration: Buffer,
  seconds: boolean,
  commandName: string,
  options: ExpireOptions,
) {
  const now = Date.now()
  const expiresAt = keyExpireDeadlineMs(
    duration,
    seconds,
    now,
    commandName,
    ctx.server.profile,
  )
  return expireAtKey(ctx, key, expiresAt, options, now)
}

function expireAtKey(
  ctx: RedisExecutionContext,
  key: Buffer,
  expiresAt: number,
  options: ExpireOptions,
  now = Date.now(),
) {
  const db = ctx.db
  const expiration = db.getExpiration(key)
  if (expiration.kind === 'missing') {
    return integer(0)
  }

  if (!shouldApplyExpireOptions(expiration, expiresAt, options)) {
    return integer(0)
  }

  // A deadline already past deletes the key there and then: `del`, or
  // `expired` on Valkey 8.1+ (#527).
  if (expiresAt <= now) {
    return integer(deleteForPastDeadline(db, key, ctx.server.profile) ? 1 : 0)
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
