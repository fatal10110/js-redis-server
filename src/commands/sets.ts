import { defineCommand } from '../core/command-definition'
import { numkeysGetKeys } from '../core/key-specs'
import { t } from '../core/command-schema'
import { integer, bulk, array } from './helpers'
import { RedisValue } from '../core/redis-value'
import {
  WrongNumberOfArgumentsError,
  WrongTypeRedisError,
  errors,
} from '../core/redis-error'
import {
  createSetData,
  type RedisDatabase,
  type RedisServerState,
  type RedisSetData,
} from '../state'
import {
  addSetMember,
  convertToIntsetIfPossible,
  type SetEncodingRules,
} from '../state/set-encoding'
import { configuredSetMaxIntsetEntries } from './config'

// ---------------------------------------------------------------------------
// Internal helpers
// ---------------------------------------------------------------------------

/** The server settings that decide when a set is an intset (#504). */
export function setEncodingRules(server: RedisServerState): SetEncodingRules {
  return {
    maxIntsetEntries: configuredSetMaxIntsetEntries(server),
    listpack: server.profile.has('set.listpack-encoding'),
  }
}

function getSetMembers(db: RedisDatabase, key: Buffer): Set<string> {
  const setData = db.getSet(key) // throws WrongTypeRedisError if wrong type
  if (!setData) return new Set()
  return new Set(setData.members.keys())
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

/**
 * `sinterGenericCommand()`'s walk: the sets sorted smallest first (a stable
 * sort, so equal sizes keep argument order), and the members of the smallest
 * that every other set holds, in its storage order. A missing key is an empty
 * set, so it empties the result.
 */
function intersectInOrder(db: RedisDatabase, keys: Buffer[]): Buffer[] {
  const sets = keys.map(key => db.getSet(key))
  const present: RedisSetData[] = []
  for (const set of sets) {
    if (!set) return []
    present.push(set)
  }
  present.sort((a, b) => a.members.size - b.members.size)
  const [smallest, ...rest] = present
  const result: Buffer[] = []
  for (const [hex, member] of smallest.members) {
    if (rest.every(set => set.members.has(hex))) result.push(member)
  }
  return result
}

/**
 * `sunionDiffGenericCommand()`: the result is built in a fresh set, member by
 * member, so it goes through the same conversions SADD does. It starts as an
 * empty intset, so integers stay sorted until the first non-integer arrives
 * — except that on Redis 8.0 / Valkey 8.0+ (`set.union-diff-hashtable`) a
 * reply-only result (no STORE destination) starts as a hashtable when any
 * source is not an intset, and so keeps the order it walks the sources in.
 * That is Valkey 8.1+'s order for a small result; a Redis 8.0 / Valkey 8.0
 * hashtable's order is undefined.
 */
function unionOrDiff(
  ctx: { db: RedisDatabase; server: RedisServerState },
  keys: Buffer[],
  op: 'union' | 'diff',
  store: boolean,
): RedisSetData {
  const { db } = ctx
  const rules = setEncodingRules(ctx.server)
  const sets = keys.map(key => db.getSet(key)) // WRONGTYPE for every key first
  const hashtable =
    !store &&
    ctx.server.profile.has('set.union-diff-hashtable') &&
    sets.some(set => !!set && !set.intset)
  const result = createSetData({ intset: !hashtable })

  if (op === 'union') {
    for (const set of sets) {
      if (!set) continue
      for (const member of set.members.values()) {
        addSetMember(result, member, rules)
      }
    }
    return result
  }

  const [first, ...others] = sets
  if (!first) return result
  // The first key given again as a later set empties the difference.
  if (keys.slice(1).some(key => key.equals(keys[0]))) return result

  // Pick the algorithm the way Redis does: #1 walks the first set and tests
  // each member against the others; #2 adds the first set whole and removes
  // every other set's members. They can leave the result in different
  // encodings, so the choice is observable in its order.
  let algoOneWork = 0
  let algoTwoWork = 0
  for (const set of sets) {
    if (!set) continue
    algoOneWork += first.members.size
    algoTwoWork += set.members.size
  }
  algoOneWork = Math.floor(algoOneWork / 2)

  if (algoOneWork <= algoTwoWork) {
    for (const [hex, member] of first.members) {
      if (others.some(set => set?.members.has(hex))) continue
      addSetMember(result, member, rules)
    }
    return result
  }

  for (const member of first.members.values()) {
    addSetMember(result, member, rules)
  }
  for (const set of others) {
    if (result.members.size === 0) break
    if (!set) continue
    for (const hex of set.members.keys()) {
      result.members.delete(hex)
    }
  }
  return result
}

function storeSetResult(
  db: RedisDatabase,
  destKey: Buffer,
  result: RedisSetData,
): number {
  if (result.members.size === 0) {
    db.delete(destKey)
    return 0
  }
  db.updateSet(destKey, set => {
    set.replaceWith(result, { forceDirty: true })
  })
  return result.members.size
}

function bulkMembers(members: Iterable<Buffer>) {
  return array(Array.from(members, member => RedisValue.bulkString(member)))
}

function parseSintercardCount(token: Buffer): number {
  const count = Number(token.toString())
  if (!Number.isSafeInteger(count) || count <= 0) {
    throw errors.numKeysGreaterThanZero()
  }

  return count
}

function parseSintercardLimit(token: Buffer): number {
  const limit = Number(token.toString())
  if (!Number.isSafeInteger(limit) || limit < 0) {
    throw errors.limitCantBeNegative()
  }

  return limit
}

const sintercardSchema = t.custom<{
  keys: Buffer[]
  limit: number
}>({ min: 2 }, (input, _index, ctx) => {
  if (input.length < 2) {
    throw new WrongNumberOfArgumentsError(ctx.commandName)
  }

  const keyCount = parseSintercardCount(input[0])
  if (keyCount > input.length - 1) {
    throw errors.wrongNumberOfKeys()
  }

  const keys = input.slice(1, 1 + keyCount)
  let cursor = 1 + keyCount
  let limit = 0

  if (cursor >= input.length) {
    return { value: { keys, limit }, nextIndex: cursor }
  }

  const option = input[cursor].toString().toUpperCase()
  if (option !== 'LIMIT') {
    throw errors.syntax()
  }

  cursor++
  if (cursor >= input.length) {
    throw errors.syntax()
  }

  limit = parseSintercardLimit(input[cursor])
  cursor++

  if (cursor !== input.length) {
    throw errors.syntax()
  }

  return { value: { keys, limit }, nextIndex: cursor }
})

// ---------------------------------------------------------------------------
// SADD key member [member ...]
// ---------------------------------------------------------------------------

export const saddCommand = defineCommand({
  name: 'sadd',
  schema: t.object({ key: t.key(), members: t.variadic(t.bulk(), { min: 1 }) }),
  flags: ['write', 'denyoom', 'fast'],
  keys: args => [args.key],
  execute: (args, ctx) => {
    const rules = setEncodingRules(ctx.server)
    const added = ctx.db.updateSet(args.key, set => {
      set.prepareForAdd(args.members[0], args.members.length, rules)
      let count = 0
      for (const member of args.members) {
        if (set.addMember(member, rules)) count++
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
  schema: t.object({ key: t.key(), members: t.variadic(t.bulk(), { min: 1 }) }),
  flags: ['write', 'fast'],
  keys: args => [args.key],
  execute: (args, ctx) => {
    if (ctx.db.getType(args.key) === null) return integer(0)
    let removed = 0
    ctx.db.updateSet(args.key, set => {
      for (const member of args.members) {
        if (set.deleteMember(member)) removed++
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
  schema: t.object({ key: t.key(), member: t.bulk() }),
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
  since: { redis: '6.2.0', valkey: '7.2.0' },
  schema: t.object({ key: t.key(), members: t.variadic(t.bulk(), { min: 1 }) }),
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

/** spopWithCountCommand() rebuilds the set when few members survive. */
const SPOP_MOVE_STRATEGY_MUL = 5

/**
 * `count` distinct random positions in `0..size-1`, in ascending order.
 * Popped and sampled members are replied in storage order: that is the order
 * Redis replies a listpack's sample in, and for an intset or hashtable, whose
 * sample it replies in random order, it is one random order among others.
 */
function randomPositions(size: number, count: number): number[] {
  const pool = Array.from({ length: size }, (_, i) => i)
  for (let i = 0; i < count; i++) {
    const j = i + Math.floor(Math.random() * (size - i))
    ;[pool[i], pool[j]] = [pool[j], pool[i]]
  }
  return pool.slice(0, count).sort((a, b) => a - b)
}

export const spopCommand = defineCommand({
  name: 'spop',
  schema: t.object({ key: t.key(), count: t.optional(t.integer()) }),
  flags: ['write', 'random', 'fast', 'noscript'],
  keys: args => [args.key],
  execute: (args, ctx) => {
    if (args.count !== undefined && args.count < 0) {
      throw errors.positiveCount()
    }

    const type = ctx.db.getType(args.key)
    if (type === null) return args.count === undefined ? bulk(null) : array([])
    if (type !== 'set') throw new WrongTypeRedisError()

    if (args.count === undefined) {
      const member = ctx.db.updateSet(args.key, set => {
        const entries = set.memberEntries()
        const [hex, picked] =
          entries[Math.floor(Math.random() * entries.length)]
        set.deleteMemberId(hex)
        return picked
      })
      return bulk(member)
    }

    const count = args.count
    if (count === 0) return array([])

    const rules = setEncodingRules(ctx.server)
    const popped = ctx.db.updateSet(args.key, set => {
      const entries = set.memberEntries()

      // The whole set is popped: Redis replies with an SUNION of the key.
      if (count >= entries.length) {
        const union = unionOrDiff(ctx, [args.key], 'union', false)
        for (const [hex] of entries) set.deleteMemberId(hex)
        return Array.from(union.members.values())
      }

      const members: Buffer[] = []
      for (const position of randomPositions(entries.length, count)) {
        const [hex, member] = entries[position]
        set.deleteMemberId(hex)
        members.push(member)
      }

      // When few members survive, Redis moves them into a new set. Through
      // 7.0 that set is created from its first member, so all-integer
      // survivors of a non-intset set become an intset; from 7.2 a
      // non-intset set is rebuilt as a listpack, in its old order.
      const remaining = entries.length - count
      if (
        remaining * SPOP_MOVE_STRATEGY_MUL <= count &&
        !rules.listpack &&
        !set.intset
      ) {
        set.convertToIntsetIfPossible(rules)
      }
      return members
    })
    return bulkMembers(popped)
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
      // Distinct members, up to count. Asking for at least the whole set
      // returns it in storage order, as Redis iterates it.
      if (count >= values.length) return bulkMembers(values)
      return bulkMembers(
        randomPositions(values.length, count).map(position => values[position]),
      )
    }

    // With repetition, |count| members in random order.
    const result: Buffer[] = []
    for (let i = 0; i < -count; i++) {
      result.push(values[Math.floor(Math.random() * values.length)])
    }
    return bulkMembers(result)
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
    const diff = unionOrDiff(ctx, args.keys, 'diff', false)
    return bulkMembers(diff.members.values())
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
  execute: (args, ctx) => bulkMembers(intersectInOrder(ctx.db, args.keys)),
})

// ---------------------------------------------------------------------------
// SINTERCARD numkeys key [key ...] [LIMIT limit]
// ---------------------------------------------------------------------------

export const sintercardCommand = defineCommand({
  name: 'sintercard',
  rawKeys: numkeysGetKeys(0, 1, 2),
  since: { redis: '7.0.0', valkey: '7.2.0' },
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
    const union = unionOrDiff(ctx, args.keys, 'union', false)
    return bulkMembers(union.members.values())
  },
})

// ---------------------------------------------------------------------------
// SMOVE source destination member
// ---------------------------------------------------------------------------

export const smoveCommand = defineCommand({
  name: 'smove',
  schema: t.object({ source: t.key(), destination: t.key(), member: t.bulk() }),
  flags: ['write', 'fast'],
  keys: args => [args.source, args.destination],
  execute: (args, ctx) => {
    const sourceType = ctx.db.getType(args.source)
    if (sourceType === null) return integer(0)
    if (sourceType !== 'set') throw new WrongTypeRedisError()

    // validate destination type before mutating source
    const destType = ctx.db.getType(args.destination)
    if (destType !== null && destType !== 'set') throw new WrongTypeRedisError()

    // Same key: real Redis only reports membership, touching nothing.
    if (args.source.equals(args.destination)) {
      return integer(
        ctx.db.getSet(args.source)?.members.has(args.member.toString('hex'))
          ? 1
          : 0,
      )
    }

    // Published as the underlying srem / sadd, as real Redis does (#446).
    const moved = ctx.db
      .withOrigin('srem')
      .updateSet(args.source, set => set.deleteMember(args.member))
    if (!moved) return integer(0)

    const rules = setEncodingRules(ctx.server)
    ctx.db.withOrigin('sadd').updateSet(args.destination, set => {
      // A new destination is created for its one member, as SADD would.
      if (set.size === 0) set.prepareForAdd(args.member, 1, rules)
      set.addMember(args.member, rules)
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
    const diff = unionOrDiff(ctx, args.keys, 'diff', true)
    return integer(storeSetResult(ctx.db, args.destination, diff))
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
    // The members arrive in the smallest set's order. A result made only of
    // integers is then stored as an intset, sorted (`maybeConvertToIntset`).
    const inter = createSetData()
    for (const member of intersectInOrder(ctx.db, args.keys)) {
      inter.members.set(member.toString('hex'), member)
    }
    convertToIntsetIfPossible(inter, setEncodingRules(ctx.server))
    return integer(storeSetResult(ctx.db, args.destination, inter))
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
    const union = unionOrDiff(ctx, args.keys, 'union', true)
    return integer(storeSetResult(ctx.db, args.destination, union))
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
