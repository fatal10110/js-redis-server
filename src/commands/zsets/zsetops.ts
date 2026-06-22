import { defineCommand } from '../../core/command-definition'
import { t, type ParseContext } from '../../core/command-schema'
import {
  AtLeastOneInputKeyError,
  LimitCantBeNegativeError,
  RedisSyntaxError,
  WeightNotFloatError,
  WrongNumberOfArgumentsError,
  WrongTypeRedisError,
} from '../../core/redis-error'
import { RedisResult } from '../../core/redis-result'
import { RedisValue } from '../../core/redis-value'
import type {
  RedisDatabase,
  RedisSortedSetData,
  RedisSortedSetMember,
} from '../../state'
import { array, integer, parseIntegerToken, scorePairs } from '../helpers'
import { getSortedMembers } from './helpers'

// ZUNIONSTORE/ZINTERSTORE/ZUNION/ZINTER/ZDIFF/ZDIFFSTORE/ZINTERCARD.
// Source keys may be sorted sets OR plain sets (a set is treated as a zset with
// every member scored 1). WEIGHTS multiply each source's scores before they are
// combined; AGGREGATE (SUM default, MIN, MAX) controls how scores merge. ZDIFF
// has neither WEIGHTS nor AGGREGATE — it just subtracts members, keeping the
// scores from the first key. STORE variants write the result to a destination
// key (deleting it when the result is empty) and reply with the cardinality.

type Aggregate = 'sum' | 'min' | 'max'

type SetOpArgs = {
  destination?: Buffer
  keys: Buffer[]
  weights?: number[]
  aggregate: Aggregate
  withScores: boolean
}

type ParserOptions = {
  hasDest: boolean
  weightsAggregate: boolean
}

// ---------------------------------------------------------------- shared logic

function getScoredMembers(
  db: RedisDatabase,
  key: Buffer,
): Map<string, RedisSortedSetMember> {
  const type = db.getType(key)
  if (type === null) return new Map()

  if (type === 'zset') {
    const zset = db.getSortedSet(key)!
    return new Map(
      Array.from(zset.members, ([hex, m]) => [
        hex,
        { member: m.member, score: m.score },
      ]),
    )
  }

  if (type === 'set') {
    const set = db.getSet(key)!
    return new Map(
      Array.from(set.members, ([hex, member]) => [hex, { member, score: 1 }]),
    )
  }

  throw new WrongTypeRedisError()
}

function weightedScore(score: number, weight: number): number {
  const result = weight * score
  // Redis resets a NaN weighted score (e.g. 0 * inf) to 0 before aggregating.
  return Number.isNaN(result) ? 0 : result
}

function aggregateScores(a: number, b: number, aggregate: Aggregate): number {
  if (aggregate === 'min') return Math.min(a, b)
  if (aggregate === 'max') return Math.max(a, b)
  const sum = a + b
  // SUM of +inf and -inf is NaN; Redis stores 0 in that case.
  return Number.isNaN(sum) ? 0 : sum
}

function weightAt(weights: number[] | undefined, index: number): number {
  return weights ? weights[index] : 1
}

function computeUnion(
  sources: Map<string, RedisSortedSetMember>[],
  weights: number[] | undefined,
  aggregate: Aggregate,
): Map<string, RedisSortedSetMember> {
  const result = new Map<string, RedisSortedSetMember>()
  for (let i = 0; i < sources.length; i++) {
    const weight = weightAt(weights, i)
    for (const [hex, { member, score }] of sources[i]) {
      const weighted = weightedScore(score, weight)
      const existing = result.get(hex)
      if (!existing) {
        result.set(hex, { member, score: weighted })
        continue
      }
      existing.score = aggregateScores(existing.score, weighted, aggregate)
    }
  }
  return result
}

function computeIntersection(
  sources: Map<string, RedisSortedSetMember>[],
  weights: number[] | undefined,
  aggregate: Aggregate,
): Map<string, RedisSortedSetMember> {
  const result = new Map<string, RedisSortedSetMember>()
  const [first, ...rest] = sources
  for (const [hex, { member, score }] of first) {
    let acc = weightedScore(score, weightAt(weights, 0))
    let present = true
    for (let i = 0; i < rest.length; i++) {
      const entry = rest[i].get(hex)
      if (!entry) {
        present = false
        break
      }
      acc = aggregateScores(
        acc,
        weightedScore(entry.score, weightAt(weights, i + 1)),
        aggregate,
      )
    }
    if (present) result.set(hex, { member, score: acc })
  }
  return result
}

function computeDifference(
  sources: Map<string, RedisSortedSetMember>[],
): Map<string, RedisSortedSetMember> {
  const [first, ...rest] = sources
  const result = new Map(first)
  for (const source of rest) {
    for (const hex of source.keys()) result.delete(hex)
  }
  return result
}

function buildScoredResult(
  members: Map<string, RedisSortedSetMember>,
  withScores: boolean,
): RedisResult {
  const sorted = getSortedMembers({
    type: 'zset',
    members,
  } as RedisSortedSetData)
  if (withScores) {
    return RedisResult.create(scorePairs(sorted))
  }
  return array(sorted.map(entry => RedisValue.bulkString(entry.member)))
}

function storeResult(
  db: RedisDatabase,
  destination: Buffer,
  members: Map<string, RedisSortedSetMember>,
): number {
  if (members.size === 0) {
    db.delete(destination)
    return 0
  }
  db.updateSortedSet(destination, zset => {
    zset.replaceMembers(members, { forceDirty: true })
  })
  return members.size
}

// ------------------------------------------------------------------- parsing

function parseWeight(token: Buffer): number {
  const raw = token.toString()
  const normalized = raw.toLowerCase()
  if (normalized === 'inf' || normalized === '+inf') return Infinity
  if (normalized === '-inf') return -Infinity

  const value = Number(raw)
  if (raw.trim() === '' || !Number.isFinite(value)) {
    throw new WeightNotFloatError()
  }
  return value
}

function parseSetOp(
  input: readonly Buffer[],
  index: number,
  ctx: ParseContext,
  options: ParserOptions,
): SetOpArgs {
  let cursor = index

  let destination: Buffer | undefined
  if (options.hasDest) {
    destination = input[cursor]
    if (!destination) throw new WrongNumberOfArgumentsError(ctx.commandName)
    cursor++
  }

  const numkeysToken = input[cursor]
  if (!numkeysToken) throw new WrongNumberOfArgumentsError(ctx.commandName)
  cursor++

  const numkeys = parseIntegerToken(numkeysToken)
  if (numkeys <= 0) throw new AtLeastOneInputKeyError(ctx.commandName)
  if (cursor + numkeys > input.length) throw new RedisSyntaxError()

  const keys = input.slice(cursor, cursor + numkeys)
  cursor += numkeys

  let weights: number[] | undefined
  let aggregate: Aggregate = 'sum'
  let withScores = false

  while (cursor < input.length) {
    const option = input[cursor]!.toString().toUpperCase()

    if (options.weightsAggregate && option === 'WEIGHTS') {
      cursor++
      weights = []
      for (let i = 0; i < numkeys; i++) {
        const token = input[cursor]
        if (!token) throw new RedisSyntaxError()
        weights.push(parseWeight(token))
        cursor++
      }
      continue
    }

    if (options.weightsAggregate && option === 'AGGREGATE') {
      cursor++
      const token = input[cursor]
      if (!token) throw new RedisSyntaxError()
      const value = token.toString().toUpperCase()
      if (value === 'SUM') aggregate = 'sum'
      else if (value === 'MIN') aggregate = 'min'
      else if (value === 'MAX') aggregate = 'max'
      else throw new RedisSyntaxError()
      cursor++
      continue
    }

    // WITHSCORES is only valid for the non-store ZUNION/ZINTER/ZDIFF variants.
    if (!options.hasDest && option === 'WITHSCORES') {
      withScores = true
      cursor++
      continue
    }

    throw new RedisSyntaxError()
  }

  return { destination, keys, weights, aggregate, withScores }
}

function setOpSchema(options: ParserOptions) {
  return t.custom<SetOpArgs>((input, index, ctx) => ({
    value: parseSetOp(input, index, ctx, options),
    nextIndex: input.length,
  }))
}

// ------------------------------------------------------------------- commands

export const zunionstoreCommand = defineCommand({
  name: 'zunionstore',
  schema: setOpSchema({ hasDest: true, weightsAggregate: true }),
  flags: ['write', 'denyoom'],
  keys: args => [args.destination!, ...args.keys],
  execute: (args, ctx) => {
    const sources = args.keys.map(k => getScoredMembers(ctx.db, k))
    const result = computeUnion(sources, args.weights, args.aggregate)
    return integer(storeResult(ctx.db, args.destination!, result))
  },
})

export const zinterstoreCommand = defineCommand({
  name: 'zinterstore',
  schema: setOpSchema({ hasDest: true, weightsAggregate: true }),
  flags: ['write', 'denyoom'],
  keys: args => [args.destination!, ...args.keys],
  execute: (args, ctx) => {
    const sources = args.keys.map(k => getScoredMembers(ctx.db, k))
    const result = computeIntersection(sources, args.weights, args.aggregate)
    return integer(storeResult(ctx.db, args.destination!, result))
  },
})

export const zdiffstoreCommand = defineCommand({
  name: 'zdiffstore',
  schema: setOpSchema({ hasDest: true, weightsAggregate: false }),
  flags: ['write', 'denyoom'],
  keys: args => [args.destination!, ...args.keys],
  execute: (args, ctx) => {
    const sources = args.keys.map(k => getScoredMembers(ctx.db, k))
    const result = computeDifference(sources)
    return integer(storeResult(ctx.db, args.destination!, result))
  },
})

export const zunionCommand = defineCommand({
  name: 'zunion',
  schema: setOpSchema({ hasDest: false, weightsAggregate: true }),
  flags: ['readonly'],
  keys: args => args.keys,
  execute: (args, ctx) => {
    const sources = args.keys.map(k => getScoredMembers(ctx.db, k))
    const result = computeUnion(sources, args.weights, args.aggregate)
    return buildScoredResult(result, args.withScores)
  },
})

export const zinterCommand = defineCommand({
  name: 'zinter',
  schema: setOpSchema({ hasDest: false, weightsAggregate: true }),
  flags: ['readonly'],
  keys: args => args.keys,
  execute: (args, ctx) => {
    const sources = args.keys.map(k => getScoredMembers(ctx.db, k))
    const result = computeIntersection(sources, args.weights, args.aggregate)
    return buildScoredResult(result, args.withScores)
  },
})

export const zdiffCommand = defineCommand({
  name: 'zdiff',
  schema: setOpSchema({ hasDest: false, weightsAggregate: false }),
  flags: ['readonly'],
  keys: args => args.keys,
  execute: (args, ctx) => {
    const sources = args.keys.map(k => getScoredMembers(ctx.db, k))
    const result = computeDifference(sources)
    return buildScoredResult(result, args.withScores)
  },
})

// ZINTERCARD numkeys key [key ...] [LIMIT limit]
type ZintercardArgs = { keys: Buffer[]; limit: number }

const zintercardSchema = t.custom<ZintercardArgs>((input, index, ctx) => {
  let cursor = index

  const numkeysToken = input[cursor]
  if (!numkeysToken) throw new WrongNumberOfArgumentsError(ctx.commandName)
  cursor++

  const numkeys = parseIntegerToken(numkeysToken)
  if (numkeys <= 0) throw new AtLeastOneInputKeyError(ctx.commandName)
  if (cursor + numkeys > input.length) throw new RedisSyntaxError()

  const keys = input.slice(cursor, cursor + numkeys)
  cursor += numkeys

  let limit = 0
  if (cursor < input.length) {
    if (input[cursor]!.toString().toUpperCase() !== 'LIMIT') {
      throw new RedisSyntaxError()
    }
    cursor++
    const limitToken = input[cursor]
    if (!limitToken) throw new RedisSyntaxError()
    const value = Number(limitToken.toString())
    if (!Number.isSafeInteger(value) || value < 0) {
      throw new LimitCantBeNegativeError()
    }
    limit = value
    cursor++
    if (cursor !== input.length) throw new RedisSyntaxError()
  }

  return { value: { keys, limit }, nextIndex: input.length }
})

export const zintercardCommand = defineCommand({
  name: 'zintercard',
  schema: zintercardSchema,
  flags: ['readonly'],
  keys: args => args.keys,
  execute: (args, ctx) => {
    const sources = args.keys.map(k => getScoredMembers(ctx.db, k))
    const count = computeIntersection(sources, undefined, 'sum').size
    if (args.limit > 0) return integer(Math.min(count, args.limit))
    return integer(count)
  },
})
