import { defineCommand } from '../../core/command-definition'
import { t } from '../../core/command-schema'
import type { RedisExecutionContext } from '../../core/redis-context'
import {
  PositiveCountError,
  TimeoutNegativeError,
  TimeoutNotFloatError,
  WrongNumberOfArgumentsError,
} from '../../core/redis-error'
import { RedisResult } from '../../core/redis-result'
import { RedisValue } from '../../core/redis-value'
import type { RedisDatabase } from '../../state'
import { array, scorePairs, scoreValue } from '../helpers'
import { deleteSortedSetIfEmpty, getSortedMembers } from './helpers'

type ZsetPopSide = 'min' | 'max'

function parsePopCountArg(s: string): number {
  if (!/^-?\d+$/.test(s)) {
    throw new PositiveCountError()
  }

  const count = Number(s)
  if (!Number.isSafeInteger(count) || count < 0) {
    throw new PositiveCountError()
  }

  return count
}

function zpopCountSchema() {
  return t.custom<number>((input, index, ctx) => {
    const token = input[index]
    if (!token) {
      throw new WrongNumberOfArgumentsError(ctx.commandName)
    }

    return {
      value: parsePopCountArg(token.toString()),
      nextIndex: index + 1,
    }
  })
}

export const zpopminCommand = defineCommand({
  name: 'zpopmin',
  schema: t.object({ key: t.key(), count: t.optional(zpopCountSchema()) }),
  flags: ['write', 'fast'],
  keys: args => [args.key],
  execute: (args, ctx) => {
    const zset = ctx.db.getSortedSet(args.key)
    if (!zset) return array([])
    const count = args.count ?? 1
    const sorted = getSortedMembers(zset)
    const toRemove = sorted.slice(0, count)
    if (toRemove.length === 0) return array([])
    ctx.db.updateSortedSet(args.key, z => {
      for (const entry of toRemove) {
        z.deleteMember(entry.member)
      }
    })
    deleteSortedSetIfEmpty(ctx.db, args.key)
    return zpopReply(toRemove, args.count !== undefined)
  },
})

export const zpopmaxCommand = defineCommand({
  name: 'zpopmax',
  schema: t.object({ key: t.key(), count: t.optional(zpopCountSchema()) }),
  flags: ['write', 'fast'],
  keys: args => [args.key],
  execute: (args, ctx) => {
    const zset = ctx.db.getSortedSet(args.key)
    if (!zset) return array([])
    const count = args.count ?? 1
    const sorted = getSortedMembers(zset).slice().reverse()
    const toRemove = sorted.slice(0, count)
    if (toRemove.length === 0) return array([])
    ctx.db.updateSortedSet(args.key, z => {
      for (const entry of toRemove) {
        z.deleteMember(entry.member)
      }
    })
    deleteSortedSetIfEmpty(ctx.db, args.key)
    return zpopReply(toRemove, args.count !== undefined)
  },
})

// ZPOPMIN/ZPOPMAX: without a count the reply is a flat `[member, score]`; with
// an explicit count it is an array of `[member, score]` pairs (nested on RESP3).
function zpopReply(
  members: readonly { member: Buffer; score: number }[],
  hasCount: boolean,
): RedisResult {
  if (hasCount) {
    return RedisResult.create(scorePairs(members))
  }
  const [entry] = members
  return array([RedisValue.bulkString(entry.member), scoreValue(entry.score)])
}

type BlockingZsetPopArgs = {
  keys: Buffer[]
  timeout: number
}

function parseBlockingZsetPopArgs(
  input: readonly Buffer[],
  index: number,
  commandName: string,
): BlockingZsetPopArgs {
  // BZPOPMIN/BZPOPMAX take `key [key ...] timeout` — at least one key plus the
  // trailing timeout, so fewer than two remaining tokens is an arity error.
  if (input.length - index < 2) {
    throw new WrongNumberOfArgumentsError(commandName)
  }

  const timeout = Number(input[input.length - 1].toString())
  if (isNaN(timeout)) throw new TimeoutNotFloatError()
  if (timeout < 0) throw new TimeoutNegativeError()

  const keys = Array.from(input.slice(index, input.length - 1))
  return { keys, timeout }
}

// Pops a single min/max member from the first non-empty sorted set among the
// keys, returning the flat `[key, member, score]` reply BZPOPMIN/BZPOPMAX use
// (distinct from ZMPOP's nested shape). Throws WRONGTYPE via getSortedSet if a
// scanned key holds a non-zset value, matching real Redis.
function tryBlockingZsetPop(
  keys: readonly Buffer[],
  side: ZsetPopSide,
  db: RedisDatabase,
): RedisResult | null {
  for (const key of keys) {
    const zset = db.getSortedSet(key)
    if (!zset || zset.members.size === 0) continue

    const sorted = getSortedMembers(zset)
    const entry = side === 'min' ? sorted[0] : sorted[sorted.length - 1]

    db.updateSortedSet(key, z => {
      z.deleteMember(entry.member)
    })
    deleteSortedSetIfEmpty(db, key)

    return RedisResult.create(
      RedisValue.array([
        RedisValue.bulkString(key),
        RedisValue.bulkString(entry.member),
        scoreValue(entry.score),
      ]),
    )
  }

  return null
}

async function blockingZsetPop(
  keys: readonly Buffer[],
  timeoutSecs: number,
  side: ZsetPopSide,
  ctx: RedisExecutionContext,
): Promise<RedisResult> {
  const timeoutMs =
    timeoutSecs === 0 ? undefined : Math.ceil(timeoutSecs * 1000)
  const deadline = timeoutMs !== undefined ? Date.now() + timeoutMs : undefined

  while (true) {
    const remaining =
      deadline !== undefined ? Math.max(0, deadline - Date.now()) : undefined
    if (remaining === 0) return RedisResult.create(RedisValue.nullArray())

    let wake!: (v: true) => void
    const waitFor = new Promise<true>(resolve => {
      wake = () => resolve(true)
    })

    const unsubs = keys.map(key =>
      ctx.db.subscribeKey(key, event => {
        if (event.type === 'write') wake(true)
      }),
    )

    let woken: boolean | null
    try {
      woken = await ctx.park({
        waitFor,
        timeoutMs: remaining,
        signal: ctx.signal,
      })
    } finally {
      for (const unsub of unsubs) {
        try {
          unsub()
        } catch {
          // ignore errors from individual unsubscribers so all are attempted
        }
      }
    }

    if (woken === null) return RedisResult.create(RedisValue.nullArray())

    const result = tryBlockingZsetPop(keys, side, ctx.db)
    if (result) return result
  }
}

function defineBlockingZsetPop(name: string, side: ZsetPopSide) {
  return defineCommand({
    name,
    schema: t.custom<BlockingZsetPopArgs>((input, index, ctx) => ({
      value: parseBlockingZsetPopArgs(input, index, ctx.commandName),
      nextIndex: input.length,
    })),
    flags: ['write', 'noscript'],
    keys: args => args.keys,
    execute: (args, ctx) => {
      const immediate = tryBlockingZsetPop(args.keys, side, ctx.db)
      if (immediate) return immediate
      return blockingZsetPop(args.keys, args.timeout, side, ctx)
    },
  })
}

export const bzpopminCommand = defineBlockingZsetPop('bzpopmin', 'min')
export const bzpopmaxCommand = defineBlockingZsetPop('bzpopmax', 'max')
