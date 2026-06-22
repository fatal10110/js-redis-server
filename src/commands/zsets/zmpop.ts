import { defineCommand } from '../../core/command-definition'
import { isIntegerToken, t, type ParseContext } from '../../core/command-schema'
import type { RedisExecutionContext } from '../../core/redis-context'
import { RedisResult } from '../../core/redis-result'
import {
  CountGreaterThanZeroError,
  NumKeysGreaterThanZeroError,
  RedisSyntaxError,
  TimeoutNegativeError,
  TimeoutNotFloatError,
  WrongNumberOfArgumentsError,
} from '../../core/redis-error'
import { RedisValue } from '../../core/redis-value'
import type { RedisDatabase } from '../../state'
import { scoreValue } from '../helpers'
import { deleteSortedSetIfEmpty, getSortedMembers } from './helpers'

type ZsetMultiPopSide = 'min' | 'max'

type ZsetMultiPopArgs = {
  keys: Buffer[]
  side: ZsetMultiPopSide
  count: number
}

type BlockingZsetMultiPopArgs = ZsetMultiPopArgs & {
  timeout: number
}

function parsePositiveZsetPopInteger(
  token: Buffer,
  createError: () => Error,
): number {
  const raw = token.toString()
  if (!isIntegerToken(raw)) {
    throw createError()
  }

  const value = Number(raw)
  if (!Number.isSafeInteger(value) || value <= 0) {
    throw createError()
  }

  return value
}

function parseZsetPopNumKeys(token: Buffer): number {
  return parsePositiveZsetPopInteger(
    token,
    () => new NumKeysGreaterThanZeroError(),
  )
}

function parseZsetPopCount(token: Buffer): number {
  return parsePositiveZsetPopInteger(
    token,
    () => new CountGreaterThanZeroError(),
  )
}

function parseZsetPopSide(token: Buffer | undefined): ZsetMultiPopSide {
  if (!token) throw new RedisSyntaxError()
  const side = token.toString().toUpperCase()
  if (side === 'MIN') return 'min'
  if (side === 'MAX') return 'max'
  throw new RedisSyntaxError()
}

function parseZsetPopTimeout(token: Buffer): number {
  const value = Number(token.toString())
  if (isNaN(value)) throw new TimeoutNotFloatError()
  if (value < 0) throw new TimeoutNegativeError()
  return value
}

function parseZsetMultiPopArgs(
  input: readonly Buffer[],
  index: number,
  ctx: ParseContext,
  options: { blocking: false },
): ZsetMultiPopArgs
function parseZsetMultiPopArgs(
  input: readonly Buffer[],
  index: number,
  ctx: ParseContext,
  options: { blocking: true },
): BlockingZsetMultiPopArgs
function parseZsetMultiPopArgs(
  input: readonly Buffer[],
  index: number,
  ctx: ParseContext,
  options: { blocking: boolean },
): ZsetMultiPopArgs | BlockingZsetMultiPopArgs {
  if (index >= input.length) {
    throw new WrongNumberOfArgumentsError(ctx.commandName)
  }

  let cursor = index
  let timeout: number | undefined

  if (options.blocking) {
    timeout = parseZsetPopTimeout(input[cursor])
    cursor++
  }

  const numKeysToken = input[cursor]
  if (!numKeysToken) {
    throw new WrongNumberOfArgumentsError(ctx.commandName)
  }

  const numKeys = parseZsetPopNumKeys(numKeysToken)
  cursor++

  const keysEnd = cursor + numKeys
  if (keysEnd >= input.length) {
    throw new RedisSyntaxError()
  }

  const keys = Array.from(input.slice(cursor, keysEnd))
  cursor = keysEnd

  const side = parseZsetPopSide(input[cursor])
  cursor++

  let count = 1
  if (cursor < input.length) {
    const option = input[cursor].toString().toUpperCase()
    if (option !== 'COUNT' || cursor + 2 !== input.length) {
      throw new RedisSyntaxError()
    }

    count = parseZsetPopCount(input[cursor + 1])
  }

  if (options.blocking) {
    return { timeout: timeout!, keys, side, count }
  }

  return { keys, side, count }
}

export function tryZsetMultiPop(
  keys: readonly Buffer[],
  side: ZsetMultiPopSide,
  count: number,
  db: RedisDatabase,
): RedisResult | null {
  for (const key of keys) {
    const zset = db.getSortedSet(key)
    if (!zset || zset.members.size === 0) continue

    const sorted = getSortedMembers(zset)
    const candidates = side === 'min' ? sorted : sorted.slice().reverse()
    const toRemove = candidates.slice(0, count)
    if (toRemove.length === 0) continue

    db.updateSortedSet(key, zset => {
      for (const entry of toRemove) {
        zset.deleteMember(entry.member)
      }
    })
    deleteSortedSetIfEmpty(db, key)

    return RedisResult.create(
      RedisValue.array([
        RedisValue.bulkString(key),
        RedisValue.array(
          toRemove.map(entry =>
            RedisValue.array([
              RedisValue.bulkString(entry.member),
              scoreValue(entry.score),
            ]),
          ),
        ),
      ]),
    )
  }

  return null
}

async function blockingZsetMultiPop(
  keys: readonly Buffer[],
  timeoutSecs: number,
  side: ZsetMultiPopSide,
  count: number,
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

    const result = tryZsetMultiPop(keys, side, count, ctx.db)
    if (result) return result
  }
}

export const zmpopCommand = defineCommand({
  name: 'zmpop',
  schema: t.custom<ZsetMultiPopArgs>((input, index, ctx) => ({
    value: parseZsetMultiPopArgs(input, index, ctx, { blocking: false }),
    nextIndex: input.length,
  })),
  flags: ['write'],
  keys: args => args.keys,
  execute: (args, ctx) =>
    tryZsetMultiPop(args.keys, args.side, args.count, ctx.db) ??
    RedisResult.create(RedisValue.nullArray()),
})

export const bzmpopCommand = defineCommand({
  name: 'bzmpop',
  schema: t.custom<BlockingZsetMultiPopArgs>((input, index, ctx) => ({
    value: parseZsetMultiPopArgs(input, index, ctx, { blocking: true }),
    nextIndex: input.length,
  })),
  flags: ['write', 'noscript'],
  keys: args => args.keys,
  execute: (args, ctx) => {
    const immediate = tryZsetMultiPop(args.keys, args.side, args.count, ctx.db)
    if (immediate) return immediate
    return blockingZsetMultiPop(
      args.keys,
      args.timeout,
      args.side,
      args.count,
      ctx,
    )
  },
})
