import { defineCommand } from '../../core/command-definition'
import { isIntegerToken, t, type ParseContext } from '../../core/command-schema'
import { WrongNumberOfArgumentsError, errors } from '../../core/redis-error'
import type { RedisExecutionContext } from '../../core/redis-context'
import { RedisResult } from '../../core/redis-result'
import { RedisValue } from '../../core/redis-value'
import type { RedisDatabase } from '../../state'
import { listPopEvent, parseMoveDirection, parseTimeout } from './helpers'

type ListMultiPopArgs = {
  keys: Buffer[]
  side: 'left' | 'right'
  count: number
}

type BlockingListMultiPopArgs = ListMultiPopArgs & {
  timeout: number
}

function parsePositiveListPopInteger(
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

function parseListPopNumKeys(token: Buffer): number {
  return parsePositiveListPopInteger(token, () =>
    errors.numKeysGreaterThanZero(),
  )
}

function parseListPopCount(token: Buffer): number {
  return parsePositiveListPopInteger(token, errors.countGreaterThanZero)
}

function parseListMultiPopArgs(
  input: readonly Buffer[],
  index: number,
  ctx: ParseContext,
  options: { blocking: false },
): ListMultiPopArgs
function parseListMultiPopArgs(
  input: readonly Buffer[],
  index: number,
  ctx: ParseContext,
  options: { blocking: true },
): BlockingListMultiPopArgs
function parseListMultiPopArgs(
  input: readonly Buffer[],
  index: number,
  ctx: ParseContext,
  options: { blocking: boolean },
): ListMultiPopArgs | BlockingListMultiPopArgs {
  if (index >= input.length) {
    throw new WrongNumberOfArgumentsError(ctx.commandName)
  }

  let cursor = index
  let timeout: number | undefined

  if (options.blocking) {
    timeout = parseTimeout(input[cursor])
    cursor++
  }

  const numKeysToken = input[cursor]
  if (!numKeysToken) {
    throw new WrongNumberOfArgumentsError(ctx.commandName)
  }

  const numKeys = parseListPopNumKeys(numKeysToken)
  cursor++

  const keysEnd = cursor + numKeys
  if (keysEnd >= input.length) {
    throw errors.syntax()
  }

  const keys = Array.from(input.slice(cursor, keysEnd))
  cursor = keysEnd

  const side = parseMoveDirection(input[cursor])
  cursor++

  let count = 1
  if (cursor < input.length) {
    const option = input[cursor].toString().toUpperCase()
    if (option !== 'COUNT' || cursor + 2 !== input.length) {
      throw errors.syntax()
    }

    count = parseListPopCount(input[cursor + 1])
  }

  if (options.blocking) {
    return { timeout: timeout!, keys, side, count }
  }

  return { keys, side, count }
}

export function tryListMultiPop(
  keys: readonly Buffer[],
  side: 'left' | 'right',
  count: number,
  db: RedisDatabase,
): RedisResult | null {
  for (const key of keys) {
    const list = db.getList(key)
    if (!list || list.values.length === 0) continue

    // Published as the underlying lpop/rpop, as real Redis does (#446).
    const values = db
      .withOrigin(listPopEvent(side))
      .updateList(key, list => list.popMany(side, count))

    return RedisResult.create(
      RedisValue.array([
        RedisValue.bulkString(key),
        RedisValue.array(
          values.map((value: Buffer) => RedisValue.bulkString(value)),
        ),
      ]),
    )
  }

  return null
}

async function blockingListMultiPop(
  keys: readonly Buffer[],
  timeoutSecs: number,
  side: 'left' | 'right',
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

    const result = tryListMultiPop(keys, side, count, ctx.db)
    if (result) return result
  }
}

export const lmpopCommand = defineCommand({
  name: 'lmpop',
  since: { redis: '7.0.0', valkey: '7.2.0' },
  schema: t.custom<ListMultiPopArgs>({ min: 3 }, (input, index, ctx) => ({
    value: parseListMultiPopArgs(input, index, ctx, { blocking: false }),
    nextIndex: input.length,
  })),
  flags: ['write'],
  keys: args => args.keys,
  execute: (args, ctx) =>
    tryListMultiPop(args.keys, args.side, args.count, ctx.db) ??
    RedisResult.create(RedisValue.nullArray()),
})

export const blmpopCommand = defineCommand({
  name: 'blmpop',
  since: { redis: '7.0.0', valkey: '7.2.0' },
  schema: t.custom<BlockingListMultiPopArgs>(
    { min: 4 },
    (input, index, ctx) => ({
      value: parseListMultiPopArgs(input, index, ctx, { blocking: true }),
      nextIndex: input.length,
    }),
  ),
  flags: ['write', 'noscript'],
  keys: args => args.keys,
  execute: (args, ctx) => {
    const immediate = tryListMultiPop(args.keys, args.side, args.count, ctx.db)
    if (immediate) return immediate
    return blockingListMultiPop(
      args.keys,
      args.timeout,
      args.side,
      args.count,
      ctx,
    )
  },
})
