import { defineCommand } from '../../core/command-definition'
import { t } from '../../core/command-schema'
import {
  RedisSyntaxError,
  WrongNumberOfArgumentsError,
} from '../../core/redis-error'
import type { RedisExecutionContext } from '../../core/redis-context'
import { RedisResult } from '../../core/redis-result'
import { RedisValue } from '../../core/redis-value'
import type { RedisDatabase } from '../../state'

export function tryListPop(
  keys: readonly Buffer[],
  side: 'left' | 'right',
  db: RedisDatabase,
): RedisResult | null {
  for (const key of keys) {
    const list = db.getList(key)
    if (!list || list.values.length === 0) continue

    const result = db.updateList(key, list => {
      const value = side === 'left' ? list.values.shift()! : list.values.pop()!
      return { value, empty: list.values.length === 0 }
    })
    if (result.empty) db.delete(key)
    return RedisResult.create(
      RedisValue.array([
        RedisValue.bulkString(key),
        RedisValue.bulkString(result.value),
      ]),
    )
  }
  return null
}

async function blockingListPop(
  keys: readonly Buffer[],
  timeoutSecs: number,
  side: 'left' | 'right',
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

    const result = tryListPop(keys, side, ctx.db)
    if (result) return result
  }
}

export const blpopCommand = defineCommand({
  name: 'blpop',
  schema: t.custom<{ keys: Buffer[]; timeout: number }>((input, index, ctx) => {
    if (input.length - index < 2)
      throw new WrongNumberOfArgumentsError(ctx.commandName)
    const timeout = Number(input[input.length - 1].toString())
    if (isNaN(timeout) || timeout < 0) throw new RedisSyntaxError()
    const keys = Array.from(input.slice(index, input.length - 1))
    return { value: { keys, timeout }, nextIndex: input.length }
  }),
  flags: ['write', 'noscript'],
  keys: args => args.keys,
  execute: (args, ctx) => {
    const immediate = tryListPop(args.keys, 'left', ctx.db)
    if (immediate) return immediate
    return blockingListPop(args.keys, args.timeout, 'left', ctx)
  },
})

export const brpopCommand = defineCommand({
  name: 'brpop',
  schema: t.custom<{ keys: Buffer[]; timeout: number }>((input, index, ctx) => {
    if (input.length - index < 2)
      throw new WrongNumberOfArgumentsError(ctx.commandName)
    const timeout = Number(input[input.length - 1].toString())
    if (isNaN(timeout) || timeout < 0) throw new RedisSyntaxError()
    const keys = Array.from(input.slice(index, input.length - 1))
    return { value: { keys, timeout }, nextIndex: input.length }
  }),
  flags: ['write', 'noscript'],
  keys: args => args.keys,
  execute: (args, ctx) => {
    const immediate = tryListPop(args.keys, 'right', ctx.db)
    if (immediate) return immediate
    return blockingListPop(args.keys, args.timeout, 'right', ctx)
  },
})
