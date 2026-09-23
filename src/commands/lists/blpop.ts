import { defineCommand } from '../../core/command-definition'
import { t } from '../../core/command-schema'
import { WrongNumberOfArgumentsError, errors } from '../../core/redis-error'
import type { RedisExecutionContext } from '../../core/redis-context'
import { RedisResult } from '../../core/redis-result'
import { RedisValue } from '../../core/redis-value'
import type { RedisDatabase } from '../../state'
import { listPopEvent } from './helpers'
import { blockOnKeys, blockingTimeoutMs } from '../blocking'

export function tryListPop(
  keys: readonly Buffer[],
  side: 'left' | 'right',
  db: RedisDatabase,
): RedisResult | null {
  for (const key of keys) {
    const list = db.getList(key)
    if (!list || list.values.length === 0) continue

    // Published as the underlying lpop/rpop, as real Redis does (#446).
    const value = db
      .withOrigin(listPopEvent(side))
      .updateList(key, list => list.pop(side))
    return RedisResult.create(
      RedisValue.array([
        RedisValue.bulkString(key),
        RedisValue.bulkString(value),
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
  const result = await blockOnKeys(ctx, {
    keys,
    type: 'list',
    timeoutMs: blockingTimeoutMs(timeoutSecs),
    attempt: () => tryListPop(keys, side, ctx.db),
  })
  return result ?? RedisResult.create(RedisValue.nullArray())
}

export const blpopCommand = defineCommand({
  name: 'blpop',
  schema: t.custom<{ keys: Buffer[]; timeout: number }>(
    { min: 2, keyRange: { start: 0, step: 1, last: -2 } },
    (input, index, ctx) => {
      if (input.length - index < 2)
        throw new WrongNumberOfArgumentsError(ctx.commandName)
      const timeout = Number(input[input.length - 1].toString())
      if (isNaN(timeout) || timeout < 0) throw errors.syntax()
      const keys = Array.from(input.slice(index, input.length - 1))
      return { value: { keys, timeout }, nextIndex: input.length }
    },
  ),
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
  schema: t.custom<{ keys: Buffer[]; timeout: number }>(
    { min: 2, keyRange: { start: 0, step: 1, last: -2 } },
    (input, index, ctx) => {
      if (input.length - index < 2)
        throw new WrongNumberOfArgumentsError(ctx.commandName)
      const timeout = Number(input[input.length - 1].toString())
      if (isNaN(timeout) || timeout < 0) throw errors.syntax()
      const keys = Array.from(input.slice(index, input.length - 1))
      return { value: { keys, timeout }, nextIndex: input.length }
    },
  ),
  flags: ['write', 'noscript'],
  keys: args => args.keys,
  execute: (args, ctx) => {
    const immediate = tryListPop(args.keys, 'right', ctx.db)
    if (immediate) return immediate
    return blockingListPop(args.keys, args.timeout, 'right', ctx)
  },
})
