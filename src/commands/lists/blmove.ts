import { defineCommand } from '../../core/command-definition'
import { t } from '../../core/command-schema'
import { WrongNumberOfArgumentsError } from '../../core/redis-error'
import type { RedisExecutionContext } from '../../core/redis-context'
import { RedisResult } from '../../core/redis-result'
import { RedisValue } from '../../core/redis-value'
import { bulk } from '../helpers'
import { parseMoveDirection } from './helpers'
import { tryListMove } from './move'
import {
  blockOrTimeOut,
  blockingTimeoutMs,
  parseBlockingTimeout,
} from '../blocking'

type BlmoveArgs = {
  source: Buffer
  destination: Buffer
  fromDirection: 'left' | 'right'
  toDirection: 'left' | 'right'
  timeout: number
}

// Blocks until an element can move from `source` to `destination`. Shared by
// BLMOVE and BRPOPLPUSH. Real Redis answers an expired wait with a null array
// (`*-1`, replyToBlockedClientTimedOut), but a call that may not block (queued
// in MULTI, or from a script) with a null bulk string (`$-1`,
// blmoveGenericCommand's CLIENT_DENY_BLOCKING branch).
export function blockingListMove(
  source: Buffer,
  destination: Buffer,
  fromDirection: 'left' | 'right',
  toDirection: 'left' | 'right',
  timeoutMs: number,
  ctx: RedisExecutionContext,
): RedisResult | Promise<RedisResult> {
  return blockOrTimeOut(
    ctx,
    {
      keys: [source],
      type: 'list',
      timeoutMs: blockingTimeoutMs(timeoutMs),
      attempt: () =>
        tryListMove(source, destination, fromDirection, toDirection, ctx.db),
    },
    () =>
      ctx.inScript || ctx.transactionReplay
        ? bulk(null)
        : RedisResult.create(RedisValue.nullArray()),
  )
}

const BLMOVE_LAYOUT = { min: 5, max: 5, keys: [0, 1] }

export const blmoveCommand = defineCommand({
  name: 'blmove',
  since: { redis: '6.2.0', valkey: '7.2.0' },
  schema: t.custom<BlmoveArgs>(BLMOVE_LAYOUT, (input, index, ctx) => {
    const source = input[index]
    const destination = input[index + 1]
    if (!source || !destination)
      throw new WrongNumberOfArgumentsError(ctx.commandName)
    const fromDirection = parseMoveDirection(input[index + 2])
    const toDirection = parseMoveDirection(input[index + 3])
    const timeoutToken = input[index + 4]
    if (!timeoutToken) throw new WrongNumberOfArgumentsError(ctx.commandName)
    const timeout = parseBlockingTimeout(timeoutToken, ctx.profile)
    return {
      value: { source, destination, fromDirection, toDirection, timeout },
      nextIndex: index + 5,
    }
  }),
  flags: ['write', 'blocking'],
  keys: args => [args.source, args.destination],
  execute: (args, ctx) => {
    const immediate = tryListMove(
      args.source,
      args.destination,
      args.fromDirection,
      args.toDirection,
      ctx.db,
    )
    if (immediate) return immediate
    return blockingListMove(
      args.source,
      args.destination,
      args.fromDirection,
      args.toDirection,
      args.timeout,
      ctx,
    )
  },
})
