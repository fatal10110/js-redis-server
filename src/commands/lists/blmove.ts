import { defineCommand } from '../../core/command-definition'
import { t } from '../../core/command-schema'
import { WrongNumberOfArgumentsError } from '../../core/redis-error'
import type { RedisExecutionContext } from '../../core/redis-context'
import { RedisResult } from '../../core/redis-result'
import { bulk } from '../helpers'
import { parseMoveDirection, parseTimeout } from './helpers'
import { tryListMove } from './move'

type BlmoveArgs = {
  source: Buffer
  destination: Buffer
  fromDirection: 'left' | 'right'
  toDirection: 'left' | 'right'
  timeout: number
}

async function blockingListMove(
  source: Buffer,
  destination: Buffer,
  fromDirection: 'left' | 'right',
  toDirection: 'left' | 'right',
  timeoutSecs: number,
  ctx: RedisExecutionContext,
): Promise<RedisResult> {
  const timeoutMs =
    timeoutSecs === 0 ? undefined : Math.ceil(timeoutSecs * 1000)
  const deadline = timeoutMs !== undefined ? Date.now() + timeoutMs : undefined

  while (true) {
    const remaining =
      deadline !== undefined ? Math.max(0, deadline - Date.now()) : undefined
    if (remaining === 0) return bulk(null)

    let wake!: (v: true) => void
    const waitFor = new Promise<true>(resolve => {
      wake = () => resolve(true)
    })

    const unsub = ctx.db.subscribeKey(source, event => {
      if (event.type === 'write') wake(true)
    })

    let woken: boolean | null
    try {
      woken = await ctx.park({
        waitFor,
        timeoutMs: remaining,
        signal: ctx.signal,
      })
    } finally {
      try {
        unsub()
      } catch {
        // ignore
      }
    }

    if (woken === null) return bulk(null)

    const result = tryListMove(
      source,
      destination,
      fromDirection,
      toDirection,
      ctx.db,
    )
    if (result) return result
  }
}

export const blmoveCommand = defineCommand({
  name: 'blmove',
  since: { redis: '6.2.0', valkey: '7.2.0' },
  schema: t.custom<BlmoveArgs>((input, index, ctx) => {
    const source = input[index]
    const destination = input[index + 1]
    if (!source || !destination)
      throw new WrongNumberOfArgumentsError(ctx.commandName)
    const fromDirection = parseMoveDirection(input[index + 2])
    const toDirection = parseMoveDirection(input[index + 3])
    const timeoutToken = input[index + 4]
    if (!timeoutToken) throw new WrongNumberOfArgumentsError(ctx.commandName)
    const timeout = parseTimeout(timeoutToken)
    return {
      value: { source, destination, fromDirection, toDirection, timeout },
      nextIndex: index + 5,
    }
  }),
  flags: ['write', 'noscript'],
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
