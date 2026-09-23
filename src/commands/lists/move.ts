import { defineCommand } from '../../core/command-definition'
import { t } from '../../core/command-schema'
import {
  RedisSyntaxError,
  WrongNumberOfArgumentsError,
} from '../../core/redis-error'
import { RedisResult } from '../../core/redis-result'
import type { RedisDatabase } from '../../state'
import { bulk } from '../helpers'
import { listPopEvent, listPushEvent } from './helpers'

function moveDirection(): ReturnType<typeof t.custom<'left' | 'right'>> {
  return t.custom<'left' | 'right'>({ min: 1, max: 1 }, (input, index, ctx) => {
    const token = input[index]
    if (!token) {
      throw new WrongNumberOfArgumentsError(ctx.commandName)
    }

    const direction = token.toString().toUpperCase()
    if (direction !== 'LEFT' && direction !== 'RIGHT') {
      throw new RedisSyntaxError()
    }

    return {
      value: direction === 'LEFT' ? 'left' : 'right',
      nextIndex: index + 1,
    }
  })
}

// Non-blocking LMOVE / RPOPLPUSH core. Returns a bulk-string result on
// success, or `null` when the source is empty/missing (the caller decides
// whether to block).
//
// Mirrors real Redis' order: the element is pushed onto the destination first
// (`lpush`/`rpush` on it), then popped from the source (`lpop`/`rpop`, then
// `del` if that emptied it). Pushing before popping also makes a same-key move
// a plain rotation — the list is never momentarily empty, so it is never
// deleted and recreated.
export function tryListMove(
  source: Buffer,
  destination: Buffer,
  fromDirection: 'left' | 'right',
  toDirection: 'left' | 'right',
  db: RedisDatabase,
): RedisResult | null {
  const sourceList = db.getList(source)
  if (!sourceList || sourceList.values.length === 0) return null

  // Validate destination type before mutating either key
  db.getList(destination)

  const values = sourceList.values
  const value = Buffer.from(
    fromDirection === 'left' ? values[0] : values[values.length - 1],
  )

  db.withOrigin(listPushEvent(toDirection)).updateList(destination, list => {
    if (toDirection === 'left') list.pushLeft([value])
    else list.pushRight([value])
  })
  db.withOrigin(listPopEvent(fromDirection)).updateList(source, list => {
    list.pop(fromDirection)
  })

  return bulk(value)
}

export const rpoplpushCommand = defineCommand({
  name: 'rpoplpush',
  schema: t.object({
    source: t.key(),
    destination: t.key(),
  }),
  flags: ['write', 'denyoom'],
  keys: args => [args.source, args.destination],
  execute: (args, ctx) =>
    tryListMove(args.source, args.destination, 'right', 'left', ctx.db) ??
    bulk(null),
})

export const lmoveCommand = defineCommand({
  name: 'lmove',
  since: { redis: '6.2.0', valkey: '7.2.0' },
  schema: t.object({
    source: t.key(),
    destination: t.key(),
    fromDirection: moveDirection(),
    toDirection: moveDirection(),
  }),
  flags: ['write', 'denyoom'],
  keys: args => [args.source, args.destination],
  execute: (args, ctx) =>
    tryListMove(
      args.source,
      args.destination,
      args.fromDirection,
      args.toDirection,
      ctx.db,
    ) ?? bulk(null),
})
