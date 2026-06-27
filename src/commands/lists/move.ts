import { defineCommand } from '../../core/command-definition'
import { t } from '../../core/command-schema'
import {
  RedisSyntaxError,
  WrongNumberOfArgumentsError,
} from '../../core/redis-error'
import { RedisResult } from '../../core/redis-result'
import type { RedisDatabase } from '../../state'
import { bulk } from '../helpers'

function moveDirection(): ReturnType<typeof t.custom<'left' | 'right'>> {
  return t.custom<'left' | 'right'>((input, index, ctx) => {
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

// Non-blocking LMOVE core. Returns a bulk-string result on success, or `null`
// when the source is empty/missing (the caller decides whether to block).
export function tryListMove(
  source: Buffer,
  destination: Buffer,
  fromDirection: 'left' | 'right',
  toDirection: 'left' | 'right',
  db: RedisDatabase,
): RedisResult | null {
  const sourceList = db.getList(source)
  if (!sourceList || sourceList.values.length === 0) return null

  // Validate destination type before mutating the source
  db.getList(destination)

  const popped = db.updateList(source, list => {
    const value = list.pop(fromDirection)
    return { value, empty: list.length === 0 }
  })
  if (popped.empty) db.delete(source)
  if (popped.value === null) return null

  db.updateList(destination, list => {
    if (toDirection === 'left') list.pushLeft([popped.value!])
    else list.pushRight([popped.value!])
  })

  return bulk(popped.value)
}

export const rpoplpushCommand = defineCommand({
  name: 'rpoplpush',
  schema: t.object({
    source: t.key(),
    destination: t.key(),
  }),
  flags: ['write', 'denyoom'],
  keys: args => [args.source, args.destination],
  execute: (args, ctx) => {
    const sourceList = ctx.db.getList(args.source)
    if (!sourceList || sourceList.values.length === 0) return bulk(null)

    // Validate destination type before mutating
    ctx.db.getList(args.destination)

    const value = ctx.db.updateList(args.source, list => {
      const val = list.pop('right')
      return { val, empty: list.length === 0 }
    })
    if (value.empty) ctx.db.delete(args.source)
    if (value.val === null) return bulk(null)

    ctx.db.updateList(args.destination, list => {
      list.pushLeft([value.val!])
    })

    return bulk(value.val)
  },
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
