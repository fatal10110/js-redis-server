import { defineCommand } from '../../core/command-definition'
import { t } from '../../core/command-schema'
import {
  RedisSyntaxError,
  WrongNumberOfArgumentsError,
} from '../../core/redis-error'
import { integer } from '../helpers'

function insertPosition(): ReturnType<typeof t.custom<'before' | 'after'>> {
  return t.custom<'before' | 'after'>((input, index, ctx) => {
    const token = input[index]
    if (!token) {
      throw new WrongNumberOfArgumentsError(ctx.commandName)
    }

    const position = token.toString().toUpperCase()
    if (position !== 'BEFORE' && position !== 'AFTER') {
      throw new RedisSyntaxError()
    }

    return {
      value: position === 'BEFORE' ? 'before' : 'after',
      nextIndex: index + 1,
    }
  })
}

export const linsertCommand = defineCommand({
  name: 'linsert',
  schema: t.object({
    key: t.key(),
    position: insertPosition(),
    pivot: t.key(),
    element: t.key(),
  }),
  flags: ['write', 'denyoom'],
  keys: args => [args.key],
  execute: (args, ctx) => {
    const list = ctx.db.getList(args.key)
    if (!list) return integer(0)

    const length = ctx.db.updateList(args.key, list =>
      list.insertRelativeTo(args.pivot, args.element, args.position),
    )
    return integer(length)
  },
})
