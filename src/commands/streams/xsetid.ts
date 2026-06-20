import { defineCommand } from '../../core/command-definition'
import { t, type ParseContext } from '../../core/command-schema'
import {
  NoSuchKeyError,
  RedisCommandError,
  RedisSyntaxError,
  WrongNumberOfArgumentsError,
} from '../../core/redis-error'
import type { StreamId } from '../../state/data-types'
import { ok } from '../helpers'
import { compareStreamId, parseExactId, parseNonNegativeInteger } from './ids'

class XsetidSmallerThanTopError extends RedisCommandError {
  constructor() {
    super(
      'The ID specified in XSETID is smaller than the target stream top item',
    )
  }
}

type XsetidArgs = {
  key: Buffer
  id: StreamId
  entriesAdded: number | null
  maxDeletedId: StreamId | null
}

function createXsetidSchema() {
  return t.custom<XsetidArgs>(
    (input: readonly Buffer[], index: number, ctx: ParseContext) => {
      const key = input[index]
      const rawId = input[index + 1]
      if (!key || !rawId) {
        throw new WrongNumberOfArgumentsError(ctx.commandName)
      }

      let cursor = index + 2
      let entriesAdded: number | null = null
      let maxDeletedId: StreamId | null = null
      while (cursor < input.length) {
        const option = input[cursor].toString().toUpperCase()
        if (option === 'ENTRIESADDED') {
          const rawEntriesAdded = input[cursor + 1]
          if (!rawEntriesAdded) throw new RedisSyntaxError()
          entriesAdded = parseNonNegativeInteger(rawEntriesAdded)
          cursor += 2
          continue
        }

        if (option === 'MAXDELETEDID') {
          const rawMaxDeletedId = input[cursor + 1]
          if (!rawMaxDeletedId) throw new RedisSyntaxError()
          maxDeletedId = parseExactId(rawMaxDeletedId.toString())
          cursor += 2
          continue
        }

        throw new RedisSyntaxError()
      }

      return {
        value: {
          key,
          id: parseExactId(rawId.toString()),
          entriesAdded,
          maxDeletedId,
        },
        nextIndex: input.length,
      }
    },
  )
}

export const xsetidCommand = defineCommand({
  name: 'xsetid',
  schema: t.object({ args: createXsetidSchema() }),
  flags: ['write', 'fast'],
  keys: args => [args.args.key],
  execute: (args, ctx) => {
    const command = args.args
    const stream = ctx.db.getStream(command.key)
    if (!stream) throw new NoSuchKeyError()
    if (compareStreamId(command.id, stream.lastId) < 0) {
      throw new XsetidSmallerThanTopError()
    }

    ctx.db.updateStream(command.key, writable => {
      writable.setId(command.id, {
        entriesAdded: command.entriesAdded,
        maxDeletedId: command.maxDeletedId,
      })
    })

    return ok()
  },
})
