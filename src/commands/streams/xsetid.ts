import { defineCommand } from '../../core/command-definition'
import { t, type ParseContext } from '../../core/command-schema'
import { WrongNumberOfArgumentsError, errors } from '../../core/redis-error'
import type { StreamId } from '../../state/data-types'
import { ok } from '../helpers'
import { compareStreamId, parseExactId, parseNonNegativeInteger } from './ids'

type XsetidArgs = {
  key: Buffer
  id: StreamId
  entriesAdded: number | null
  maxDeletedId: StreamId | null
}

function createXsetidSchema() {
  return t.custom<XsetidArgs>(
    { min: 2, keys: [0] },
    (input: readonly Buffer[], index: number, ctx: ParseContext) => {
      const key = input[index]
      const rawId = input[index + 1]
      if (!key || !rawId) {
        throw new WrongNumberOfArgumentsError(ctx.commandName)
      }

      let cursor = index + 2
      // ENTRIESADDED / MAXDELETEDID are 7.0+: 6.2 takes exactly `key id`.
      if (cursor < input.length && !ctx.profile.has('xsetid.entries-added')) {
        throw new WrongNumberOfArgumentsError(ctx.commandName)
      }

      let entriesAdded: number | null = null
      let maxDeletedId: StreamId | null = null
      while (cursor < input.length) {
        const option = input[cursor].toString().toUpperCase()
        if (option === 'ENTRIESADDED') {
          const rawEntriesAdded = input[cursor + 1]
          if (!rawEntriesAdded) throw errors.syntax()
          entriesAdded = parseNonNegativeInteger(rawEntriesAdded)
          cursor += 2
          continue
        }

        if (option === 'MAXDELETEDID') {
          const rawMaxDeletedId = input[cursor + 1]
          if (!rawMaxDeletedId) throw errors.syntax()
          maxDeletedId = parseExactId(rawMaxDeletedId.toString())
          cursor += 2
          continue
        }

        throw errors.syntax()
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
  introspection: {
    arity: profile => (profile.has('xsetid.entries-added') ? -3 : 3),
  },
  keys: args => [args.args.key],
  execute: (args, ctx) => {
    const command = args.args
    const stream = ctx.db.getStream(command.key)
    if (!stream) throw errors.noSuchKey()
    if (compareStreamId(command.id, stream.lastId) < 0) {
      throw errors.xsetidSmallerThanTop()
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
