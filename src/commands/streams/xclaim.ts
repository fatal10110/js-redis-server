import { defineCommand } from '../../core/command-definition'
import { t, type ParseContext } from '../../core/command-schema'
import {
  RedisSyntaxError,
  WrongNumberOfArgumentsError,
} from '../../core/redis-error'
import type { StreamId } from '../../state/data-types'
import { array } from '../helpers'
import { requireStreamGroup } from './groups'
import { parseExactId, parseNonNegativeInteger } from './ids'
import { entryToReply, streamIdValue } from './replies'

type XclaimArgs = {
  key: Buffer
  group: Buffer
  consumer: Buffer
  minIdleMs: number
  ids: StreamId[]
  idleMs: number | null
  timeMs: number | null
  retryCount: number | null
  force: boolean
  justId: boolean
  lastId: StreamId | null
}

function createXclaimSchema() {
  return t.custom<XclaimArgs>(
    (input: readonly Buffer[], index: number, ctx: ParseContext) => {
      const key = input[index]
      const group = input[index + 1]
      const consumer = input[index + 2]
      const rawMinIdle = input[index + 3]
      if (!key || !group || !consumer || !rawMinIdle) {
        throw new WrongNumberOfArgumentsError(ctx.commandName)
      }

      let cursor = index + 4
      const ids: StreamId[] = []
      while (cursor < input.length && !isXclaimOption(input[cursor])) {
        ids.push(parseExactId(input[cursor].toString()))
        cursor++
      }
      if (ids.length === 0)
        throw new WrongNumberOfArgumentsError(ctx.commandName)

      let idleMs: number | null = null
      let timeMs: number | null = null
      let retryCount: number | null = null
      let force = false
      let justId = false
      let lastId: StreamId | null = null

      while (cursor < input.length) {
        const option = input[cursor].toString().toUpperCase()
        if (option === 'IDLE' || option === 'TIME' || option === 'RETRYCOUNT') {
          const rawValue = input[cursor + 1]
          if (!rawValue) throw new WrongNumberOfArgumentsError(ctx.commandName)
          const value = parseNonNegativeInteger(rawValue)
          if (option === 'IDLE') idleMs = value
          if (option === 'TIME') timeMs = value
          if (option === 'RETRYCOUNT') retryCount = value
          cursor += 2
          continue
        }

        if (option === 'FORCE') {
          force = true
          cursor++
          continue
        }

        if (option === 'JUSTID') {
          justId = true
          cursor++
          continue
        }

        if (option === 'LASTID') {
          const rawValue = input[cursor + 1]
          if (!rawValue) throw new WrongNumberOfArgumentsError(ctx.commandName)
          lastId = parseExactId(rawValue.toString())
          cursor += 2
          continue
        }

        throw new RedisSyntaxError()
      }

      return {
        value: {
          key,
          group,
          consumer,
          minIdleMs: parseNonNegativeInteger(rawMinIdle),
          ids,
          idleMs,
          timeMs,
          retryCount,
          force,
          justId,
          lastId,
        },
        nextIndex: input.length,
      }
    },
  )
}

function isXclaimOption(token: Buffer): boolean {
  const option = token.toString().toUpperCase()
  return (
    option === 'IDLE' ||
    option === 'TIME' ||
    option === 'RETRYCOUNT' ||
    option === 'FORCE' ||
    option === 'JUSTID' ||
    option === 'LASTID'
  )
}

export const xclaimCommand = defineCommand({
  name: 'xclaim',
  schema: t.object({ args: createXclaimSchema() }),
  flags: ['write'],
  keys: args => [args.args.key],
  execute: (args, ctx) => {
    const command = args.args
    const now = Date.now()
    requireStreamGroup(
      ctx.db.getStream(command.key),
      command.key,
      command.group,
    )
    const claimed = ctx.db.updateStream(command.key, stream => {
      const group = requireStreamGroup(stream.value, command.key, command.group)
      return stream.claim(
        group,
        command.consumer,
        command.ids,
        {
          minIdleMs: command.minIdleMs,
          idleMs: command.idleMs,
          timeMs: command.timeMs,
          retryCount: command.retryCount,
          force: command.force,
          justId: command.justId,
          lastId: command.lastId,
        },
        now,
      )
    })

    const replies = claimed.map(entry =>
      command.justId
        ? streamIdValue(entry.id)
        : entryToReply(entry.id, entry.fields),
    )
    return array(replies)
  },
})
