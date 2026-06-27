import { defineCommand } from '../../core/command-definition'
import { t, type ParseContext } from '../../core/command-schema'
import {
  RedisSyntaxError,
  WrongNumberOfArgumentsError,
} from '../../core/redis-error'
import { RedisValue } from '../../core/redis-value'
import type { StreamId } from '../../state/data-types'
import { array } from '../helpers'
import { requireStreamGroup } from './groups'
import { parseExactId, parseNonNegativeInteger } from './ids'
import { entryToReply, streamIdValue } from './replies'

type XautoclaimArgs = {
  key: Buffer
  group: Buffer
  consumer: Buffer
  minIdleMs: number
  start: StreamId
  count: number
  justId: boolean
}

function createXautoclaimSchema() {
  return t.custom<XautoclaimArgs>(
    (input: readonly Buffer[], index: number, ctx: ParseContext) => {
      const key = input[index]
      const group = input[index + 1]
      const consumer = input[index + 2]
      const rawMinIdle = input[index + 3]
      const rawStart = input[index + 4]
      if (!key || !group || !consumer || !rawMinIdle || !rawStart) {
        throw new WrongNumberOfArgumentsError(ctx.commandName)
      }

      let cursor = index + 5
      let count = 100
      let justId = false
      while (cursor < input.length) {
        const option = input[cursor].toString().toUpperCase()
        if (option === 'COUNT') {
          const rawCount = input[cursor + 1]
          if (!rawCount) throw new WrongNumberOfArgumentsError(ctx.commandName)
          count = parseNonNegativeInteger(rawCount)
          cursor += 2
          continue
        }

        if (option === 'JUSTID') {
          justId = true
          cursor++
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
          start: parseExactId(rawStart.toString()),
          count,
          justId,
        },
        nextIndex: input.length,
      }
    },
  )
}

export const xautoclaimCommand = defineCommand({
  name: 'xautoclaim',
  since: { redis: '6.2.0', valkey: '7.2.0' },
  schema: t.object({ args: createXautoclaimSchema() }),
  flags: ['write'],
  keys: args => [args.args.key],
  execute: (args, ctx) => {
    const command = args.args
    const now = Date.now()
    const includeDeletedIds = ctx.server.profile.has(
      'stream.xautoclaim-deleted-ids',
    )
    requireStreamGroup(
      ctx.db.getStream(command.key),
      command.key,
      command.group,
    )
    const result = ctx.db.updateStream(command.key, stream => {
      const group = requireStreamGroup(stream.value, command.key, command.group)
      return stream.autoClaim(
        group,
        command.consumer,
        {
          minIdleMs: command.minIdleMs,
          start: command.start,
          count: command.count,
          justId: command.justId,
          cleanDeletedEntries: includeDeletedIds,
        },
        now,
      )
    })

    const claimed = result.claimed.map(entry =>
      command.justId
        ? streamIdValue(entry.id)
        : entry.fields === null
          ? RedisValue.bulkString(null)
          : entryToReply(entry.id, entry.fields),
    )
    const deleted = result.deleted.map(id => streamIdValue(id))

    const reply = [streamIdValue(result.nextStartId), RedisValue.array(claimed)]
    if (includeDeletedIds) {
      reply.push(RedisValue.array(deleted))
    }
    return array(reply)
  },
})
