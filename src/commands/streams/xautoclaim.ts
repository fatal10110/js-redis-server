import { defineCommand } from '../../core/command-definition'
import { t, type ParseContext } from '../../core/command-schema'
import {
  RedisCommandError,
  RedisSyntaxError,
  WrongNumberOfArgumentsError,
} from '../../core/redis-error'
import { RedisValue } from '../../core/redis-value'
import type { StreamId } from '../../state/data-types'
import { array } from '../helpers'
import { createConsumerIfMissing, requireStreamGroup } from './groups'
import { incrementStreamId, parseLongLong, parseRangeId } from './ids'
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
    { min: 5, keys: [0] },
    (input: readonly Buffer[], index: number, ctx: ParseContext) => {
      const key = input[index]
      const group = input[index + 1]
      const consumer = input[index + 2]
      const rawMinIdle = input[index + 3]
      const rawStart = input[index + 4]
      if (!key || !group || !consumer || !rawMinIdle || !rawStart) {
        throw new WrongNumberOfArgumentsError(ctx.commandName)
      }

      // Real Redis validates every argument before it looks at the key (so a
      // bad argument beats WRONGTYPE / NOGROUP), each with its own message.
      const minIdle = parseLongLong(
        rawMinIdle,
        'Invalid min-idle-time argument for XAUTOCLAIM',
      )
      const bound = parseRangeId(rawStart.toString(), true)
      let start: StreamId | null = bound.id
      if (bound.exclusive) start = incrementStreamId(start)
      if (!start)
        throw new RedisCommandError('invalid start ID for the interval')

      let cursor = index + 5
      let count = 100
      let justId = false
      while (cursor < input.length) {
        const option = input[cursor].toString().toUpperCase()
        const hasValue = cursor + 1 < input.length
        if (option === 'COUNT' && hasValue) {
          count = parseXautoclaimCount(input[cursor + 1])
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
          minIdleMs: minIdle < 0n ? 0 : Number(minIdle),
          start,
          count,
          justId,
        },
        nextIndex: input.length,
      }
    },
  )
}

// Real Redis: COUNT is a long in [1, LONG_MAX / 16] (its attempts factor),
// and anything else — including a non-integer — is `ERR COUNT must be > 0`.
const MAX_XAUTOCLAIM_COUNT = ((1n << 63n) - 1n) / 16n

function parseXautoclaimCount(token: Buffer): number {
  const message = 'COUNT must be > 0'
  const count = parseLongLong(token, message)
  if (count < 1n || count > MAX_XAUTOCLAIM_COUNT) {
    throw new RedisCommandError(message)
  }
  return Number(count)
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
    createConsumerIfMissing(
      ctx.db,
      command.key,
      command.group,
      command.consumer,
      now,
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
