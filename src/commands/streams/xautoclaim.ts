import { defineCommand } from '../../core/command-definition'
import { t, type ParseContext } from '../../core/command-schema'
import {
  RedisSyntaxError,
  WrongNumberOfArgumentsError,
} from '../../core/redis-error'
import { RedisValue } from '../../core/redis-value'
import type { StreamId } from '../../state/data-types'
import { array } from '../helpers'
import {
  ensureConsumer,
  findEntry,
  pendingEntriesSorted,
  requireStreamGroup,
} from './groups'
import {
  bufferId,
  cloneStreamId,
  compareStreamId,
  MIN_ID,
  parseExactId,
  parseNonNegativeInteger,
  streamIdKey,
} from './ids'
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
  schema: t.object({ args: createXautoclaimSchema() }),
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
    const result = ctx.db.updateStream(command.key, stream => {
      const group = requireStreamGroup(stream, command.key, command.group)
      ensureConsumer(group, command.consumer, now).activeAt = now
      const consumerId = bufferId(command.consumer)
      const claimed: RedisValue[] = []
      const deleted: RedisValue[] = []
      let nextStartId: StreamId = MIN_ID

      for (const pending of pendingEntriesSorted(group)) {
        if (compareStreamId(pending.id, command.start) < 0) continue

        const entry = findEntry(stream, pending.id)
        if (!entry) {
          group.pending.delete(streamIdKey(pending.id))
          deleted.push(streamIdValue(pending.id))
          continue
        }

        const idleTime = Math.max(0, now - pending.deliveredAt)
        if (idleTime < command.minIdleMs) continue

        pending.consumerId = consumerId
        pending.deliveredAt = now
        if (!command.justId) pending.deliveryCount++
        claimed.push(
          command.justId
            ? streamIdValue(entry.id)
            : entryToReply(entry.id, entry.fields),
        )

        if (claimed.length >= command.count) {
          const next = pendingEntriesSorted(group).find(
            item => compareStreamId(item.id, pending.id) > 0,
          )
          nextStartId = next ? cloneStreamId(next.id) : MIN_ID
          break
        }
      }

      return { nextStartId, claimed, deleted }
    })

    return array([
      streamIdValue(result.nextStartId),
      RedisValue.array(result.claimed),
      RedisValue.array(result.deleted),
    ])
  },
})
