import { asciiUpperToken } from '../../core/ascii-case'
import { defineCommand } from '../../core/command-definition'
import { t, type ParseContext } from '../../core/command-schema'
import { WrongNumberOfArgumentsError } from '../../core/redis-error'
import { RedisValue } from '../../core/redis-value'
import { array } from '../helpers'
import {
  consumerPendingCount,
  pendingEntriesSorted,
  requireStreamGroup,
} from './groups'
import {
  bufferId,
  compareStreamId,
  exclusiveAware,
  parseNonNegativeInteger,
  parseRangeId,
  type RangeBound,
} from './ids'
import { bulkString, integerValue, nullBulk, streamIdValue } from './replies'

type XpendingArgs =
  | { mode: 'summary'; key: Buffer; group: Buffer }
  | {
      mode: 'range'
      key: Buffer
      group: Buffer
      minIdleMs: number | null
      start: RangeBound
      end: RangeBound
      count: number
      consumer: Buffer | null
    }

function createXpendingSchema() {
  return t.custom<XpendingArgs>(
    { min: 2, keys: [0] },
    (input: readonly Buffer[], index: number, ctx: ParseContext) => {
      const key = input[index]
      const group = input[index + 1]
      if (!key || !group) throw new WrongNumberOfArgumentsError(ctx.commandName)

      if (input.length === index + 2) {
        return {
          value: { mode: 'summary', key, group },
          nextIndex: input.length,
        }
      }

      let cursor = index + 2
      let minIdleMs: number | null = null
      if (asciiUpperToken(input[cursor]) === 'IDLE') {
        const raw = input[cursor + 1]
        if (!raw) throw new WrongNumberOfArgumentsError(ctx.commandName)
        minIdleMs = parseNonNegativeInteger(raw)
        cursor += 2
      }

      const startToken = input[cursor]?.toString()
      const endToken = input[cursor + 1]?.toString()
      const rawCount = input[cursor + 2]
      if (startToken === undefined || endToken === undefined || !rawCount) {
        throw new WrongNumberOfArgumentsError(ctx.commandName)
      }
      cursor += 3

      const consumer = input[cursor] ?? null
      if (cursor + (consumer ? 1 : 0) !== input.length) {
        throw new WrongNumberOfArgumentsError(ctx.commandName)
      }

      return {
        value: {
          mode: 'range',
          key,
          group,
          minIdleMs,
          start: parseRangeId(startToken, true),
          end: parseRangeId(endToken, false),
          count: parseNonNegativeInteger(rawCount),
          consumer,
        },
        nextIndex: input.length,
      }
    },
  )
}

export const xpendingCommand = defineCommand({
  name: 'xpending',
  schema: t.object({ args: createXpendingSchema() }),
  flags: ['readonly'],
  keys: args => [args.args.key],
  execute: (args, ctx) => {
    const command = args.args
    const stream = ctx.db.getStream(command.key)
    const group = requireStreamGroup(stream, command.key, command.group)

    if (command.mode === 'summary') {
      const pending = pendingEntriesSorted(group)
      if (pending.length === 0) {
        return array([
          integerValue(0),
          nullBulk(),
          nullBulk(),
          RedisValue.nullArray(),
        ])
      }

      return array([
        integerValue(pending.length),
        streamIdValue(pending[0].id),
        streamIdValue(pending[pending.length - 1].id),
        // The consumers with something pending, by name in byte order, each
        // with its count as a bulk string (real Redis' addReplyBulkLongLong).
        RedisValue.array(
          Array.from(group.consumers.entries())
            .sort(([, a], [, b]) => Buffer.compare(a.name, b.name))
            .flatMap(([consumerId, consumer]) => {
              const count = consumerPendingCount(group, consumerId)
              return count === 0
                ? []
                : [
                    RedisValue.array([
                      bulkString(consumer.name),
                      bulkString(String(count)),
                    ]),
                  ]
            }),
        ),
      ])
    }

    const now = Date.now()
    const consumerId = command.consumer ? bufferId(command.consumer) : null
    const replies: RedisValue[] = []
    for (const pending of pendingEntriesSorted(group)) {
      if (
        !exclusiveAware(
          compareStreamId(pending.id, command.start.id),
          command.start.exclusive,
          true,
        )
      ) {
        continue
      }
      if (
        !exclusiveAware(
          compareStreamId(pending.id, command.end.id),
          command.end.exclusive,
          false,
        )
      ) {
        continue
      }
      if (consumerId !== null && pending.consumerId !== consumerId) continue

      const idleMs = Math.max(0, now - pending.deliveredAt)
      if (command.minIdleMs !== null && idleMs < command.minIdleMs) continue

      const consumer = group.consumers.get(pending.consumerId)
      replies.push(
        RedisValue.array([
          streamIdValue(pending.id),
          bulkString(consumer?.name ?? Buffer.from(pending.consumerId, 'hex')),
          integerValue(idleMs),
          integerValue(pending.deliveryCount),
        ]),
      )
      if (replies.length >= command.count) break
    }

    return array(replies)
  },
})
