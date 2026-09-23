import { asciiUpperCase } from '../../core/ascii-case'
import { defineCommand } from '../../core/command-definition'
import { t, type ParseContext } from '../../core/command-schema'
import {
  NoSuchKeyError,
  WrongNumberOfArgumentsError,
} from '../../core/redis-error'
import { RedisResult } from '../../core/redis-result'
import { RedisValue } from '../../core/redis-value'
import type {
  RedisStreamConsumer,
  RedisStreamConsumerGroup,
  RedisStreamData,
} from '../../state/data-types'
import {
  array,
  subcommandSyntaxError,
  unknownSubcommandError,
} from '../helpers'
import {
  consumerPendingCount,
  pendingEntriesSorted,
  requireStreamGroup,
  streamLag,
} from './groups'
import { parseNonNegativeInteger } from './ids'
import {
  bulkString,
  entryToReply,
  integerValue,
  nullBulk,
  streamIdValue,
} from './replies'

type XinfoArgs =
  | { subcommand: 'stream'; key: Buffer; full: boolean; count: number | null }
  | { subcommand: 'groups'; key: Buffer }
  | { subcommand: 'consumers'; key: Buffer; group: Buffer }

function isToken(arg: Buffer, token: string): boolean {
  return arg.toString().toUpperCase() === token
}

function createXinfoSchema() {
  return t.custom<XinfoArgs>(
    { min: 1 },
    (input: readonly Buffer[], index: number, ctx: ParseContext) => {
      const rawSubcommand = input[index]
      if (!rawSubcommand) {
        throw new WrongNumberOfArgumentsError(ctx.commandName)
      }
      const subcommand = asciiUpperCase(rawSubcommand.toString())
      // A parser only knows the container (`ctx.commandName`), so arity errors
      // for a dispatched subcommand spell out `xinfo|<sub>` themselves, as real
      // Redis 7.0+ does (#438). An option list the subcommand cannot use is
      // real Redis' `addReplySubcommandSyntaxError`, not an arity error.

      if (subcommand === 'STREAM') {
        const key = input[index + 1]
        if (!key) throw new WrongNumberOfArgumentsError('xinfo|stream')

        // `[FULL [COUNT <count>]]`: nothing, `FULL`, or `FULL COUNT <count>`.
        const options = input.slice(index + 2)
        const valid =
          options.length === 0 ||
          (isToken(options[0], 'FULL') &&
            (options.length === 1 ||
              (options.length === 3 && isToken(options[1], 'COUNT'))))
        if (!valid) {
          throw subcommandSyntaxError('XINFO', rawSubcommand, ctx.profile)
        }
        const full = options.length > 0
        const count =
          options.length === 3 ? parseNonNegativeInteger(options[2]) : null

        return {
          value: { subcommand: 'stream', key, full, count },
          nextIndex: input.length,
        }
      }

      if (subcommand === 'GROUPS') {
        const key = input[index + 1]
        if (!key || input.length !== index + 2) {
          throw new WrongNumberOfArgumentsError('xinfo|groups')
        }
        return {
          value: { subcommand: 'groups', key },
          nextIndex: input.length,
        }
      }

      if (subcommand === 'CONSUMERS') {
        const key = input[index + 1]
        const group = input[index + 2]
        if (!key || !group || input.length !== index + 3) {
          throw new WrongNumberOfArgumentsError('xinfo|consumers')
        }
        return {
          value: { subcommand: 'consumers', key, group },
          nextIndex: input.length,
        }
      }

      throw unknownSubcommandError('XINFO', rawSubcommand, ctx.profile)
    },
  )
}

export const xinfoCommand = defineCommand({
  name: 'xinfo',
  schema: t.object({ args: createXinfoSchema() }),
  flags: ['readonly'],
  keys: args => [args.args.key],
  execute: (args, ctx) => {
    const command = args.args
    const stream = ctx.db.getStream(command.key)
    if (!stream) throw new NoSuchKeyError()

    if (command.subcommand === 'stream') {
      // XINFO replies are field/value maps: a flat array on RESP2, a `%` map on
      // RESP3 (matching real Redis). first/last-entry and PEL rows stay arrays.
      return RedisResult.create(
        kvMap(streamInfoReply(stream, command.full, command.count)),
      )
    }

    if (command.subcommand === 'groups') {
      return array(Array.from(stream.groups.values(), groupInfoReply(stream)))
    }

    const group = requireStreamGroup(stream, command.key, command.group)
    const now = Date.now()
    return array(
      Array.from(group.consumers.entries()).map(([consumerId, consumer]) =>
        consumerInfoReply(group, consumerId, consumer, now),
      ),
    )
  },
})

// Pair a flat [key, value, ...] list into a `map` reply: encoded flat on RESP2
// (unchanged from the previous array form) and as a `%` map on RESP3.
function kvMap(flat: RedisValue[]): RedisValue {
  const entries: [RedisValue, RedisValue][] = []
  for (let i = 0; i < flat.length; i += 2) {
    entries.push([flat[i], flat[i + 1]])
  }
  return RedisValue.map(entries)
}

function streamInfoReply(
  stream: RedisStreamData,
  full: boolean,
  count: number | null,
): RedisValue[] {
  const firstEntry = stream.entries[0] ?? null
  const lastEntry = stream.entries[stream.entries.length - 1] ?? null

  const fields: RedisValue[] = [
    bulkString('length'),
    integerValue(stream.entries.length),
    bulkString('radix-tree-keys'),
    integerValue(stream.entries.length > 0 ? 1 : 0),
    bulkString('radix-tree-nodes'),
    integerValue(stream.entries.length > 0 ? 2 : 1),
    bulkString('last-generated-id'),
    streamIdValue(stream.lastId),
    bulkString('max-deleted-entry-id'),
    streamIdValue(stream.maxDeletedEntryId),
    bulkString('entries-added'),
    integerValue(stream.entriesAdded),
    bulkString('recorded-first-entry-id'),
    firstEntry ? streamIdValue(firstEntry.id) : bulkString('0-0'),
    bulkString('groups'),
  ]

  if (!full) {
    fields.push(
      integerValue(stream.groups.size),
      bulkString('first-entry'),
      firstEntry ? entryToReply(firstEntry.id, firstEntry.fields) : nullBulk(),
      bulkString('last-entry'),
      lastEntry ? entryToReply(lastEntry.id, lastEntry.fields) : nullBulk(),
    )
    return fields
  }

  const fullCount = count ?? 10
  fields.push(
    RedisValue.array(
      Array.from(stream.groups.values(), group =>
        fullGroupInfoReply(stream, group, fullCount),
      ),
    ),
    bulkString('entries'),
    RedisValue.array(
      stream.entries
        .slice(0, fullCount)
        .map(entry => entryToReply(entry.id, entry.fields)),
    ),
  )
  return fields
}

function groupInfoReply(stream: RedisStreamData) {
  return (group: RedisStreamConsumerGroup): RedisValue =>
    kvMap([
      bulkString('name'),
      bulkString(group.name),
      bulkString('consumers'),
      integerValue(group.consumers.size),
      bulkString('pending'),
      integerValue(group.pending.size),
      bulkString('last-delivered-id'),
      streamIdValue(group.lastDeliveredId),
      bulkString('entries-read'),
      group.entriesRead === null ? nullBulk() : integerValue(group.entriesRead),
      bulkString('lag'),
      integerValue(streamLag(stream, group)),
    ])
}

function fullGroupInfoReply(
  stream: RedisStreamData,
  group: RedisStreamConsumerGroup,
  count: number,
): RedisValue {
  return kvMap([
    bulkString('name'),
    bulkString(group.name),
    bulkString('last-delivered-id'),
    streamIdValue(group.lastDeliveredId),
    bulkString('entries-read'),
    group.entriesRead === null ? nullBulk() : integerValue(group.entriesRead),
    bulkString('lag'),
    integerValue(streamLag(stream, group)),
    bulkString('pel-count'),
    integerValue(group.pending.size),
    bulkString('pending'),
    RedisValue.array(
      pendingEntriesSorted(group)
        .slice(0, count)
        .map(pending =>
          RedisValue.array([
            streamIdValue(pending.id),
            bulkString(
              group.consumers.get(pending.consumerId)?.name ??
                Buffer.from(pending.consumerId, 'hex'),
            ),
            integerValue(Math.max(0, Date.now() - pending.deliveredAt)),
            integerValue(pending.deliveryCount),
          ]),
        ),
    ),
    bulkString('consumers'),
    RedisValue.array(
      Array.from(group.consumers.entries()).map(([consumerId, consumer]) =>
        consumerInfoReply(group, consumerId, consumer, Date.now()),
      ),
    ),
  ])
}

function consumerInfoReply(
  group: RedisStreamConsumerGroup,
  consumerId: string,
  consumer: RedisStreamConsumer,
  now: number,
): RedisValue {
  const idle = Math.max(0, now - consumer.seenAt)
  const inactive =
    consumer.activeAt === null ? idle : Math.max(0, now - consumer.activeAt)
  return kvMap([
    bulkString('name'),
    bulkString(consumer.name),
    bulkString('pending'),
    integerValue(consumerPendingCount(group, consumerId)),
    bulkString('idle'),
    integerValue(idle),
    bulkString('inactive'),
    integerValue(inactive),
  ])
}
