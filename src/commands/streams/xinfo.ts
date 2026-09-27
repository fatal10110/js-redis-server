import { asciiUpperCase } from '../../core/ascii-case'
import {
  defineCommand,
  type CommandIntrospection,
} from '../../core/command-definition'
import {
  streamContainerIntrospection,
  streamSubcommandInfo,
} from '../introspection'
import { t, type ParseContext } from '../../core/command-schema'
import { WrongNumberOfArgumentsError, errors } from '../../core/redis-error'
import { RedisResult } from '../../core/redis-result'
import { RedisValue } from '../../core/redis-value'
import type {
  RedisStreamConsumer,
  RedisStreamConsumerGroup,
  RedisStreamData,
} from '../../state/data-types'
import {
  array,
  helpReply,
  subcommandSyntaxError,
  unknownSubcommandError,
} from '../helpers'
import {
  consumerPendingCount,
  pendingEntriesSorted,
  streamGroup,
  streamLag,
} from './groups'
import type { CompatibilityProfile } from '../../core/compatibility'
import { parseLongLong } from './ids'
import {
  bulkString,
  entryToReply,
  integerValue,
  nullBulk,
  streamIdValue,
} from './replies'

type XinfoArgs =
  | {
      subcommand: 'stream'
      rawName: Buffer
      key: Buffer
      options: readonly Buffer[]
    }
  | { subcommand: 'groups'; key: Buffer }
  | { subcommand: 'consumers'; key: Buffer; group: Buffer }
  | { subcommand: 'help'; key: Buffer | undefined }
  | { subcommand: 'unknown'; name: Buffer; key: Buffer | undefined }

const XINFO_HELP = [
  'XINFO <subcommand> [<arg> [value] [opt] ...]. Subcommands are:',
  'CONSUMERS <key> <groupname>',
  '    Show consumers of <groupname>.',
  'GROUPS <key>',
  '    Show the stream consumer groups.',
  'STREAM <key> [FULL [COUNT <count>]',
  '    Show information about the stream.',
]

function isToken(arg: Buffer, token: string): boolean {
  return asciiUpperCase(arg.toString()) === token
}

type StreamInfoOptions = { full: boolean; count: number }

/**
 * `[FULL [COUNT <count>]]`: nothing, `FULL`, or `FULL COUNT <count>`, read
 * after the key lookup as real Redis does (so a missing key wins over a bad
 * option). A negative COUNT means the default, 10; 0 means no limit.
 */
function parseStreamOptions(
  rawName: Buffer,
  options: readonly Buffer[],
  profile: CompatibilityProfile,
): StreamInfoOptions {
  if (options.length === 0) return { full: false, count: 10 }
  if (
    (options.length !== 1 && options.length !== 3) ||
    !isToken(options[0], 'FULL') ||
    (options.length === 3 && !isToken(options[1], 'COUNT'))
  ) {
    throw subcommandSyntaxError('XINFO', rawName, profile)
  }
  if (options.length === 1) return { full: true, count: 10 }

  const count = parseLongLong(
    options[2],
    'value is not an integer or out of range',
  )
  if (count < 0n) return { full: true, count: 10 }
  // Anything past the stream's size is the same as no limit.
  return {
    full: true,
    count: count > BigInt(Number.MAX_SAFE_INTEGER) ? 0 : Number(count),
  }
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

        // The options are read only after the key (#507): see
        // parseStreamOptions.
        return {
          value: {
            subcommand: 'stream',
            rawName: rawSubcommand,
            key,
            options: input.slice(index + 2),
          },
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

      // 7.0+ resolves `xinfo|help` in the command table: no key, arity 2.
      // 6.2 answers HELP before it counts arguments or looks a key up, but
      // its key spec still names the third argument, so it keeps routing on
      // it (and COMMAND GETKEYS reports it).
      const lookup = ctx.profile.has('error.unknown-subcommand-dispatch-timing')
      if (subcommand === 'HELP') {
        if (lookup && input.length !== index + 1) {
          throw new WrongNumberOfArgumentsError('xinfo|help')
        }
        return {
          value: {
            subcommand: 'help',
            key: lookup ? undefined : input[index + 1],
          },
          nextIndex: input.length,
        }
      }

      // Not rejected here: on 7.0+ profiles command lookup has already turned
      // an unknown name away (`CommandExecutor.plan()`), so only 6.2 gets
      // here — and it rejects the name when XINFO runs, after the key (#436).
      return {
        value: {
          subcommand: 'unknown',
          name: rawSubcommand,
          key: lookup ? undefined : input[index + 1],
        },
        nextIndex: input.length,
      }
    },
  )
}

// The real subcommand entries (#518): lookup checks a call against their
// arity, and COMMAND INFO lists them.
const xinfoIntrospection: CommandIntrospection = streamContainerIntrospection({
  summaries: {
    before72: 'A container for stream introspection commands',
    from72: 'A container for stream introspection commands.',
  },
  legacy: { flags: ['readonly', 'random'], keyFlags: ['RO', 'access'] },
  subcommands: [
    streamSubcommandInfo('xinfo|help', 2, {
      flags: ['loading', 'stale'],
      summaries: {
        before72: 'Show helpful text about the different subcommands',
        from72: 'Returns helpful text about the different subcommands.',
      },
    }),
    streamSubcommandInfo('xinfo|stream', -3, {
      flags: ['readonly'],
      keyFlags: ['RO', 'access'],
      summaries: {
        before72: 'Get information about a stream',
        from72: 'Returns information about a stream.',
      },
    }),
    streamSubcommandInfo('xinfo|groups', 3, {
      flags: ['readonly'],
      keyFlags: ['RO', 'access'],
      summaries: {
        before72: 'List the consumer groups of a stream',
        from72: 'Returns a list of the consumer groups of a stream.',
      },
    }),
    streamSubcommandInfo('xinfo|consumers', 4, {
      flags: ['readonly'],
      keyFlags: ['RO', 'access'],
      tips: ['nondeterministic_output'],
      summaries: {
        before72: 'List the consumers in a consumer group',
        from72: 'Returns a list of the consumers in a consumer group.',
      },
    }),
  ],
})

export const xinfoCommand = defineCommand({
  name: 'xinfo',
  schema: t.object({ args: createXinfoSchema() }),
  flags: ['readonly'],
  introspection: xinfoIntrospection,
  keys: args => (args.args.key ? [args.args.key] : []),
  execute: (args, ctx) => {
    const command = args.args
    const profile = ctx.server.profile
    if (command.subcommand === 'help') {
      return helpReply(XINFO_HELP, profile)
    }

    if (command.subcommand === 'unknown') {
      // Real 6.2 looks the key up (getStream: WRONGTYPE) before it looks at
      // the subcommand.
      if (command.key && !ctx.db.getStream(command.key)) {
        throw errors.noSuchKey()
      }
      throw unknownSubcommandError('XINFO', command.name, profile)
    }

    const stream = ctx.db.getStream(command.key)
    if (!stream) throw errors.noSuchKey()

    if (command.subcommand === 'stream') {
      const options = parseStreamOptions(
        command.rawName,
        command.options,
        profile,
      )
      // XINFO replies are field/value maps: a flat array on RESP2, a `%` map on
      // RESP3 (matching real Redis). first/last-entry and PEL rows stay arrays.
      return RedisResult.create(
        kvMap(streamInfoReply(stream, options, profile)),
      )
    }

    if (command.subcommand === 'groups') {
      return array(
        Array.from(stream.groups.values(), group =>
          groupInfoReply(stream, group, profile),
        ),
      )
    }

    // XINFO CONSUMERS answers a missing group in XGROUP's wording.
    const group = streamGroup(stream, command.group)
    if (!group) throw errors.xgroupNoSuchGroup(command.key, command.group)
    const now = Date.now()
    return array(
      Array.from(group.consumers.entries()).map(([consumerId, consumer]) =>
        consumerInfoReply(group, consumerId, consumer, now, profile),
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

// `count` 0 lists everything.
function limited<T>(items: T[], count: number): T[] {
  return count === 0 ? items : items.slice(0, count)
}

// A group's `entries-read` / `lag` pair (Redis 7.0+).
function groupLagFields(
  stream: RedisStreamData,
  group: RedisStreamConsumerGroup,
): RedisValue[] {
  return [
    bulkString('entries-read'),
    group.entriesRead === null ? nullBulk() : integerValue(group.entriesRead),
    bulkString('lag'),
    integerValue(streamLag(stream, group)),
  ]
}

function streamInfoReply(
  stream: RedisStreamData,
  options: StreamInfoOptions,
  profile: CompatibilityProfile,
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
  ]
  if (profile.has('stream.consumer-group-lag')) {
    fields.push(
      bulkString('max-deleted-entry-id'),
      streamIdValue(stream.maxDeletedEntryId),
      bulkString('entries-added'),
      integerValue(stream.entriesAdded),
      bulkString('recorded-first-entry-id'),
      firstEntry ? streamIdValue(firstEntry.id) : bulkString('0-0'),
    )
  }

  if (!options.full) {
    fields.push(
      bulkString('groups'),
      integerValue(stream.groups.size),
      bulkString('first-entry'),
      firstEntry ? entryToReply(firstEntry.id, firstEntry.fields) : nullBulk(),
      bulkString('last-entry'),
      lastEntry ? entryToReply(lastEntry.id, lastEntry.fields) : nullBulk(),
    )
    return fields
  }

  // FULL lists the entries before the groups.
  fields.push(
    bulkString('entries'),
    RedisValue.array(
      limited(stream.entries, options.count).map(entry =>
        entryToReply(entry.id, entry.fields),
      ),
    ),
    bulkString('groups'),
    RedisValue.array(
      Array.from(stream.groups.values(), group =>
        fullGroupInfoReply(stream, group, options.count, profile),
      ),
    ),
  )
  return fields
}

function groupInfoReply(
  stream: RedisStreamData,
  group: RedisStreamConsumerGroup,
  profile: CompatibilityProfile,
): RedisValue {
  return kvMap([
    bulkString('name'),
    bulkString(group.name),
    bulkString('consumers'),
    integerValue(group.consumers.size),
    bulkString('pending'),
    integerValue(group.pending.size),
    bulkString('last-delivered-id'),
    streamIdValue(group.lastDeliveredId),
    ...(profile.has('stream.consumer-group-lag')
      ? groupLagFields(stream, group)
      : []),
  ])
}

function fullGroupInfoReply(
  stream: RedisStreamData,
  group: RedisStreamConsumerGroup,
  count: number,
  profile: CompatibilityProfile,
): RedisValue {
  return kvMap([
    bulkString('name'),
    bulkString(group.name),
    bulkString('last-delivered-id'),
    streamIdValue(group.lastDeliveredId),
    ...(profile.has('stream.consumer-group-lag')
      ? groupLagFields(stream, group)
      : []),
    bulkString('pel-count'),
    integerValue(group.pending.size),
    bulkString('pending'),
    RedisValue.array(
      limited(pendingEntriesSorted(group), count).map(pending =>
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
        consumerInfoReply(group, consumerId, consumer, Date.now(), profile),
      ),
    ),
  ])
}

function consumerInfoReply(
  group: RedisStreamConsumerGroup,
  consumerId: string,
  consumer: RedisStreamConsumer,
  now: number,
  profile: CompatibilityProfile,
): RedisValue {
  const fields: RedisValue[] = [
    bulkString('name'),
    bulkString(consumer.name),
    bulkString('pending'),
    integerValue(consumerPendingCount(group, consumerId)),
    bulkString('idle'),
    integerValue(Math.max(0, now - consumer.seenAt)),
  ]
  // -1 until the consumer is first delivered or claims an entry (real 7.2+,
  // which added the field).
  if (profile.has('stream.consumer-active-time')) {
    fields.push(
      bulkString('inactive'),
      integerValue(
        consumer.activeAt === null ? -1 : Math.max(0, now - consumer.activeAt),
      ),
    )
  }
  return kvMap(fields)
}
