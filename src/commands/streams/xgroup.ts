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
import {
  RedisCommandError,
  WrongNumberOfArgumentsError,
  errors,
} from '../../core/redis-error'
import type { RedisResult } from '../../core/redis-result'
import type { RedisDatabase } from '../../state/database'
import type { StreamId } from '../../state/data-types'
import type { CompatibilityProfile } from '../../core/compatibility'
import {
  helpReply,
  integer,
  ok,
  subcommandSyntaxError,
  unknownSubcommandError,
} from '../helpers'
import { requireStreamGroup, streamGroup } from './groups'
import {
  bufferId,
  cloneStreamId,
  MAX_ID,
  MIN_ID,
  parseExactId,
  parseLongLong,
} from './ids'

// CREATE / SETID: the id and the options after it stay raw. Real Redis reads
// the options before the key but the id only after the key and group checks,
// so both are parsed in execute (#507).
type XgroupIdArgs = {
  rawName: Buffer
  key: Buffer
  group: Buffer
  id: Buffer
  options: readonly Buffer[]
}

type XgroupArgs =
  | ({ subcommand: 'create' } & XgroupIdArgs)
  | ({ subcommand: 'setid' } & XgroupIdArgs)
  | { subcommand: 'destroy'; key: Buffer; group: Buffer }
  | {
      subcommand: 'createconsumer'
      key: Buffer
      group: Buffer
      consumer: Buffer
    }
  | { subcommand: 'delconsumer'; key: Buffer; group: Buffer; consumer: Buffer }
  | { subcommand: 'help'; key: undefined }
  | {
      subcommand: 'unknown'
      name: Buffer
      key: Buffer | undefined
      group: Buffer | undefined
    }

const XGROUP_HELP_HEAD = [
  'XGROUP <subcommand> [<arg> [value] [opt] ...]. Subcommands are:',
  'CREATE <key> <groupname> <id|$> [option]',
  '    Create a new consumer group. Options are:',
  '    * MKSTREAM',
  '      Create the empty stream if it does not exist.',
]

const XGROUP_HELP_CONSUMERS = [
  'CREATECONSUMER <key> <groupname> <consumer>',
  '    Create a new consumer in the specified group.',
  'DELCONSUMER <key> <groupname> <consumer>',
  '    Remove the specified consumer.',
]

// Captured from real 7.0.15 / 8.0.6 and 6.2.24 (whose DESTROY line really
// runs its description onto the same line).
function xgroupHelpLines(profile: CompatibilityProfile): string[] {
  if (!profile.has('xgroup.help-entriesread')) {
    return [
      ...XGROUP_HELP_HEAD,
      ...XGROUP_HELP_CONSUMERS,
      'DESTROY <key> <groupname>    Remove the specified group.',
      'SETID <key> <groupname> <id|$>',
      '    Set the current group ID.',
    ]
  }

  return [
    ...XGROUP_HELP_HEAD,
    '    * ENTRIESREAD entries_read',
    "      Set the group's entries_read counter (internal use).",
    ...XGROUP_HELP_CONSUMERS,
    'DESTROY <key> <groupname>',
    '    Remove the specified group.',
    'SETID <key> <groupname> <id|$> [ENTRIESREAD entries_read]',
    '    Set the current group ID and entries_read counter.',
  ]
}

function createXgroupSchema() {
  return t.custom<XgroupArgs>(
    { min: 1 },
    (input: readonly Buffer[], index: number, ctx: ParseContext) => {
      const rawSubcommand = input[index]
      if (!rawSubcommand) {
        throw new WrongNumberOfArgumentsError(ctx.commandName)
      }
      const subcommand = asciiUpperCase(rawSubcommand.toString())
      // A parser only knows the container (`ctx.commandName`), so arity errors
      // for a dispatched subcommand spell out `xgroup|<sub>` themselves, as
      // real Redis 7.0+ does (#438). An option list the subcommand cannot use
      // is real Redis' `addReplySubcommandSyntaxError`, not an arity error.

      if (subcommand === 'CREATE' || subcommand === 'SETID') {
        const name = subcommand === 'CREATE' ? 'create' : 'setid'
        const key = input[index + 1]
        const group = input[index + 2]
        const id = input[index + 3]
        if (!key || !group || !id) {
          throw new WrongNumberOfArgumentsError(`xgroup|${name}`)
        }

        return {
          value: {
            subcommand: name,
            rawName: rawSubcommand,
            key,
            group,
            id,
            options: input.slice(index + 4),
          },
          nextIndex: input.length,
        }
      }

      if (subcommand === 'DESTROY') {
        const key = input[index + 1]
        const group = input[index + 2]
        if (!key || !group || input.length !== index + 3) {
          throw new WrongNumberOfArgumentsError('xgroup|destroy')
        }
        return {
          value: { subcommand: 'destroy', key, group },
          nextIndex: input.length,
        }
      }

      if (subcommand === 'CREATECONSUMER' || subcommand === 'DELCONSUMER') {
        const name =
          subcommand === 'CREATECONSUMER' ? 'createconsumer' : 'delconsumer'
        const key = input[index + 1]
        const group = input[index + 2]
        const consumer = input[index + 3]
        if (!key || !group || !consumer || input.length !== index + 4) {
          throw new WrongNumberOfArgumentsError(`xgroup|${name}`)
        }
        return {
          value: {
            subcommand: name,
            key,
            group,
            consumer,
          },
          nextIndex: input.length,
        }
      }

      // 7.0+ resolves `xgroup|help` in the command table: no key, arity 2.
      // 6.2 answers a bare HELP and treats HELP with arguments exactly like
      // an unknown subcommand, below.
      const lookup = ctx.profile.has('error.unknown-subcommand-dispatch-timing')
      if (subcommand === 'HELP' && input.length === index + 1) {
        return {
          value: { subcommand: 'help', key: undefined },
          nextIndex: input.length,
        }
      }
      if (subcommand === 'HELP' && lookup) {
        throw new WrongNumberOfArgumentsError('xgroup|help')
      }

      // Not rejected here: on 7.0+ profiles command lookup has already turned
      // an unknown name away (`CommandExecutor.plan()`), so only 6.2 gets
      // here — and it rejects the name when XGROUP runs, after the key (#436).
      return {
        value: {
          subcommand: 'unknown',
          name: rawSubcommand,
          key: lookup ? undefined : input[index + 1],
          group: lookup ? undefined : input[index + 2],
        },
        nextIndex: input.length,
      }
    },
  )
}

// The real subcommand entries (#518): lookup checks a call against their
// arity, and COMMAND INFO lists them.
const xgroupIntrospection: CommandIntrospection = streamContainerIntrospection({
  summaries: {
    before72: 'A container for consumer groups commands',
    from72: 'A container for consumer groups commands.',
  },
  subcommands: [
    streamSubcommandInfo('xgroup|help', 2, {
      summaries: {
        before72: 'Show helpful text about the different subcommands',
        from72: 'Returns helpful text about the different subcommands.',
      },
    }),
    streamSubcommandInfo('xgroup|destroy', 4, {
      complexity:
        "O(N) where N is the number of entries in the group's pending entries list (PEL).",
      summaries: {
        before72: 'Destroy a consumer group.',
        from72: 'Destroys a consumer group.',
      },
    }),
    streamSubcommandInfo('xgroup|setid', -5, {
      summaries: {
        before72:
          'Set a consumer group to an arbitrary last delivered ID value.',
        from72: 'Sets the last-delivered ID of a consumer group.',
      },
    }),
    streamSubcommandInfo('xgroup|createconsumer', 5, {
      since: '6.2.0',
      summaries: {
        before72: 'Create a consumer in a consumer group.',
        from72: 'Creates a consumer in a consumer group.',
      },
    }),
    streamSubcommandInfo('xgroup|delconsumer', 5, {
      summaries: {
        before72: 'Delete a consumer from a consumer group.',
        from72: 'Deletes a consumer from a consumer group.',
      },
    }),
    streamSubcommandInfo('xgroup|create', -5, {
      summaries: {
        before72: 'Create a consumer group.',
        from72: 'Creates a consumer group.',
      },
    }),
  ],
})

export const xgroupCommand = defineCommand({
  name: 'xgroup',
  schema: t.object({ args: createXgroupSchema() }),
  flags: ['write'],
  introspection: xgroupIntrospection,
  keys: args => (args.args.key ? [args.args.key] : []),
  execute: (args, ctx) => {
    const command = args.args
    // Real Redis publishes each subcommand under its own name
    // (xgroup-create, xgroup-setid, ...), never the parent `xgroup` (#381).
    const db = ctx.db.withOrigin(`xgroup-${command.subcommand}`)

    if (command.subcommand === 'help') {
      return helpReply(xgroupHelpLines(ctx.server.profile), ctx.server.profile)
    }

    if (command.subcommand === 'unknown') {
      // Real 6.2 looks the key up (getStream: WRONGTYPE) as soon as a group
      // name is present, before it looks at the subcommand.
      if (command.key && command.group && !ctx.db.getStream(command.key)) {
        throw errors.xgroupCreateMissingKey()
      }
      throw unknownSubcommandError('XGROUP', command.name, ctx.server.profile)
    }

    if (command.subcommand === 'create' || command.subcommand === 'setid') {
      return createOrSetId(command, db, ctx.server.profile)
    }

    // Every other subcommand needs the stream, and all but DESTROY the group.
    const stream = db.getStream(command.key)
    if (!stream) throw errors.xgroupCreateMissingKey()
    const exists = streamGroup(stream, command.group) !== null

    if (command.subcommand === 'destroy') {
      if (!exists) return integer(0)
      db.updateStream(command.key, writable =>
        writable.deleteGroup(bufferId(command.group)),
      )
      // Like real Redis: wake XREADGROUP clients blocked on the key, so those
      // reading the destroyed group reply NOGROUP. Not a modification — WATCH
      // stays clean.
      db.signalKeyReady(command.key)
      return integer(1)
    }

    if (!exists) throw errors.xgroupNoSuchGroup(command.key, command.group)

    if (command.subcommand === 'createconsumer') {
      const created = db.updateStream(command.key, writable => {
        const group = requireStreamGroup(
          writable.value,
          command.key,
          command.group,
        )
        const consumerId = bufferId(command.consumer)
        return writable.addConsumer(group, consumerId, {
          name: Buffer.from(command.consumer),
          seenAt: Date.now(),
          activeAt: null,
        })
      })
      return integer(created ? 1 : 0)
    }

    const deleted = db.updateStream(command.key, writable => {
      const group = requireStreamGroup(
        writable.value,
        command.key,
        command.group,
      )
      const consumerId = bufferId(command.consumer)
      return writable.deleteConsumer(group, consumerId)
    })
    return integer(deleted)
  },
})

type XgroupIdCommand = Extract<XgroupArgs, { subcommand: 'create' | 'setid' }>

/**
 * XGROUP CREATE / SETID, in real Redis' order: the options, then the key and
 * group checks, then the argument count, then the id (#507).
 */
function createOrSetId(
  command: XgroupIdCommand,
  db: RedisDatabase,
  profile: CompatibilityProfile,
): RedisResult {
  const create = command.subcommand === 'create'
  const { mkstream, entriesRead } = parseXgroupOptions(command, profile)

  const stream = db.getStream(command.key)
  if (!mkstream) {
    if (!stream) throw errors.xgroupCreateMissingKey()
    if (!create && !streamGroup(stream, command.group)) {
      throw errors.xgroupNoSuchGroup(command.key, command.group)
    }
  }

  // Past its options loop real Redis still checks the argument count, so an
  // option list it accepted can still be too long for the subcommand. `argc`
  // counts XGROUP itself: the id is argument 4.
  const argc = 5 + command.options.length
  const lag = profile.has('stream.consumer-group-lag')
  const argcOk = create
    ? argc <= (lag ? 8 : 6)
    : argc === 5 || (lag && argc === 7)
  if (!argcOk) {
    throw subcommandSyntaxError('XGROUP', command.rawName, profile)
  }

  const rawId = command.id.toString()
  if (create) {
    const lastDeliveredId =
      rawId === '$' ? (stream?.lastId ?? MIN_ID) : parseExactId(rawId)

    db.updateStream(command.key, writable => {
      const groupId = bufferId(command.group)
      if (writable.value.groups.has(groupId)) {
        throw errors.busyStreamGroup()
      }

      writable.addGroup(groupId, {
        name: Buffer.from(command.group),
        lastDeliveredId: cloneStreamId(lastDeliveredId),
        entriesRead,
        consumers: new Map(),
        pending: new Map(),
      })
    })
    return ok()
  }

  // SETID parses its id like a range bound: `-` and `+` are 0-0 and the
  // maximum id, where CREATE's strict parse rejects both.
  const id = parseSetIdTarget(rawId)
  db.updateStream(command.key, writable => {
    const group = requireStreamGroup(writable.value, command.key, command.group)
    writable.setGroupId(group, id ?? writable.lastId, entriesRead)
  })
  return ok()
}

// null is `$`: the stream's last id, read inside the update.
function parseSetIdTarget(rawId: string): StreamId | null {
  if (rawId === '$') return null
  if (rawId === '-') return MIN_ID
  if (rawId === '+') return MAX_ID
  return parseExactId(rawId)
}

/**
 * The options after the id. 7.0+ reads `MKSTREAM` (CREATE only) and
 * `ENTRIESREAD <n>` in any order and number before it looks at the key, and
 * rejects anything else with the subcommand syntax error. 6.2 knows only
 * CREATE's `MKSTREAM`, and only as the sole option: a single other CREATE
 * option is rejected before the key, and every other option list is left to
 * the argument count check after it.
 */
function parseXgroupOptions(
  command: XgroupIdCommand,
  profile: CompatibilityProfile,
): { mkstream: boolean; entriesRead: number | null } {
  const create = command.subcommand === 'create'
  const options = command.options

  if (!profile.has('stream.consumer-group-lag')) {
    if (!create || options.length !== 1) {
      return { mkstream: false, entriesRead: null }
    }
    if (asciiUpperCase(options[0].toString()) !== 'MKSTREAM') {
      throw subcommandSyntaxError('XGROUP', command.rawName, profile)
    }
    return { mkstream: true, entriesRead: null }
  }

  let mkstream = false
  let entriesRead: number | null = null
  for (let i = 0; i < options.length; i++) {
    const option = asciiUpperCase(options[i].toString())
    if (create && option === 'MKSTREAM') {
      mkstream = true
      continue
    }

    if (option === 'ENTRIESREAD' && i + 1 < options.length) {
      const value = parseLongLong(
        options[i + 1],
        'value is not an integer or out of range',
      )
      // -1 is the "unknown" sentinel, the same as not giving the option.
      if (value < -1n) {
        throw new RedisCommandError(
          'value for ENTRIESREAD must be positive or -1',
        )
      }
      // Counters are numbers here, so like XSETID ENTRIESADDED a value past
      // 2^53 - 1 is refused (real Redis takes any int64).
      if (value > BigInt(Number.MAX_SAFE_INTEGER)) {
        throw errors.expectedInteger()
      }
      entriesRead = value === -1n ? null : Number(value)
      i++
      continue
    }

    throw subcommandSyntaxError('XGROUP', command.rawName, profile)
  }
  return { mkstream, entriesRead }
}
