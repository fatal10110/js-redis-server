import { asciiUpperCase } from '../../core/ascii-case'
import { defineCommand } from '../../core/command-definition'
import { t, type ParseContext } from '../../core/command-schema'
import { WrongNumberOfArgumentsError, errors } from '../../core/redis-error'
import type { StreamId } from '../../state/data-types'
import type { CompatibilityProfile } from '../../core/compatibility'
import {
  helpReply,
  integer,
  ok,
  subcommandSyntaxError,
  unknownSubcommandError,
} from '../helpers'
import { requireStreamGroup } from './groups'
import {
  bufferId,
  cloneStreamId,
  MIN_ID,
  parseExactId,
  parseNonNegativeInteger,
} from './ids'

type XgroupArgs =
  | {
      subcommand: 'create'
      key: Buffer
      group: Buffer
      id: StreamId | '$'
      mkstream: boolean
      entriesRead: number | null
    }
  | {
      subcommand: 'setid'
      key: Buffer
      group: Buffer
      id: StreamId | '$'
      entriesRead: number | null
    }
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
        const rawId = input[index + 3]?.toString()
        if (!key || !group || rawId === undefined) {
          throw new WrongNumberOfArgumentsError(`xgroup|${name}`)
        }

        let cursor = index + 4
        let mkstream = false
        let entriesRead: number | null = null
        while (cursor < input.length) {
          const option = input[cursor].toString().toUpperCase()
          if (subcommand === 'CREATE' && option === 'MKSTREAM') {
            mkstream = true
            cursor++
            continue
          }

          if (option === 'ENTRIESREAD') {
            const rawEntriesRead = input[cursor + 1]
            if (!rawEntriesRead) break
            entriesRead = parseNonNegativeInteger(rawEntriesRead)
            cursor += 2
            continue
          }

          break
        }

        if (cursor !== input.length) {
          throw subcommandSyntaxError('XGROUP', rawSubcommand, ctx.profile)
        }

        return {
          value: {
            subcommand: name,
            key,
            group,
            id: rawId === '$' ? '$' : parseExactId(rawId),
            mkstream,
            entriesRead,
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

export const xgroupCommand = defineCommand({
  name: 'xgroup',
  schema: t.object({ args: createXgroupSchema() }),
  flags: ['write'],
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

    if (command.subcommand === 'create') {
      const type = db.getType(command.key)
      if (type === null && !command.mkstream) {
        throw errors.xgroupCreateMissingKey()
      }

      const lastDeliveredId =
        command.id === '$'
          ? (db.getStream(command.key)?.lastId ?? MIN_ID)
          : command.id

      db.updateStream(command.key, stream => {
        const groupId = bufferId(command.group)
        if (stream.value.groups.has(groupId)) {
          throw errors.busyStreamGroup()
        }

        stream.addGroup(groupId, {
          name: Buffer.from(command.group),
          lastDeliveredId: cloneStreamId(lastDeliveredId),
          entriesRead: command.entriesRead,
          consumers: new Map(),
          pending: new Map(),
        })
      })
      return ok()
    }

    if (command.subcommand === 'setid') {
      requireStreamGroup(db.getStream(command.key), command.key, command.group)
      db.updateStream(command.key, stream => {
        const group = requireStreamGroup(
          stream.value,
          command.key,
          command.group,
        )
        const lastDeliveredId = command.id === '$' ? stream.lastId : command.id
        stream.setGroupId(group, lastDeliveredId, command.entriesRead)
      })
      return ok()
    }

    if (command.subcommand === 'destroy') {
      const stream = db.getStream(command.key)
      if (!stream) return integer(0)

      const removed = db.updateStream(command.key, writable => {
        return writable.deleteGroup(bufferId(command.group))
      })
      // Like real Redis: wake XREADGROUP clients blocked on the key, so those
      // reading the destroyed group reply NOGROUP. Not a modification — WATCH
      // stays clean.
      if (removed) db.signalKeyReady(command.key)
      return integer(removed ? 1 : 0)
    }

    if (command.subcommand === 'createconsumer') {
      requireStreamGroup(db.getStream(command.key), command.key, command.group)
      const created = db.updateStream(command.key, stream => {
        const group = requireStreamGroup(
          stream.value,
          command.key,
          command.group,
        )
        const consumerId = bufferId(command.consumer)
        return stream.addConsumer(group, consumerId, {
          name: Buffer.from(command.consumer),
          seenAt: Date.now(),
          activeAt: null,
        })
      })
      return integer(created ? 1 : 0)
    }

    requireStreamGroup(db.getStream(command.key), command.key, command.group)
    const deleted = db.updateStream(command.key, stream => {
      const group = requireStreamGroup(stream.value, command.key, command.group)
      const consumerId = bufferId(command.consumer)
      return stream.deleteConsumer(group, consumerId)
    })
    return integer(deleted)
  },
})
