import { defineCommand } from '../../core/command-definition'
import { t, type ParseContext } from '../../core/command-schema'
import {
  RedisCommandError,
  RedisSyntaxError,
  WrongNumberOfArgumentsError,
} from '../../core/redis-error'
import type { StreamId } from '../../state/data-types'
import { integer, ok } from '../helpers'
import { BusyStreamGroupError, requireStreamGroup } from './groups'
import {
  bufferId,
  cloneStreamId,
  MIN_ID,
  parseExactId,
  parseNonNegativeInteger,
} from './ids'

class XgroupCreateMissingKeyError extends RedisCommandError {
  constructor() {
    super(
      'The XGROUP subcommand requires the key to exist. Note that for CREATE you may want to use the MKSTREAM option to create an empty stream automatically.',
    )
  }
}

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

function createXgroupSchema() {
  return t.custom<XgroupArgs>(
    (input: readonly Buffer[], index: number, ctx: ParseContext) => {
      const subcommand = input[index]?.toString().toUpperCase()
      if (!subcommand) throw new WrongNumberOfArgumentsError(ctx.commandName)

      if (subcommand === 'CREATE' || subcommand === 'SETID') {
        const key = input[index + 1]
        const group = input[index + 2]
        const rawId = input[index + 3]?.toString()
        if (!key || !group || rawId === undefined) {
          throw new WrongNumberOfArgumentsError(ctx.commandName)
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
            if (!rawEntriesRead) {
              throw new WrongNumberOfArgumentsError(ctx.commandName)
            }
            entriesRead = parseNonNegativeInteger(rawEntriesRead)
            cursor += 2
            continue
          }

          throw new RedisSyntaxError()
        }

        return {
          value: {
            subcommand: subcommand === 'CREATE' ? 'create' : 'setid',
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
          throw new WrongNumberOfArgumentsError(ctx.commandName)
        }
        return {
          value: { subcommand: 'destroy', key, group },
          nextIndex: input.length,
        }
      }

      if (subcommand === 'CREATECONSUMER' || subcommand === 'DELCONSUMER') {
        const key = input[index + 1]
        const group = input[index + 2]
        const consumer = input[index + 3]
        if (!key || !group || !consumer || input.length !== index + 4) {
          throw new WrongNumberOfArgumentsError(ctx.commandName)
        }
        return {
          value: {
            subcommand:
              subcommand === 'CREATECONSUMER'
                ? 'createconsumer'
                : 'delconsumer',
            key,
            group,
            consumer,
          },
          nextIndex: input.length,
        }
      }

      throw new RedisCommandError(
        `Unknown subcommand or wrong number of arguments for '${subcommand}'. Try XGROUP HELP.`,
      )
    },
  )
}

export const xgroupCommand = defineCommand({
  name: 'xgroup',
  schema: t.object({ args: createXgroupSchema() }),
  flags: ['write'],
  keys: args => [args.args.key],
  execute: (args, ctx) => {
    const command = args.args

    if (command.subcommand === 'create') {
      const type = ctx.db.getType(command.key)
      if (type === null && !command.mkstream) {
        throw new XgroupCreateMissingKeyError()
      }

      const lastDeliveredId =
        command.id === '$'
          ? (ctx.db.getStream(command.key)?.lastId ?? MIN_ID)
          : command.id

      ctx.db.updateStream(command.key, stream => {
        const groupId = bufferId(command.group)
        if (stream.groups.has(groupId)) {
          throw new BusyStreamGroupError()
        }

        stream.groups.set(groupId, {
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
      requireStreamGroup(
        ctx.db.getStream(command.key),
        command.key,
        command.group,
      )
      ctx.db.updateStream(command.key, stream => {
        const group = requireStreamGroup(stream, command.key, command.group)
        group.lastDeliveredId =
          command.id === '$'
            ? cloneStreamId(stream.lastId)
            : cloneStreamId(command.id)
        group.entriesRead = command.entriesRead
      })
      return ok()
    }

    if (command.subcommand === 'destroy') {
      const stream = ctx.db.getStream(command.key)
      if (!stream) return integer(0)

      const removed = ctx.db.updateStream(command.key, writable =>
        writable.groups.delete(bufferId(command.group)),
      )
      return integer(removed ? 1 : 0)
    }

    if (command.subcommand === 'createconsumer') {
      requireStreamGroup(
        ctx.db.getStream(command.key),
        command.key,
        command.group,
      )
      const created = ctx.db.updateStream(command.key, stream => {
        const group = requireStreamGroup(stream, command.key, command.group)
        const consumerId = bufferId(command.consumer)
        if (group.consumers.has(consumerId)) return false

        group.consumers.set(consumerId, {
          name: Buffer.from(command.consumer),
          seenAt: Date.now(),
          activeAt: null,
        })
        return true
      })
      return integer(created ? 1 : 0)
    }

    requireStreamGroup(
      ctx.db.getStream(command.key),
      command.key,
      command.group,
    )
    const deleted = ctx.db.updateStream(command.key, stream => {
      const group = requireStreamGroup(stream, command.key, command.group)
      const consumerId = bufferId(command.consumer)
      if (!group.consumers.delete(consumerId)) return 0

      let removedPending = 0
      for (const [pendingId, pending] of Array.from(group.pending)) {
        if (pending.consumerId !== consumerId) continue
        group.pending.delete(pendingId)
        removedPending++
      }
      return removedPending
    })
    return integer(deleted)
  },
})
