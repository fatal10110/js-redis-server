import { Command } from '../../../../../types'
import { DB } from '../../../db'
import {
  WrongNumberOfArguments,
  UnknownCommandSubCommand,
  InvalidCommandArgs,
} from '../../../../../core/errors'
import {
  defineCommand,
  CommandCategory,
  CommandMetadata,
  CommandFlags,
} from '../../metadata'
import {
  createSchemaCommand,
  SchemaCommandContext,
  SchemaCommandRegistration,
  t,
} from '../../../schema'

const metadata = defineCommand('command', {
  arity: -1, // COMMAND [subcommand] [args...]
  flags: {
    readonly: true,
    fast: true,
  },
  firstKey: -1,
  lastKey: -1,
  keyStep: 0,
  categories: [CommandCategory.SERVER],
})

export const CommandInfoDefinition: SchemaCommandRegistration<[Buffer[]]> = {
  metadata,
  schema: t.tuple([t.variadic(t.string())]),
  handler: async ([args], ctx) => {
    // COMMAND (no args) - list all commands
    if (args.length === 0) {
      return handleCommandList(ctx)
    }

    const subcommand = args[0].toString().toLowerCase()

    switch (subcommand) {
      case 'info':
        return handleCommandInfo(args.slice(1), ctx)
      case 'count':
        return handleCommandCount(ctx)
      case 'getkeys':
        return handleCommandGetKeys(args.slice(1), ctx)
      case 'docs':
        return handleCommandDocs(args.slice(1), ctx)
      case 'list':
        return handleCommandNames(ctx)
      case 'help':
        return handleCommandHelp()
      default:
        throw new UnknownCommandSubCommand(subcommand)
    }
  },
}

/**
 * COMMAND - Return all command metadata
 */
function handleCommandList(ctx: SchemaCommandContext): unknown[] {
  const commands = getAllCommands(ctx)
  return commands.map(cmd => formatCommand(cmd.metadata))
}

/**
 * COMMAND INFO <cmd> [<cmd> ...]
 */
function handleCommandInfo(args: Buffer[], ctx: SchemaCommandContext): unknown {
  if (args.length === 0) {
    throw new WrongNumberOfArguments('command|info')
  }

  const commands = ctx.commands || {}
  return args.map(arg => {
    const cmdName = arg.toString().toLowerCase()
    const cmd = commands[cmdName]
    return cmd ? formatCommand(cmd.metadata) : null
  })
}

/**
 * COMMAND COUNT
 */
function handleCommandCount(ctx: SchemaCommandContext): number {
  return getAllCommands(ctx).length
}

/**
 * COMMAND GETKEYS <command> <arg> [arg ...]
 */
function handleCommandGetKeys(
  args: Buffer[],
  ctx: SchemaCommandContext,
): Buffer[] {
  if (args.length < 1) {
    throw new WrongNumberOfArguments('command|getkeys')
  }

  const cmdName = args[0].toString().toLowerCase()
  const commands = ctx.commands || {}
  const cmd = commands[cmdName]

  if (!cmd) {
    throw new InvalidCommandArgs(cmdName)
  }

  const cmdArgs = args.slice(1)

  try {
    return cmd.getKeys(args[0], cmdArgs)
  } catch {
    throw new InvalidCommandArgs(cmdName)
  }
}

/**
 * COMMAND DOCS [<cmd> ...] (Redis 7.0+)
 * Returns documentation for commands - stub implementation
 */
function handleCommandDocs(
  args: Buffer[],
  ctx: SchemaCommandContext,
): unknown[] {
  if (args.length === 0) {
    // Return docs for all commands - stub with empty array
    return []
  }

  // Return docs for specific commands - stub with empty entries
  return args.map(() => [])
}

/**
 * COMMAND LIST (Redis 7.0+)
 * Returns list of command names
 */
function handleCommandNames(ctx: SchemaCommandContext): string[] {
  return getAllCommands(ctx).map(cmd => cmd.metadata.name)
}

/**
 * COMMAND HELP
 * Returns help text for COMMAND command
 */
function handleCommandHelp(): string[] {
  return [
    'COMMAND',
    '    Return details about all Redis commands.',
    'COMMAND COUNT',
    '    Return the total number of commands in this Redis server.',
    'COMMAND DOCS [<command-name> [command-name ...]]',
    '    Return documentary information about commands.',
    'COMMAND GETKEYS <full-command>',
    '    Return the keys from a full Redis command.',
    'COMMAND INFO [<command-name> [command-name ...]]',
    '    Return details about multiple Redis commands.',
    'COMMAND LIST [FILTERBY <filter> <value>]',
    '    Return a list of command names.',
    'COMMAND HELP',
    '    Prints this help.',
  ]
}

/**
 * Get all commands from context
 */
function getAllCommands(ctx: SchemaCommandContext): Command[] {
  const commands = ctx.commands || {}
  return Object.values(commands)
}

/**
 * Format command metadata to Redis response format
 * Returns: [name, arity, flags, firstKey, lastKey, keyStep, categories]
 *
 * Redis returns 1-indexed positions where 0 means no keys
 */
function formatCommand(meta: CommandMetadata): unknown[] {
  return [
    meta.name,
    meta.arity,
    formatFlags(meta.flags),
    meta.firstKey < 0 ? 0 : meta.firstKey + 1, // Convert to 1-indexed, 0 = no keys
    meta.lastKey < 0 ? meta.lastKey : meta.lastKey + 1, // Keep -1/-2 semantics, otherwise 1-index
    meta.keyStep,
    meta.categories,
  ]
}

/**
 * Convert CommandFlags to array of flag strings
 */
function formatFlags(flags: CommandFlags): string[] {
  const result: string[] = []

  if (flags.readonly) result.push('readonly')
  if (flags.write) result.push('write')
  if (flags.denyoom) result.push('denyoom')
  if (flags.admin) result.push('admin')
  if (flags.noscript) result.push('noscript')
  if (flags.random) result.push('random')
  if (flags.blocking) result.push('blocking')
  if (flags.fast) result.push('fast')
  if (flags.movablekeys) result.push('movablekeys')
  if (flags.transaction) result.push('transaction')

  return result
}

export default function (db: DB) {
  return createSchemaCommand(CommandInfoDefinition, { db })
}
