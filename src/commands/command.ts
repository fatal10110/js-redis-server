import {
  defineCommand,
  type CommandDefinition,
  type CommandDocumentation,
  type CommandDocumentationArgument,
  type CommandIntrospection,
  type CommandKeySpec,
} from '../core/command-definition'
import { t } from '../core/command-schema'
import {
  RedisCommandError,
  RedisSyntaxError,
  WrongNumberOfArgumentsError,
} from '../core/redis-error'
import type { RedisExecutionContext } from '../core/redis-context'
import { RedisResult } from '../core/redis-result'
import { RedisValue } from '../core/redis-value'
import type { FeatureId } from '../core/compatibility'
import { commandDocs, commandSubcommandInfo } from './introspection'

type CommandArgs = {
  subcommand?: string
  args: Buffer[]
}

type CommandInfo = {
  name: string
  arity: number
  flags: readonly string[]
  firstKey: number
  lastKey: number
  keyStep: number
  categories: readonly string[]
  tips: readonly string[]
  keySpecs: readonly CommandKeySpec[]
  subcommands: readonly CommandInfo[]
  docs?: CommandDocumentation
}

const SUBCOMMAND_FEATURES: Record<string, FeatureId> = {
  'command|docs': 'command.docs',
  'command|getkeysandflags': 'command.getkeysandflags',
  'client|no-evict': 'client.no-evict',
  'client|setinfo': 'client.setinfo',
  'pubsub|shardchannels': 'pubsub.sharded',
  'pubsub|shardnumsub': 'pubsub.sharded',
}

const commandIntrospection: CommandIntrospection = {
  arity: -1,
  flags: ['loading', 'stale'],
  firstKey: 0,
  lastKey: 0,
  keyStep: 0,
  categories: ['@slow', '@connection'],
  tips: ['nondeterministic_output_order'],
  keySpecs: [],
  subcommands: [
    commandSubcommandInfo('command|docs', -2, {
      tips: ['nondeterministic_output_order'],
    }),
    commandSubcommandInfo('command|getkeys', -4),
    commandSubcommandInfo('command|getkeysandflags', -4),
    commandSubcommandInfo('command|info', -2, {
      tips: ['nondeterministic_output_order'],
    }),
    commandSubcommandInfo('command|count', 2),
    commandSubcommandInfo('command|list', -2, {
      tips: ['nondeterministic_output_order'],
    }),
    commandSubcommandInfo('command|help', 2),
  ],
  docs: commandDocs('Return details about Redis commands', 'connection', [], {
    since: '2.8.13',
    complexity: 'O(N) where N is the total number of Redis commands',
  }),
}

export const commandCommand = defineCommand({
  name: 'command',
  schema: t.object({
    subcommand: t.optional(t.string()),
    args: t.variadic(t.bulk()),
  }),
  flags: ['readonly'],
  introspection: commandIntrospection,
  keys: () => [],
  execute: (args, ctx) => {
    if (args.subcommand === undefined) {
      expectArgCount('command', args.args, 0)
      return commandInfo(allRootCommandInfos(ctx))
    }

    switch (args.subcommand.toLowerCase()) {
      case 'count':
        return commandCount(args, ctx)
      case 'list':
        return commandList(args, ctx)
      case 'info':
        return commandInfoSubcommand(args, ctx)
      case 'docs':
        if (!ctx.server.profile.has('command.docs')) {
          throw commandSubcommandError(args.subcommand)
        }
        return commandDocsSubcommand(args, ctx)
      case 'getkeys':
        return commandGetKeys(args, ctx)
      case 'getkeysandflags':
        if (!ctx.server.profile.has('command.getkeysandflags')) {
          throw commandSubcommandError(args.subcommand)
        }
        return commandGetKeysAndFlags(args, ctx)
      case 'help':
        return commandHelp(args, ctx)
      default:
        throw new RedisCommandError(
          `unknown subcommand '${args.subcommand}'. Try COMMAND HELP.`,
        )
    }
  },
})

function commandSubcommandError(subcommand: string): RedisCommandError {
  return new RedisCommandError(
    `Unknown subcommand or wrong number of arguments for '${subcommand}'. Try COMMAND HELP.`,
  )
}

function commandCount(
  args: CommandArgs,
  ctx: RedisExecutionContext,
): RedisResult {
  expectArgCount('command|count', args.args, 0)
  return RedisResult.create(RedisValue.integer(allCommandInfos(ctx).length))
}

function commandList(
  args: CommandArgs,
  ctx: RedisExecutionContext,
): RedisResult {
  let names = allCommandInfos(ctx).map(info => info.name)

  if (args.args.length > 0) {
    if (
      args.args.length !== 3 ||
      !equalsAscii(args.args[0], 'filterby') ||
      (!equalsAscii(args.args[1], 'pattern') &&
        !equalsAscii(args.args[1], 'module'))
    ) {
      throw new RedisSyntaxError()
    }

    if (equalsAscii(args.args[1], 'module')) {
      names = []
    } else {
      const pattern = args.args[2].toString()
      names = names.filter(name => globMatches(pattern, name))
    }
  }

  return RedisResult.create(RedisValue.array(names.map(bulkString)))
}

function commandInfoSubcommand(
  args: CommandArgs,
  ctx: RedisExecutionContext,
): RedisResult {
  if (args.args.length === 0) {
    return commandInfo(allRootCommandInfos(ctx))
  }

  return commandInfo(
    args.args.map(name => findCommandInfo(ctx, name.toString())),
    true,
  )
}

function commandDocsSubcommand(
  args: CommandArgs,
  ctx: RedisExecutionContext,
): RedisResult {
  const infos =
    args.args.length === 0
      ? allCommandInfos(ctx)
      : args.args
          .map(name => findCommandInfo(ctx, name.toString()))
          .filter((info): info is CommandInfo => info !== null)

  const entries: [RedisValue, RedisValue][] = []
  for (const info of infos) {
    if (!info.docs) {
      continue
    }

    entries.push([bulkString(info.name), formatDocs(info.docs)])
  }

  return RedisResult.create(RedisValue.map(entries))
}

function commandGetKeys(
  args: CommandArgs,
  ctx: RedisExecutionContext,
): RedisResult {
  const { keys } = planCommandKeys(args, ctx, 'command|getkeys')
  return RedisResult.create(RedisValue.array(keys.map(key => bulk(key))))
}

function commandGetKeysAndFlags(
  args: CommandArgs,
  ctx: RedisExecutionContext,
): RedisResult {
  const { definition, keys } = planCommandKeys(
    args,
    ctx,
    'command|getkeysandflags',
  )
  const flags = keyAccessFlags(definition)
  return RedisResult.create(
    RedisValue.array(
      keys.map(key => RedisValue.array([bulk(key), RedisValue.array(flags)])),
    ),
  )
}

function commandHelp(
  args: CommandArgs,
  ctx: RedisExecutionContext,
): RedisResult {
  expectArgCount('command|help', args.args, 0)
  const lines = [
    'COMMAND <subcommand> [<arg> [value] [opt] ...]. Subcommands are:',
    '(no subcommand)',
    '    Return details about all Redis commands.',
    'COUNT',
    '    Return the total number of commands in this Redis server.',
    'LIST',
    '    Return a list of all commands in this Redis server.',
    'INFO [<command-name> ...]',
    '    Return details about multiple Redis commands.',
    '    If no command names are given, documentation details for all',
    '    commands are returned.',
  ]

  if (ctx.server.profile.has('command.docs')) {
    lines.push(
      'DOCS [<command-name> ...]',
      '    Return documentation details about multiple Redis commands.',
      '    If no command names are given, documentation details for all',
      '    commands are returned.',
    )
  }

  lines.push(
    'GETKEYS <full-command>',
    '    Return the keys from a full Redis command.',
  )

  if (ctx.server.profile.has('command.getkeysandflags')) {
    lines.push(
      'GETKEYSANDFLAGS <full-command>',
      '    Return the keys and the access flags from a full Redis command.',
    )
  }

  lines.push('HELP', '    Prints this help.')

  return RedisResult.create(RedisValue.array(lines.map(bulkString)))
}

function planCommandKeys(
  args: CommandArgs,
  ctx: RedisExecutionContext,
  commandName: string,
): {
  definition: CommandDefinition<unknown>
  keys: readonly Buffer[]
} {
  if (args.args.length < 1) {
    throw new WrongNumberOfArgumentsError(commandName)
  }

  const targetName = args.args[0].toString().toLowerCase()
  const definition = ctx.executor.getCommandDefinition(targetName)
  if (!definition) {
    throw new RedisCommandError('Invalid command specified')
  }

  let keys: readonly Buffer[]
  try {
    keys = ctx.executor.plan(targetName, args.args.slice(1)).keys
  } catch (err) {
    if (err instanceof RedisCommandError) {
      throw new WrongNumberOfArgumentsError(commandName)
    }

    throw err
  }

  if (keys.length === 0) {
    throw new RedisCommandError('The command has no key arguments')
  }

  return { definition, keys }
}

function allRootCommandInfos(ctx: RedisExecutionContext): CommandInfo[] {
  return ctx.executor
    .getCommandDefinitions()
    .map(definition => createCommandInfo(definition, ctx))
}

function allCommandInfos(ctx: RedisExecutionContext): CommandInfo[] {
  const infos: CommandInfo[] = []
  for (const info of allRootCommandInfos(ctx)) {
    infos.push(info, ...info.subcommands)
  }
  return infos
}

function findCommandInfo(
  ctx: RedisExecutionContext,
  name: string,
): CommandInfo | null {
  const target = name.toLowerCase()
  for (const info of allCommandInfos(ctx)) {
    if (info.name === target) {
      return info
    }
  }

  return null
}

function createCommandInfo(
  definition: CommandDefinition<unknown>,
  ctx: RedisExecutionContext,
): CommandInfo {
  const name = definition.name.toLowerCase()
  return createCommandInfoFromIntrospection(
    name,
    definition.flags,
    definition.introspection,
    ctx,
  )
}

function createCommandInfoFromIntrospection(
  name: string,
  fallbackFlags: readonly string[],
  introspection?: CommandIntrospection,
  ctx?: RedisExecutionContext,
): CommandInfo {
  const keySpecs = introspection?.keySpecs ?? []
  const firstKey = introspection?.firstKey ?? firstKeyFromSpecs(keySpecs)
  const lastKey = introspection?.lastKey ?? lastKeyFromSpecs(firstKey, keySpecs)
  const keyStep = introspection?.keyStep ?? keyStepFromSpecs(keySpecs)
  const flags = introspection?.flags ?? fallbackFlags

  return {
    name,
    arity: introspection?.arity ?? -1,
    flags,
    firstKey,
    lastKey,
    keyStep,
    categories: introspection?.categories ?? inferCategories(flags),
    tips: introspection?.tips ?? [],
    keySpecs,
    subcommands: (introspection?.subcommands ?? [])
      .filter(subcommand => subcommandAvailable(subcommand, ctx))
      .map(subcommand => {
        if (!subcommand.name) {
          throw new Error('Synthetic command introspection is missing a name')
        }
        return createCommandInfoFromIntrospection(
          subcommand.name,
          subcommand.flags ?? [],
          subcommand,
          ctx,
        )
      }),
    docs:
      introspection?.docs ??
      commandDocs(`${name.toUpperCase()} command`, 'generic'),
  }
}

function subcommandAvailable(
  introspection: CommandIntrospection,
  ctx?: RedisExecutionContext,
): boolean {
  if (!ctx || !introspection.name) {
    return true
  }

  const feature = SUBCOMMAND_FEATURES[introspection.name.toLowerCase()]
  return feature === undefined || ctx.server.profile.has(feature)
}

function commandInfo(
  infos: readonly (CommandInfo | null)[],
  preserveNulls = false,
): RedisResult {
  return RedisResult.create(
    RedisValue.array(
      infos
        .map(info => {
          if (!info) {
            return preserveNulls ? RedisValue.null() : null
          }

          return formatCommandInfo(info)
        })
        .filter((value): value is RedisValue => value !== null),
    ),
  )
}

function formatCommandInfo(info: CommandInfo): RedisValue {
  return RedisValue.array([
    bulkString(info.name),
    RedisValue.integer(info.arity),
    RedisValue.array(info.flags.map(bulkString)),
    RedisValue.integer(info.firstKey),
    RedisValue.integer(info.lastKey),
    RedisValue.integer(info.keyStep),
    RedisValue.array(info.categories.map(bulkString)),
    RedisValue.array(info.tips.map(bulkString)),
    RedisValue.array(info.keySpecs.map(formatKeySpec)),
    RedisValue.array(info.subcommands.map(formatCommandInfo)),
  ])
}

function formatKeySpec(spec: CommandKeySpec): RedisValue {
  const items: RedisValue[] = []
  if (spec.notes) {
    items.push(bulkString('notes'), bulkString(spec.notes))
  }

  items.push(
    bulkString('flags'),
    RedisValue.array(spec.flags.map(bulkString)),
    bulkString('begin_search'),
    RedisValue.array([
      bulkString('type'),
      bulkString('index'),
      bulkString('spec'),
      RedisValue.array([
        bulkString('index'),
        RedisValue.integer(spec.beginSearchIndex),
      ]),
    ]),
    bulkString('find_keys'),
    RedisValue.array([
      bulkString('type'),
      bulkString('range'),
      bulkString('spec'),
      RedisValue.array([
        bulkString('lastkey'),
        RedisValue.integer(spec.lastKey),
        bulkString('keystep'),
        RedisValue.integer(spec.keyStep),
        bulkString('limit'),
        RedisValue.integer(spec.limit ?? 0),
      ]),
    ]),
  )

  return RedisValue.array(items)
}

function formatDocs(docs: CommandDocumentation): RedisValue {
  const entries: [RedisValue, RedisValue][] = [
    [bulkString('summary'), bulkString(docs.summary)],
  ]

  if (docs.since) {
    entries.push([bulkString('since'), bulkString(docs.since)])
  }

  entries.push([bulkString('group'), bulkString(docs.group)])

  if (docs.complexity) {
    entries.push([bulkString('complexity'), bulkString(docs.complexity)])
  }

  if (docs.arguments) {
    entries.push([
      bulkString('arguments'),
      RedisValue.array(docs.arguments.map(formatDocsArgument)),
    ])
  }

  return RedisValue.map(entries)
}

function formatDocsArgument(arg: CommandDocumentationArgument): RedisValue {
  const entries: [RedisValue, RedisValue][] = [
    [bulkString('name'), bulkString(arg.name)],
    [bulkString('type'), bulkString(arg.type)],
  ]

  if (arg.keySpecIndex !== undefined) {
    entries.push([
      bulkString('key_spec_index'),
      RedisValue.integer(arg.keySpecIndex),
    ])
  }

  if (arg.token) {
    entries.push([bulkString('token'), bulkString(arg.token)])
  }

  if (arg.flags) {
    entries.push([
      bulkString('flags'),
      RedisValue.array(arg.flags.map(simpleString)),
    ])
  }

  return RedisValue.map(entries)
}

function keyAccessFlags(definition: CommandDefinition<unknown>): RedisValue[] {
  const flags =
    definition.introspection?.keySpecs?.[0]?.flags ??
    fallbackKeyAccessFlags(definition.flags)
  return flags.map(bulkString)
}

function fallbackKeyAccessFlags(flags: readonly string[]): readonly string[] {
  if (flags.includes('readonly')) {
    return ['RO', 'access']
  }

  if (flags.includes('write')) {
    return ['RW', 'access', 'update']
  }

  return ['RO', 'access']
}

function inferCategories(flags: readonly string[]): readonly string[] {
  if (flags.includes('write')) {
    return ['@write', '@slow']
  }

  if (flags.includes('readonly')) {
    return ['@read', flags.includes('fast') ? '@fast' : '@slow']
  }

  return ['@slow']
}

function firstKeyFromSpecs(specs: readonly CommandKeySpec[]): number {
  return specs[0]?.beginSearchIndex ?? 0
}

function lastKeyFromSpecs(
  firstKey: number,
  specs: readonly CommandKeySpec[],
): number {
  if (specs.length === 0) {
    return 0
  }

  const lastKey = specs[0].lastKey
  return lastKey === 0 ? firstKey : lastKey
}

function keyStepFromSpecs(specs: readonly CommandKeySpec[]): number {
  return specs[0]?.keyStep ?? 0
}

function expectArgCount(
  commandName: string,
  args: readonly Buffer[],
  count: number,
): void {
  if (args.length !== count) {
    throw new WrongNumberOfArgumentsError(commandName)
  }
}

function equalsAscii(value: Buffer, expected: string): boolean {
  return value.toString().toLowerCase() === expected
}

function bulkString(value: string): RedisValue {
  return bulk(Buffer.from(value))
}

function simpleString(value: string): RedisValue {
  return RedisValue.simpleString(value)
}

function bulk(value: Buffer | null): RedisValue {
  return RedisValue.bulkString(value)
}

function globMatches(pattern: string, value: string): boolean {
  let source = '^'
  for (let i = 0; i < pattern.length; i++) {
    const char = pattern[i]
    if (char === '*') {
      source += '.*'
    } else if (char === '?') {
      source += '.'
    } else {
      source += escapeRegExp(char)
    }
  }
  source += '$'
  return new RegExp(source, 'i').test(value)
}

function escapeRegExp(value: string): string {
  return value.replace(/[\\^$.*+?()[\]{}|]/g, '\\$&')
}
