import { asciiLowerCase, equalsAscii } from '../core/ascii-case'
import {
  defineCommand,
  type CommandDefinition,
  type CommandDocumentation,
  type CommandDocumentationArgument,
  type CommandIntrospection,
  type CommandKeySpec,
  introspectionFor,
} from '../core/command-definition'
import {
  commandTableArity,
  failsTableArity,
  lookupSubcommandEntry,
} from '../core/command-arity'
import { t, type CommandSchema } from '../core/command-schema'
import {
  keysFromKeySpecs,
  legacyKeyRange,
  legacyRangeKeys,
  type KeyWithFlags,
  type LegacyKeyRange,
} from '../core/key-specs'
import {
  RedisCommandError,
  WrongNumberOfArgumentsError,
  errors,
} from '../core/redis-error'
import type { RedisExecutionContext } from '../core/redis-context'
import { RedisResult } from '../core/redis-result'
import { RedisValue } from '../core/redis-value'
import type { CompatibilityProfile, FeatureId } from '../core/compatibility'
import { containerSubcommandExists } from '../core/compatibility/subcommand-gates'
import {
  helpReply,
  subcommandSyntaxError,
  unknownSubcommandError,
} from './helpers'
import { commandDocs, commandSubcommandInfo } from './introspection'

type CommandArgs = {
  subcommand?: Buffer
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
  'acl|dryrun': 'acl.dryrun',
  'command|docs': 'command.docs',
  'command|getkeysandflags': 'command.getkeysandflags',
  'client|no-evict': 'client.no-evict',
  'client|setinfo': 'client.setinfo',
  'pubsub|shardchannels': 'pubsub.sharded',
  'pubsub|shardnumsub': 'pubsub.sharded',
}

// Tokens GETKEYS / GETKEYSANDFLAGS need after the subcommand. Only 7.0 wants
// the target command plus at least one argument (its new per-subcommand arity
// was -4); 6.2 checks just for a target and 7.2+ relaxed the arity to -3.
function minGetKeysArgs(profile: CompatibilityProfile): number {
  const redis70 =
    profile.has('command.getkeysandflags') &&
    !profile.has('command.getkeys-single-arg')
  return redis70 ? 2 : 1
}

function getKeysArity(profile: CompatibilityProfile): number {
  return -(minGetKeysArgs(profile) + 2)
}

const commandIntrospection: CommandIntrospection = {
  subcommands: [
    commandSubcommandInfo('command|docs', -2),
    commandSubcommandInfo('command|getkeys', getKeysArity),
    commandSubcommandInfo('command|getkeysandflags', getKeysArity),
    commandSubcommandInfo('command|info', -2),
    commandSubcommandInfo('command|count', 2),
    commandSubcommandInfo('command|list', -2),
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
    // Raw bytes, not `t.string()`: the unknown-subcommand reply echoes the
    // name the client sent, and a UTF-8 decode here would lose its bytes.
    subcommand: t.optional(t.bulk()),
    args: t.variadic(t.bulk()),
  }),
  flags: ['readonly'],
  introspection: commandIntrospection,
  keys: () => [],
  execute: (args, ctx) => {
    if (args.subcommand === undefined) {
      expectArgCount('command', args.args, 0)
      return commandInfo(ctx, allRootCommandInfos(ctx))
    }

    if (!ctx.server.profile.has('command.list')) {
      return legacyCommandSubcommand(args.subcommand, args, ctx)
    }

    switch (asciiLowerCase(args.subcommand.toString())) {
      case 'count':
        return commandCount(args, ctx)
      case 'list':
        return commandList(args, ctx)
      case 'info':
        return commandInfoSubcommand(args, ctx)
      case 'docs':
        if (!ctx.server.profile.has('command.docs')) {
          throw unknownSubcommandError(
            'COMMAND',
            args.subcommand,
            ctx.server.profile,
          )
        }
        return commandDocsSubcommand(args, ctx)
      case 'getkeys':
        return commandGetKeys(args, ctx)
      case 'getkeysandflags':
        if (!ctx.server.profile.has('command.getkeysandflags')) {
          throw unknownSubcommandError(
            'COMMAND',
            args.subcommand,
            ctx.server.profile,
          )
        }
        return commandGetKeysAndFlags(args, ctx)
      case 'help':
        return commandHelp(args, ctx)
      default:
        throw unknownSubcommandError(
          'COMMAND',
          args.subcommand,
          ctx.server.profile,
        )
    }
  },
})

/**
 * Redis 6.2's COMMAND dispatch: by name and argument count at once, HELP and
 * COUNT taking no arguments, GETKEYS at least one and INFO any; anything else,
 * a wrong count included, is `addReplySubcommandSyntaxError`.
 */
function legacyCommandSubcommand(
  subcommand: Buffer,
  args: CommandArgs,
  ctx: RedisExecutionContext,
): RedisResult {
  const count = args.args.length
  switch (asciiLowerCase(subcommand.toString())) {
    case 'help':
      if (count === 0) return commandHelp(args, ctx)
      break
    case 'info':
      return commandInfoSubcommand(args, ctx)
    case 'count':
      if (count === 0) return commandCount(args, ctx)
      break
    case 'getkeys':
      if (count > 0) return commandGetKeys(args, ctx)
      break
  }
  throw subcommandSyntaxError('COMMAND', subcommand, ctx.server.profile)
}

function commandCount(
  args: CommandArgs,
  ctx: RedisExecutionContext,
): RedisResult {
  expectArgCount('command|count', args.args, 0)
  // Redis counts command-table entries, not their subcommands.
  return RedisResult.create(RedisValue.integer(allRootCommandInfos(ctx).length))
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
      throw errors.syntax()
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
    // Redis 6.2 answers a bare COMMAND INFO with an empty array.
    return commandInfo(
      ctx,
      ctx.server.profile.has('command.list') ? allRootCommandInfos(ctx) : [],
    )
  }

  return commandInfo(
    ctx,
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
  const keys = commandKeys(args, ctx, 'command|getkeys')
  return RedisResult.create(RedisValue.array(keys.map(({ key }) => bulk(key))))
}

function commandGetKeysAndFlags(
  args: CommandArgs,
  ctx: RedisExecutionContext,
): RedisResult {
  const keys = commandKeys(args, ctx, 'command|getkeysandflags')
  return RedisResult.create(
    RedisValue.array(
      keys.map(({ key, flags }) =>
        RedisValue.array([bulk(key), RedisValue.set(flags.map(simpleString))]),
      ),
    ),
  )
}

function commandHelp(
  args: CommandArgs,
  ctx: RedisExecutionContext,
): RedisResult {
  const profile = ctx.server.profile
  expectArgCount('command|help', args.args, 0)
  if (!profile.has('command.list')) {
    return helpReply(
      [
        'COMMAND <subcommand> [<arg> [value] [opt] ...]. Subcommands are:',
        '(no subcommand)',
        '    Return details about all Redis commands.',
        'COUNT',
        '    Return the total number of commands in this Redis server.',
        'GETKEYS <full-command>',
        '    Return the keys from a full Redis command.',
        'INFO [<command-name> ...]',
        '    Return details about multiple Redis commands.',
      ],
      profile,
    )
  }

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
    'DOCS [<command-name> ...]',
    '    Return documentation details about multiple Redis commands.',
    '    If no command names are given, documentation details for all',
    '    commands are returned.',
    'GETKEYS <full-command>',
    '    Return the keys from a full Redis command.',
    'GETKEYSANDFLAGS <full-command>',
    '    Return the keys and the access flags from a full Redis command.',
  ]
  // Valkey 8.0+ says "commands" and "this server" where Redis says "Redis".
  return helpReply(
    profile.has('command.help-valkey-wording')
      ? lines.map(line => line.replace(/ Redis /g, ' '))
      : lines,
    profile,
  )
}

/**
 * The keys `COMMAND GETKEYS` / `GETKEYSANDFLAGS` report, found the way Redis's
 * `getKeysSubcommandImpl` finds them, without running the command: lookup
 * (from 7.0 the `container|subcommand` entry, an unknown one being an invalid
 * command), then whether that entry has keys at all, then its table arity,
 * then the keys. From 7.0 (`command.getkeysandflags`) the key specs decide,
 * each key taking its spec's flags, unless a spec is `variable_flags` or
 * cannot be applied, when the command's getkeys proc (`rawKeys`) does; 6.2
 * asks the proc, else the legacy key range. A command that declares neither
 * key specs nor a proc is answered from its parsed keys (its key range when
 * it does not parse), with flags from its command flags.
 */
function commandKeys(
  args: CommandArgs,
  ctx: RedisExecutionContext,
  commandName: string,
): KeyWithFlags[] {
  const profile = ctx.server.profile
  if (args.args.length < minGetKeysArgs(profile)) {
    throw new WrongNumberOfArgumentsError(commandName)
  }

  const [target, ...rawArgs] = args.args
  const definition = ctx.executor.getCommandDefinition(
    asciiLowerCase(target.toString()),
  )
  if (
    !definition ||
    !inCommandTable(definition, profile) ||
    (rawArgs.length > 0 &&
      profile.has('error.unknown-subcommand-dispatch-timing') &&
      containerSubcommandExists(definition.name, rawArgs[0], profile) === false)
  ) {
    throw errors.invalidCommandSpecified()
  }

  const argv = [Buffer.from(definition.name), ...rawArgs]
  const subcommand = lookupSubcommandEntry(definition, rawArgs, profile)
  const entry = subcommand
    ? introspectionFor(subcommand.introspection, profile, subcommand.name)
    : introspectionFor(definition.introspection, profile, definition.name)
  const specs = entry?.keySpecs ?? []
  const proc = subcommand ? undefined : definition.rawKeys
  const range = legacyKeyRange(
    entry,
    subcommand ? undefined : definition.schema,
  )
  // Redis's doesCommandHaveKeys: a getkeys proc, or a key spec that is not
  // `not_key` (SPUBLISH / SSUBSCRIBE / SUNSUBSCRIBE have only `not_key` ones).
  if (
    !proc &&
    specs.length > 0 &&
    specs.every(spec => spec.flags.includes('not_key'))
  ) {
    throw errors.commandHasNoKeyArguments()
  }
  // Nothing declares where the keys are: a keyless command, or one (typically
  // added with `extraCommands`) whose keys come only from `keys(args)`. Its
  // parsed keys answer, and none - or a call that does not parse - means it
  // has no key arguments, which is what Redis answers for a keyless command
  // before checking its arity.
  if (!proc && specs.length === 0 && range.firstKey === 0) {
    const keys = parsedKeys(definition, rawArgs, ctx, range, argv)
    if (keys.length === 0) {
      throw errors.commandHasNoKeyArguments()
    }
    return keys
  }

  const arity =
    subcommand?.arity ??
    commandTableArity(definition.introspection, profile, definition.schema)
  if (failsTableArity(arity, rawArgs.length + 1)) {
    throw new RedisCommandError(
      'Invalid number of arguments specified for command',
    )
  }

  const keySpecEra = profile.has('command.getkeysandflags')
  let keys: readonly KeyWithFlags[] | null = null
  if (
    keySpecEra &&
    specs.length > 0 &&
    !specs.some(spec => spec.flags.includes('variable_flags'))
  ) {
    keys = keysFromKeySpecs(specs, argv)
  }
  if (!keys && proc) {
    keys = proc(argv)
  }
  if (!keys && specs.length === 0) {
    keys = parsedKeys(definition, rawArgs, ctx, range, argv)
  }
  if (!keys && !keySpecEra) {
    keys = legacyRangeKeys(range, argv).map(key => ({ key, flags: [] }))
  }

  if (!keys || keys.length === 0) {
    // EVAL / FCALL (`no_mandatory_keys`) answer an empty list from 7.0.
    if (keySpecEra && entry?.flags?.includes('no_mandatory_keys')) {
      return []
    }
    throw new RedisCommandError('Invalid arguments specified for command')
  }
  return [...keys]
}

// A command without key specs or a getkeys proc: the keys its parser finds,
// or, when it does not parse, its key range; flags from its command flags.
function parsedKeys(
  definition: CommandDefinition<unknown>,
  rawArgs: readonly Buffer[],
  ctx: RedisExecutionContext,
  range: LegacyKeyRange,
  argv: readonly Buffer[],
): KeyWithFlags[] {
  let keys: readonly Buffer[]
  try {
    keys = ctx.executor.plan(definition.name, rawArgs).keys
  } catch (err) {
    if (!(err instanceof RedisCommandError)) {
      throw err
    }
    keys = legacyRangeKeys(range, argv)
  }
  const flags = fallbackKeyAccessFlags(definition.flags)
  return keys.map(key => ({ key, flags }))
}

function allRootCommandInfos(ctx: RedisExecutionContext): CommandInfo[] {
  return ctx.executor
    .getCommandDefinitions()
    .filter(definition => inCommandTable(definition, ctx.server.profile))
    .map(definition => createCommandInfo(definition, ctx))
}

// Redis 6.2 has no QUIT entry: its connection loop answers QUIT before
// command lookup, so COMMAND does not list it and GETKEYS does not know it.
function inCommandTable(
  definition: CommandDefinition<unknown>,
  profile: CompatibilityProfile,
): boolean {
  return definition.name !== 'quit' || profile.has('command.quit-table-entry')
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
  const target = asciiLowerCase(name)
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
    definition.schema,
  )
}

function createCommandInfoFromIntrospection(
  name: string,
  fallbackFlags: readonly string[],
  declared: CommandIntrospection | undefined,
  ctx: RedisExecutionContext,
  schema?: CommandSchema<unknown>,
): CommandInfo {
  const introspection = introspectionFor(declared, ctx.server.profile, name)
  const keySpecs = introspection?.keySpecs ?? []
  const flags = introspection?.flags ?? fallbackFlags

  return {
    name,
    arity: commandArity(introspection, ctx, schema),
    flags,
    ...legacyKeyRange(introspection, schema),
    categories: introspection?.categories ?? inferCategories(flags),
    tips: introspection?.tips ?? [],
    keySpecs,
    // Redis 6.2's command table has no subcommand entries at all.
    subcommands: (ctx.server.profile.has(
      'error.unknown-subcommand-dispatch-timing',
    )
      ? (introspection?.subcommands ?? [])
      : []
    )
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

function commandArity(
  introspection: CommandIntrospection | undefined,
  ctx: RedisExecutionContext,
  schema?: CommandSchema<unknown>,
): number {
  return commandTableArity(introspection, ctx.server.profile, schema)
}

/**
 * The legacy first/last/step triple of `keySpecs` alone (see
 * `legacyKeyRange`), kept for callers that fold a spec list directly.
 */
export function keySpecsKeyRange(
  keySpecs: readonly CommandKeySpec[],
): LegacyKeyRange {
  return legacyKeyRange({ keySpecs })
}

function subcommandAvailable(
  introspection: CommandIntrospection,
  ctx: RedisExecutionContext,
): boolean {
  if (!introspection.name) {
    return true
  }

  const feature = SUBCOMMAND_FEATURES[introspection.name.toLowerCase()]
  return feature === undefined || ctx.server.profile.has(feature)
}

function commandInfo(
  ctx: RedisExecutionContext,
  infos: readonly (CommandInfo | null)[],
  preserveNulls = false,
): RedisResult {
  const extended = ctx.server.profile.has('command.info-extended-fields')
  return RedisResult.create(
    RedisValue.array(
      infos
        .map(info => {
          if (!info) {
            return preserveNulls ? RedisValue.null() : null
          }

          return formatCommandInfo(info, extended, ctx.server.profile)
        })
        .filter((value): value is RedisValue => value !== null),
    ),
  )
}

// Redis 6.2's entries stop at the ACL categories; 7.0 added tips, key specs
// and subcommands (`command.info-extended-fields`). Flags, categories and
// key-spec flags are status strings in sets, tips bulk strings in a set, a key
// spec is a map, and the subcommands an array - or, on Redis, an empty set
// when there are none. That is what RESP3 shows; RESP2 renders sets as arrays
// and maps as flat arrays.
function formatCommandInfo(
  info: CommandInfo,
  extended: boolean,
  profile: CompatibilityProfile,
): RedisValue {
  const fields = [
    bulkString(info.name),
    RedisValue.integer(info.arity),
    statusSet(info.flags),
    RedisValue.integer(info.firstKey),
    RedisValue.integer(info.lastKey),
    RedisValue.integer(info.keyStep),
    statusSet(info.categories),
  ]
  if (extended) {
    fields.push(
      RedisValue.set(info.tips.map(bulkString)),
      RedisValue.set(info.keySpecs.map(formatKeySpec)),
      info.subcommands.length === 0 &&
        !profile.has('command.info-subcommands-array')
        ? RedisValue.set([])
        : RedisValue.array(
            info.subcommands.map(subcommand =>
              formatCommandInfo(subcommand, extended, profile),
            ),
          ),
    )
  }
  return RedisValue.array(fields)
}

function statusSet(values: readonly string[]): RedisValue {
  return RedisValue.set(values.map(simpleString))
}

function formatKeySpec(spec: CommandKeySpec): RedisValue {
  const entries: [RedisValue, RedisValue][] = []
  if (spec.notes) {
    entries.push([bulkString('notes'), bulkString(spec.notes)])
  }
  entries.push(
    [bulkString('flags'), statusSet(spec.flags)],
    [bulkString('begin_search'), formatBeginSearch(spec)],
    [bulkString('find_keys'), formatFindKeys(spec)],
  )
  return RedisValue.map(entries)
}

function formatBeginSearch(spec: CommandKeySpec): RedisValue {
  const keyword = spec.beginSearchKeyword
  if (spec.beginSearchUnknown) {
    return searchType('unknown', [])
  }
  if (keyword) {
    return searchType('keyword', [
      ['keyword', bulkString(keyword.keyword)],
      ['startfrom', RedisValue.integer(keyword.startFrom)],
    ])
  }
  return searchType('index', [
    ['index', RedisValue.integer(spec.beginSearchIndex)],
  ])
}

function formatFindKeys(spec: CommandKeySpec): RedisValue {
  const keynum = spec.findKeysKeynum
  if (spec.findKeysUnknown) {
    return searchType('unknown', [])
  }
  if (keynum) {
    return searchType('keynum', [
      ['keynumidx', RedisValue.integer(keynum.keyNumIdx)],
      ['firstkey', RedisValue.integer(keynum.firstKey)],
      ['keystep', RedisValue.integer(keynum.keyStep)],
    ])
  }
  return searchType('range', [
    ['lastkey', RedisValue.integer(spec.lastKey)],
    ['keystep', RedisValue.integer(spec.keyStep)],
    ['limit', RedisValue.integer(spec.limit ?? 0)],
  ])
}

function searchType(
  type: string,
  spec: readonly [string, RedisValue][],
): RedisValue {
  return RedisValue.map([
    [bulkString('type'), bulkString(type)],
    [
      bulkString('spec'),
      RedisValue.map(spec.map(([name, value]) => [bulkString(name), value])),
    ],
  ])
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

function expectArgCount(
  commandName: string,
  args: readonly Buffer[],
  count: number,
): void {
  if (args.length !== count) {
    throw new WrongNumberOfArgumentsError(commandName)
  }
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
