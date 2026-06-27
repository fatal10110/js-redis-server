import { defineCommand } from '../core/command-definition'
import { t } from '../core/command-schema'
import {
  NoScriptError,
  RedisCommandError,
  ScriptDebugModeError,
  ScriptFlushOptionError,
  UnknownScriptSubcommandError,
  WrongNumberOfKeysError,
  WrongNumberOfArgumentsError,
} from '../core/redis-error'
import { luaReplyToRedisValue, renderScriptError } from '../core/lua-runtime'
import type { RedisExecutionContext } from '../core/redis-context'
import { RedisResult } from '../core/redis-result'
import { RedisValue } from '../core/redis-value'
import { parseFunctionLibrary, type RedisFunctionLibrary } from '../state'
import { array, bulk, ok } from './helpers'
import { commandSubcommandInfo } from './introspection'

type ScriptArgs = {
  subcommand: string
  rest: Buffer[]
}

type EvalArgs = {
  script: Buffer
  numKeys: number
  rest: Buffer[]
}

type EvalShaArgs = {
  sha: string
  numKeys: number
  rest: Buffer[]
}

type FunctionArgs = {
  subcommand: string
  rest: Buffer[]
}

type FcallArgs = {
  functionName: string
  numKeys: number
  rest: Buffer[]
}

export const scriptCommand = defineCommand({
  name: 'script',
  schema: t.object({
    subcommand: t.string(),
    rest: t.variadic(t.bulk()),
  }),
  flags: ['admin', 'noscript'],
  introspection: {
    arity: -2,
    flags: ['admin', 'noscript'],
    firstKey: 0,
    lastKey: 0,
    keyStep: 0,
    categories: ['@slow', '@scripting'],
    keySpecs: [],
    subcommands: [
      commandSubcommandInfo('script|debug', 3, {
        categories: ['@slow', '@scripting'],
      }),
      commandSubcommandInfo('script|exists', -3, {
        categories: ['@slow', '@scripting'],
      }),
      commandSubcommandInfo('script|flush', -2, {
        categories: ['@slow', '@scripting'],
      }),
      commandSubcommandInfo('script|help', 2, {
        categories: ['@slow', '@scripting'],
      }),
      commandSubcommandInfo('script|kill', 2, {
        categories: ['@slow', '@scripting'],
      }),
      commandSubcommandInfo('script|load', 3, {
        categories: ['@slow', '@scripting'],
      }),
    ],
  },
  keys: () => [],
  execute: (args, ctx) => {
    switch (args.subcommand.toLowerCase()) {
      case 'load':
        return scriptLoad(args, ctx)
      case 'exists':
        return scriptExists(args, ctx)
      case 'flush':
        return scriptFlush(args, ctx)
      case 'kill':
        return scriptKill(args)
      case 'debug':
        return scriptDebug(args, ctx)
      case 'help':
        return scriptHelp(args)
      default:
        throw new UnknownScriptSubcommandError(args.subcommand)
    }
  },
})

export const evalCommand = defineCommand<EvalArgs>({
  name: 'eval',
  schema: t.object({
    script: t.bulk(),
    numKeys: t.integer({ min: 0 }),
    rest: t.variadic(t.bulk()),
  }),
  flags: ['write', 'movablekeys', 'noscript'],
  capabilities: { scriptKeys: true, movableKeys: true },
  keys: evalKeys,
  execute: async (args, ctx) => {
    const { keys, argv } = splitEvalArgs(args)
    // Cache the script so a later EVALSHA can find it by digest.
    ctx.server.scriptCache.load(args.script)
    return runLuaScript(args.script, keys, argv, ctx)
  },
})

export const evalshaCommand = defineCommand<EvalShaArgs>({
  name: 'evalsha',
  schema: t.object({
    sha: t.string(),
    numKeys: t.integer({ min: 0 }),
    rest: t.variadic(t.bulk()),
  }),
  flags: ['write', 'movablekeys', 'noscript'],
  capabilities: { scriptKeys: true, movableKeys: true },
  keys: evalKeys,
  execute: async (args, ctx) => {
    const script = ctx.server.scriptCache.get(args.sha)
    if (!script) {
      throw new NoScriptError()
    }

    const { keys, argv } = splitEvalArgs(args)
    return runLuaScript(script, keys, argv, ctx)
  },
})

export const evalRoCommand = defineCommand<EvalArgs>({
  name: 'eval_ro',
  since: { redis: '7.0.0', valkey: '7.2.0' },
  schema: evalCommand.schema,
  flags: ['readonly', 'movablekeys', 'noscript'],
  capabilities: { scriptKeys: true, movableKeys: true },
  keys: evalKeys,
  execute: async (args, ctx) => {
    const { keys, argv } = splitEvalArgs(args)
    ctx.server.scriptCache.load(args.script)
    return runLuaScript(args.script, keys, argv, ctx, true)
  },
})

export const evalshaRoCommand = defineCommand<EvalShaArgs>({
  name: 'evalsha_ro',
  since: { redis: '7.0.0', valkey: '7.2.0' },
  schema: evalshaCommand.schema,
  flags: ['readonly', 'movablekeys', 'noscript'],
  capabilities: { scriptKeys: true, movableKeys: true },
  keys: evalKeys,
  execute: async (args, ctx) => {
    const script = ctx.server.scriptCache.get(args.sha)
    if (!script) {
      throw new NoScriptError()
    }

    const { keys, argv } = splitEvalArgs(args)
    return runLuaScript(script, keys, argv, ctx, true)
  },
})

export const functionCommand = defineCommand<FunctionArgs>({
  name: 'function',
  since: { redis: '7.0.0', valkey: '7.2.0' },
  schema: t.object({
    subcommand: t.string(),
    rest: t.variadic(t.bulk()),
  }),
  flags: ['admin', 'noscript'],
  introspection: {
    arity: -2,
    flags: ['admin', 'noscript'],
    firstKey: 0,
    lastKey: 0,
    keyStep: 0,
    categories: ['@slow', '@scripting'],
    subcommands: [
      commandSubcommandInfo('function|load', -3, {
        categories: ['@slow', '@scripting'],
      }),
      commandSubcommandInfo('function|delete', 3, {
        categories: ['@slow', '@scripting'],
      }),
      commandSubcommandInfo('function|list', -2, {
        categories: ['@slow', '@scripting'],
      }),
      commandSubcommandInfo('function|stats', 2, {
        categories: ['@slow', '@scripting'],
      }),
      commandSubcommandInfo('function|dump', 2, {
        categories: ['@slow', '@scripting'],
      }),
      commandSubcommandInfo('function|restore', -3, {
        categories: ['@slow', '@scripting'],
      }),
      commandSubcommandInfo('function|flush', -2, {
        categories: ['@slow', '@scripting'],
      }),
      commandSubcommandInfo('function|kill', 2, {
        categories: ['@slow', '@scripting'],
      }),
      commandSubcommandInfo('function|help', 2, {
        categories: ['@slow', '@scripting'],
      }),
    ],
  },
  keys: () => [],
  execute: (args, ctx) => {
    switch (args.subcommand.toLowerCase()) {
      case 'load':
        return functionLoad(args, ctx)
      case 'delete':
        return functionDelete(args, ctx)
      case 'list':
        return functionList(args, ctx)
      case 'stats':
        return functionStats(args, ctx)
      case 'dump':
        return bulk(ctx.server.functionRegistry.dump())
      case 'restore':
        return functionRestore(args, ctx)
      case 'flush':
        return functionFlush(args, ctx)
      case 'kill':
        return functionKill(args)
      case 'help':
        return functionHelp(args)
      default:
        throw new RedisCommandError(
          `unknown subcommand '${args.subcommand}'. Try FUNCTION HELP.`,
        )
    }
  },
})

export const fcallCommand = defineCommand<FcallArgs>({
  name: 'fcall',
  since: { redis: '7.0.0', valkey: '7.2.0' },
  schema: t.object({
    functionName: t.string(),
    numKeys: t.integer({ min: 0 }),
    rest: t.variadic(t.bulk()),
  }),
  flags: ['write', 'movablekeys', 'noscript'],
  capabilities: { scriptKeys: true, movableKeys: true },
  keys: fcallKeys,
  execute: (args, ctx) => runFunction(args, ctx, false),
})

export const fcallRoCommand = defineCommand<FcallArgs>({
  name: 'fcall_ro',
  since: { redis: '7.0.0', valkey: '7.2.0' },
  schema: fcallCommand.schema,
  flags: ['readonly', 'movablekeys', 'noscript'],
  capabilities: { scriptKeys: true, movableKeys: true },
  keys: fcallKeys,
  execute: (args, ctx) => runFunction(args, ctx, true),
})

export const scriptsCommands = [
  scriptCommand,
  evalCommand,
  evalshaCommand,
  evalRoCommand,
  evalshaRoCommand,
  functionCommand,
  fcallCommand,
  fcallRoCommand,
]

function scriptLoad(args: ScriptArgs, ctx: RedisExecutionContext): RedisResult {
  expectRestLength(args, 'script|load', 1)
  const sha = ctx.server.scriptCache.load(args.rest[0])
  return RedisResult.create(RedisValue.bulkString(Buffer.from(sha)))
}

function scriptExists(
  args: ScriptArgs,
  ctx: RedisExecutionContext,
): RedisResult {
  if (args.rest.length === 0) {
    throw new WrongNumberOfArgumentsError('script|exists')
  }

  return array(
    args.rest.map(sha =>
      RedisValue.integer(ctx.server.scriptCache.exists(sha.toString()) ? 1 : 0),
    ),
  )
}

function scriptFlush(
  args: ScriptArgs,
  ctx: RedisExecutionContext,
): RedisResult {
  if (args.rest.length > 1) {
    throw new ScriptFlushOptionError()
  }

  const mode = args.rest[0]?.toString().toUpperCase()
  if (mode !== undefined && mode !== 'ASYNC' && mode !== 'SYNC') {
    throw new ScriptFlushOptionError()
  }

  ctx.server.scriptCache.flush()
  return ok()
}

function scriptKill(args: ScriptArgs): RedisResult {
  expectRestLength(args, 'script|kill', 0)
  return RedisResult.error('No scripts in execution right now.', 'NOTBUSY')
}

function scriptDebug(
  args: ScriptArgs,
  ctx: RedisExecutionContext,
): RedisResult {
  expectRestLength(args, 'script|debug', 1)
  const mode = args.rest[0].toString().toUpperCase()
  if (mode !== 'YES' && mode !== 'SYNC' && mode !== 'NO') {
    throw new ScriptDebugModeError()
  }

  if (ctx.transactionReplay) {
    throw new RedisCommandError(
      'SCRIPT DEBUG must be called outside a pipeline',
    )
  }

  return ok()
}

function scriptHelp(args: ScriptArgs): RedisResult {
  expectRestLength(args, 'script|help', 0)
  return array(
    [
      'SCRIPT <subcommand> [<arg> [value] [opt] ...]. Subcommands are:',
      'DEBUG <YES|SYNC|NO>',
      '    Set the debug mode for subsequent scripts executed.',
      'EXISTS <sha1> [<sha1> ...]',
      '    Check if scripts exist in the script cache by SHA1 digest.',
      'FLUSH [ASYNC|SYNC]',
      '    Flush the Lua scripts cache. Very dangerous on replicas.',
      'HELP',
      '    Prints this help.',
      'KILL',
      '    Kill the currently executing Lua script.',
      'LOAD <script>',
      '    Load a script into the scripts cache without executing it.',
    ].map(line => RedisValue.bulkString(Buffer.from(line))),
  )
}

function expectRestLength(
  args: ScriptArgs,
  commandName: string,
  expected: number,
): void {
  if (args.rest.length !== expected) {
    throw new WrongNumberOfArgumentsError(commandName)
  }
}

function evalKeys(args: Pick<EvalArgs, 'numKeys' | 'rest'>): readonly Buffer[] {
  validateNumberOfKeys(args)
  return args.rest.slice(0, args.numKeys)
}

function splitEvalArgs(args: EvalArgs | EvalShaArgs): {
  keys: Buffer[]
  argv: Buffer[]
} {
  validateNumberOfKeys(args)
  return {
    keys: args.rest.slice(0, args.numKeys),
    argv: args.rest.slice(args.numKeys),
  }
}

function validateNumberOfKeys(args: Pick<EvalArgs, 'numKeys' | 'rest'>): void {
  if (args.numKeys > args.rest.length) {
    throw new WrongNumberOfKeysError()
  }
}

async function runLuaScript(
  script: Buffer,
  keys: readonly Buffer[],
  argv: readonly Buffer[],
  ctx: RedisExecutionContext,
  readOnly = false,
): Promise<RedisResult> {
  const runtime = await ctx.server.getLuaRuntime()

  try {
    const reply = renderScriptError(
      runtime.eval(script, keys, argv, ctx, { readOnly }),
    )
    return RedisResult.create(luaReplyToRedisValue(reply))
  } catch (err) {
    if (err instanceof RedisCommandError) {
      return RedisResult.error(err.message, err.code)
    }

    const message = err instanceof Error ? err.message : String(err)
    return RedisResult.error(message, 'ERR')
  }
}

function functionLoad(
  args: FunctionArgs,
  ctx: RedisExecutionContext,
): RedisResult {
  let replace = false
  let code: Buffer

  if (args.rest.length === 1) {
    code = args.rest[0]
  } else if (
    args.rest.length === 2 &&
    args.rest[0].toString().toLowerCase() === 'replace'
  ) {
    replace = true
    code = args.rest[1]
  } else {
    throw new WrongNumberOfArgumentsError('function|load')
  }

  try {
    const library = parseFunctionLibrary(code)
    ctx.server.functionRegistry.load(library, replace)
    return bulk(Buffer.from(library.name))
  } catch (err) {
    throw new RedisCommandError(errorMessage(err))
  }
}

function functionDelete(
  args: FunctionArgs,
  ctx: RedisExecutionContext,
): RedisResult {
  expectRestLength(args, 'function|delete', 1)
  if (!ctx.server.functionRegistry.delete(args.rest[0].toString())) {
    throw new RedisCommandError(`Library not found`)
  }

  return ok()
}

function functionList(
  args: FunctionArgs,
  ctx: RedisExecutionContext,
): RedisResult {
  const { libraryName, withCode } = parseFunctionListOptions(args.rest)
  const libraries = ctx.server.functionRegistry
    .list()
    .filter(
      library => libraryName === undefined || library.name === libraryName,
    )
  return array(
    libraries.map(library => functionLibraryReply(library, withCode)),
  )
}

function functionStats(
  args: FunctionArgs,
  ctx: RedisExecutionContext,
): RedisResult {
  expectRestLength(args, 'function|stats', 0)
  const libraries = ctx.server.functionRegistry.list()
  const functionCount = libraries.reduce(
    (count, library) => count + library.functions.length,
    0,
  )

  return array([
    RedisValue.bulkString(Buffer.from('running_script')),
    RedisValue.null(),
    RedisValue.bulkString(Buffer.from('engines')),
    RedisValue.array([
      RedisValue.bulkString(Buffer.from('LUA')),
      RedisValue.array([
        RedisValue.bulkString(Buffer.from('libraries_count')),
        RedisValue.integer(libraries.length),
        RedisValue.bulkString(Buffer.from('functions_count')),
        RedisValue.integer(functionCount),
      ]),
    ]),
  ])
}

function functionRestore(
  args: FunctionArgs,
  ctx: RedisExecutionContext,
): RedisResult {
  if (args.rest.length < 1 || args.rest.length > 2) {
    throw new WrongNumberOfArgumentsError('function|restore')
  }

  const mode = args.rest[1]?.toString().toLowerCase() ?? 'append'
  if (mode !== 'append' && mode !== 'flush' && mode !== 'replace') {
    throw new RedisCommandError(
      'FUNCTION RESTORE only supports FLUSH|APPEND|REPLACE',
    )
  }

  try {
    ctx.server.functionRegistry.restore(args.rest[0], mode)
    return ok()
  } catch (err) {
    throw new RedisCommandError(errorMessage(err))
  }
}

function functionFlush(
  args: FunctionArgs,
  ctx: RedisExecutionContext,
): RedisResult {
  if (args.rest.length > 1) {
    throw functionFlushOptionError()
  }

  const mode = args.rest[0]?.toString().toUpperCase()
  if (mode !== undefined && mode !== 'ASYNC' && mode !== 'SYNC') {
    throw functionFlushOptionError()
  }

  ctx.server.functionRegistry.clear()
  return ok()
}

function functionKill(args: FunctionArgs): RedisResult {
  expectRestLength(args, 'function|kill', 0)
  return RedisResult.error('No scripts in execution right now.', 'NOTBUSY')
}

function functionHelp(args: FunctionArgs): RedisResult {
  expectRestLength(args, 'function|help', 0)
  return array(
    [
      'FUNCTION <subcommand> [<arg> [value] [opt] ...]. Subcommands are:',
      'LOAD [REPLACE] <FUNCTION CODE>',
      '    Create a library with the functions in the given code.',
      'DELETE <LIBRARY NAME>',
      '    Delete the given library and all its functions.',
      'LIST [LIBRARYNAME <LIBRARY NAME>] [WITHCODE]',
      '    Return information about the functions and libraries.',
      'STATS',
      '    Return information about the current function execution.',
      'DUMP',
      '    Return a serialized payload representing the loaded libraries.',
      'RESTORE <PAYLOAD> [FLUSH|APPEND|REPLACE]',
      '    Restore libraries from a serialized payload.',
      'FLUSH [ASYNC|SYNC]',
      '    Delete all the libraries.',
      'KILL',
      '    Kill the currently executing function.',
      'HELP',
      '    Prints this help.',
    ].map(line => RedisValue.bulkString(Buffer.from(line))),
  )
}

async function runFunction(
  args: FcallArgs,
  ctx: RedisExecutionContext,
  readOnly: boolean,
): Promise<RedisResult> {
  const fn = ctx.server.functionRegistry.findFunction(args.functionName)
  if (!fn) {
    throw new RedisCommandError('Function not found')
  }

  const { keys, argv } = splitFcallArgs(args)
  return runLuaScript(fn.script, keys, argv, ctx, readOnly)
}

function fcallKeys(args: FcallArgs): readonly Buffer[] {
  validateNumberOfKeys(args)
  return args.rest.slice(0, args.numKeys)
}

function splitFcallArgs(args: FcallArgs): {
  keys: Buffer[]
  argv: Buffer[]
} {
  validateNumberOfKeys(args)
  return {
    keys: args.rest.slice(0, args.numKeys),
    argv: args.rest.slice(args.numKeys),
  }
}

function parseFunctionListOptions(args: readonly Buffer[]): {
  libraryName?: string
  withCode: boolean
} {
  let libraryName: string | undefined
  let withCode = false

  for (let index = 0; index < args.length; index++) {
    const option = args[index].toString().toLowerCase()
    if (option === 'withcode') {
      withCode = true
      continue
    }

    if (option === 'libraryname' && index + 1 < args.length) {
      libraryName = args[++index].toString()
      continue
    }

    throw new RedisCommandError('syntax error')
  }

  return { libraryName, withCode }
}

function functionLibraryReply(
  library: RedisFunctionLibrary,
  withCode: boolean,
): RedisValue {
  const items = [
    RedisValue.bulkString(Buffer.from('library_name')),
    RedisValue.bulkString(Buffer.from(library.name)),
    RedisValue.bulkString(Buffer.from('engine')),
    RedisValue.bulkString(Buffer.from('LUA')),
    RedisValue.bulkString(Buffer.from('functions')),
    RedisValue.array(library.functions.map(functionReply)),
  ]

  if (withCode) {
    items.push(
      RedisValue.bulkString(Buffer.from('library_code')),
      RedisValue.bulkString(library.code),
    )
  }

  return RedisValue.array(items)
}

function functionReply(fn: { name: string }): RedisValue {
  return RedisValue.array([
    RedisValue.bulkString(Buffer.from('name')),
    RedisValue.bulkString(Buffer.from(fn.name)),
    RedisValue.bulkString(Buffer.from('description')),
    RedisValue.bulkString(null),
    RedisValue.bulkString(Buffer.from('flags')),
    RedisValue.array([]),
  ])
}

function functionFlushOptionError(): RedisCommandError {
  return new RedisCommandError('FUNCTION FLUSH only support SYNC|ASYNC option')
}

function errorMessage(err: unknown): string {
  return err instanceof Error ? err.message : String(err)
}
