import { createHash } from 'node:crypto'
import { asciiLowerCase } from '../core/ascii-case'
import { numkeysGetKeys } from '../core/key-specs'
import { defineCommand } from '../core/command-definition'
import { t } from '../core/command-schema'
import {
  RedisCommandError,
  WrongNumberOfArgumentsError,
  errors,
} from '../core/redis-error'
import {
  isCompileError,
  luaReplyToRedisValue,
  renderScriptError,
  type LuaReplyValue,
} from '../core/lua-runtime'
import type { RedisExecutionContext } from '../core/redis-context'
import { RedisResult } from '../core/redis-result'
import { RedisValue } from '../core/redis-value'
import {
  isScriptShebangFlag,
  parseScriptShebang,
  type ParsedScript,
} from '../core/script-shebang'
import {
  parseFunctionLibrary,
  type RedisFunctionDefinition,
  type RedisFunctionLibrary,
} from '../state'
import {
  functionLibraryBody,
  parseFunctionLibraryName,
} from '../state/function-registry'
import { array, bulk, ok, unknownSubcommandError } from './helpers'
import { commandSubcommandInfo } from './introspection'

type ScriptArgs = {
  subcommand: Buffer
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
  subcommand: Buffer
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
    // Raw bytes, not `t.string()`: the unknown-subcommand reply echoes the
    // name the client sent, and a UTF-8 decode here would lose its bytes.
    subcommand: t.bulk(),
    rest: t.variadic(t.bulk()),
  }),
  flags: ['admin', 'noscript'],
  introspection: {
    subcommands: [
      commandSubcommandInfo('script|debug', 3),
      commandSubcommandInfo('script|exists', -3),
      commandSubcommandInfo('script|flush', -2),
      commandSubcommandInfo('script|help', 2),
      commandSubcommandInfo('script|kill', 2),
      commandSubcommandInfo('script|load', 3),
    ],
  },
  keys: () => [],
  execute: (args, ctx) => {
    switch (asciiLowerCase(args.subcommand.toString())) {
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
        throw unknownSubcommandError(
          'SCRIPT',
          args.subcommand,
          ctx.server.profile,
        )
    }
  },
})

export const evalCommand = defineCommand<EvalArgs>({
  name: 'eval',
  rawKeys: numkeysGetKeys(0, 2, 3),
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
    return runLuaScript(args.script, keys, argv, ctx, { cache: true })
  },
})

export const evalshaCommand = defineCommand<EvalShaArgs>({
  name: 'evalsha',
  rawKeys: numkeysGetKeys(0, 2, 3),
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
      throw errors.noScript(ctx.server.profile)
    }

    const { keys, argv } = splitEvalArgs(args)
    return runLuaScript(script, keys, argv, ctx)
  },
})

export const evalRoCommand = defineCommand<EvalArgs>({
  name: 'eval_ro',
  rawKeys: numkeysGetKeys(0, 2, 3),
  since: { redis: '7.0.0', valkey: '7.2.0' },
  schema: evalCommand.schema,
  flags: ['readonly', 'movablekeys', 'noscript'],
  capabilities: { scriptKeys: true, movableKeys: true },
  keys: evalKeys,
  execute: async (args, ctx) => {
    const { keys, argv } = splitEvalArgs(args)
    return runLuaScript(args.script, keys, argv, ctx, {
      cache: true,
      readOnly: true,
    })
  },
})

export const evalshaRoCommand = defineCommand<EvalShaArgs>({
  name: 'evalsha_ro',
  rawKeys: numkeysGetKeys(0, 2, 3),
  since: { redis: '7.0.0', valkey: '7.2.0' },
  schema: evalshaCommand.schema,
  flags: ['readonly', 'movablekeys', 'noscript'],
  capabilities: { scriptKeys: true, movableKeys: true },
  keys: evalKeys,
  execute: async (args, ctx) => {
    const script = ctx.server.scriptCache.get(args.sha)
    if (!script) {
      throw errors.noScript(ctx.server.profile)
    }

    const { keys, argv } = splitEvalArgs(args)
    return runLuaScript(script, keys, argv, ctx, { readOnly: true })
  },
})

export const functionCommand = defineCommand<FunctionArgs>({
  name: 'function',
  since: { redis: '7.0.0', valkey: '7.2.0' },
  schema: t.object({
    // Raw bytes, not `t.string()`: the unknown-subcommand reply echoes the
    // name the client sent, and a UTF-8 decode here would lose its bytes.
    subcommand: t.bulk(),
    rest: t.variadic(t.bulk()),
  }),
  flags: ['admin', 'noscript'],
  introspection: {
    subcommands: [
      commandSubcommandInfo('function|load', -3),
      commandSubcommandInfo('function|delete', 3),
      commandSubcommandInfo('function|list', -2),
      commandSubcommandInfo('function|stats', 2),
      commandSubcommandInfo('function|dump', 2),
      commandSubcommandInfo('function|restore', -3),
      commandSubcommandInfo('function|flush', -2),
      commandSubcommandInfo('function|kill', 2),
      commandSubcommandInfo('function|help', 2),
    ],
  },
  keys: () => [],
  execute: (args, ctx) => {
    switch (asciiLowerCase(args.subcommand.toString())) {
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
        throw unknownSubcommandError(
          'FUNCTION',
          args.subcommand,
          ctx.server.profile,
        )
    }
  },
})

export const fcallCommand = defineCommand<FcallArgs>({
  name: 'fcall',
  rawKeys: numkeysGetKeys(0, 2, 3),
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
  rawKeys: numkeysGetKeys(0, 2, 3),
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

/**
 * Caches a script without running it. Like real Redis (every version), one
 * that does not compile is refused with the error EVAL would give and is not
 * cached, and so is one whose shebang Redis refuses (7.0+, see
 * {@link parseScriptShebang}). The shebang line is blanked for the compile.
 */
async function scriptLoad(
  args: ScriptArgs,
  ctx: RedisExecutionContext,
): Promise<RedisResult> {
  expectRestLength(args, 'script|load', 1)
  const script = args.rest[0]
  const parsed = parseScriptShebang(script, ctx.server.profile)
  const runtime = await ctx.server.getLuaRuntime()
  let compileError: ReturnType<typeof runtime.compile>
  try {
    compileError = runtime.compile(parsed.body)
  } catch (err) {
    // As for EVAL: the engine throws only for a script its heap cannot hold,
    // or when it has faulted (the server then replaces it, #539).
    return RedisResult.error(errorMessage(err), 'ERR')
  }
  if (compileError) {
    return RedisResult.create(
      luaReplyToRedisValue(
        renderScriptError(compileError, { profile: ctx.server.profile }),
      ),
    )
  }
  const sha = ctx.server.scriptCache.load(script)
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
    throw errors.scriptFlushOption()
  }

  const mode = args.rest[0]?.toString().toUpperCase()
  if (mode !== undefined && mode !== 'ASYNC' && mode !== 'SYNC') {
    throw errors.scriptFlushOption()
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
    throw errors.scriptDebugMode()
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
    throw errors.wrongNumberOfKeys()
  }
}

async function runLuaScript(
  script: Buffer,
  keys: readonly Buffer[],
  argv: readonly Buffer[],
  ctx: RedisExecutionContext,
  options: {
    /** EVAL / EVAL_RO: cache the script for EVALSHA once it compiles. */
    cache?: boolean
    readOnly?: boolean
  } = {},
): Promise<RedisResult> {
  // A shebang Redis refuses fails before anything is compiled or cached.
  const parsed = parseScriptShebang(script, ctx.server.profile)
  // The engine names the SHA of the body it ran; Redis names the script's.
  const sha = parsed.body === script ? undefined : scriptSha(script)
  const render = (value: LuaReplyValue) =>
    RedisResult.create(
      luaReplyToRedisValue(
        renderScriptError(value, { profile: ctx.server.profile, sha }),
        ctx.server.profile,
      ),
    )
  const runtime = await ctx.server.getLuaRuntime()

  try {
    // Redis caches a script once it compiles, before it checks the
    // shebang's flags against the call, so a refused script is cached.
    const refusal = scriptRunRefusal(parsed.flags, options.readOnly, ctx)
    if (refusal) {
      const compileError = runtime.compile(parsed.body)
      if (compileError) {
        return render(compileError)
      }
      if (options.cache) {
        ctx.server.scriptCache.load(script)
      }
      return RedisResult.fromError(refusal)
    }

    const result = runtime.eval(parsed.body, keys, argv, ctx, {
      readOnly:
        options.readOnly === true || parsed.flags?.includes('no-writes'),
    })
    // Real Redis caches a script when it compiles, before it runs, so one
    // that fails at run time is cached and one that does not compile is not.
    if (options.cache && !isCompileError(result)) {
      ctx.server.scriptCache.load(script)
    }
    return render(result)
  } catch (err) {
    if (err instanceof RedisCommandError) {
      return RedisResult.fromError(err)
    }

    const message = err instanceof Error ? err.message : String(err)
    return RedisResult.error(message, 'ERR')
  }
}

/**
 * Redis's `scriptPrepareForRun` checks of a script's declared flags against
 * the call, or `null` when it may run. A script without a shebang (`flags`
 * `null`) declares nothing and is never refused here. `allow-oom`,
 * `allow-stale` and `allow-cross-slot-keys` are accepted but change nothing:
 * the server has no `maxmemory`, no stale replica and no per-script slot
 * check for them to relax.
 */
function scriptRunRefusal(
  flags: ParsedScript['flags'],
  readOnly: boolean | undefined,
  ctx: RedisExecutionContext,
): RedisCommandError | null {
  if (!flags) {
    return null
  }
  if (flags.includes('no-cluster') && ctx.server.clusterEnabled) {
    return new RedisCommandError(
      "Can not run script on cluster, 'no-cluster' flag is set.",
    )
  }
  if (readOnly && !flags.includes('no-writes')) {
    return new RedisCommandError(
      'Can not execute a script with write flag using *_ro command.',
    )
  }
  return null
}

function scriptSha(script: Buffer): string {
  return createHash('sha1').update(script).digest('hex')
}

/**
 * Redis's order (`functionsCreateWithLibraryCtx`): the metadata, an existing
 * library without REPLACE, then the code must compile, and only then must it
 * register a function. A library that does not compile is refused and
 * nothing changes, REPLACE included (#538).
 */
async function functionLoad(
  args: FunctionArgs,
  ctx: RedisExecutionContext,
): Promise<RedisResult> {
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

  let library: RedisFunctionLibrary
  try {
    const name = parseFunctionLibraryName(code)
    if (!replace && ctx.server.functionRegistry.has(name)) {
      throw new Error(`Library '${name}' already exists`)
    }
    const runtime = await ctx.server.getLuaRuntime()
    const compileError = runtime.compile(functionLibraryBody(code))
    if (compileError) {
      throw new Error(functionCompileError(compileError.err))
    }
    library = parseFunctionLibrary(code)
    ctx.server.functionRegistry.load(library, replace)
  } catch (err) {
    throw new RedisCommandError(errorMessage(err))
  }
  return bulk(Buffer.from(library.name))
}

/**
 * Redis compiles a library under the chunk name `user_function`
 * (`Error compiling function: user_function:2: ...`); the engine always
 * names its chunk `user_script`, so the prefix is swapped here.
 */
function functionCompileError(luaMessage: Buffer): string {
  const message = luaMessage.toString()
  const engineChunk = 'user_script:'
  return `Error compiling function: ${
    message.startsWith(engineChunk)
      ? `user_function:${message.slice(engineChunk.length)}`
      : message
  }`
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

  return RedisResult.create(
    RedisValue.map([
      [RedisValue.bulkString(Buffer.from('running_script')), RedisValue.null()],
      [
        RedisValue.bulkString(Buffer.from('engines')),
        RedisValue.map([
          [
            RedisValue.bulkString(Buffer.from('LUA')),
            RedisValue.map([
              [
                RedisValue.bulkString(Buffer.from('libraries_count')),
                RedisValue.integer(libraries.length),
              ],
              [
                RedisValue.bulkString(Buffer.from('functions_count')),
                RedisValue.integer(functionCount),
              ],
            ]),
          ],
        ]),
      ],
    ]),
  )
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
      'Wrong restore policy given, value should be either FLUSH, APPEND or REPLACE.',
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
    throw errors.functionFlushOption()
  }

  const mode = args.rest[0]?.toString().toUpperCase()
  if (mode !== undefined && mode !== 'ASYNC' && mode !== 'SYNC') {
    throw errors.functionFlushOption()
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

  // A function always declares its flags (it never runs in the shebang-less
  // compatibility mode), so they are checked like a shebang's.
  const refusal = scriptRunRefusal(
    fn.flags.filter(isScriptShebangFlag),
    readOnly,
    ctx,
  )
  if (refusal) {
    throw refusal
  }

  const { keys, argv } = splitFcallArgs(args)
  // A no-writes function runs read-only under FCALL too.
  return runLuaScript(fn.script, keys, argv, ctx, {
    readOnly: readOnly || fn.flags.includes('no-writes'),
  })
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

    throw errors.syntax()
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

function functionReply(fn: RedisFunctionDefinition): RedisValue {
  return RedisValue.array([
    RedisValue.bulkString(Buffer.from('name')),
    RedisValue.bulkString(Buffer.from(fn.name)),
    RedisValue.bulkString(Buffer.from('description')),
    RedisValue.bulkString(null),
    RedisValue.bulkString(Buffer.from('flags')),
    RedisValue.array(
      fn.flags.map(flag => RedisValue.bulkString(Buffer.from(flag))),
    ),
  ])
}

function errorMessage(err: unknown): string {
  return err instanceof Error ? err.message : String(err)
}
