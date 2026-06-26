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
import { luaReplyToRedisValue } from '../core/lua-runtime'
import type { RedisExecutionContext } from '../core/redis-context'
import { RedisResult } from '../core/redis-result'
import { RedisValue } from '../core/redis-value'
import { array, ok } from './helpers'
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

export const scriptsCommands = [scriptCommand, evalCommand, evalshaCommand]

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
): Promise<RedisResult> {
  const runtime = await ctx.server.getLuaRuntime()

  try {
    const reply = runtime.eval(script, keys, argv, ctx)
    return RedisResult.create(luaReplyToRedisValue(reply))
  } catch (err) {
    if (err instanceof RedisCommandError) {
      return RedisResult.error(err.message, err.code)
    }

    const message = err instanceof Error ? err.message : String(err)
    return RedisResult.error(message, 'ERR')
  }
}
