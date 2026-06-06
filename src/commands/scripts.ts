import { defineCommand } from '../core/command-definition'
import { t } from '../core/command-schema'
import {
  NoScriptError,
  RedisSyntaxError,
  UnknownScriptSubcommandError,
  WrongNumberOfKeysError,
  WrongNumberOfArgumentsError,
} from '../core/redis-error'
import {
  getDefaultRedisLuaRuntime,
  luaReplyToRedisValue,
} from '../core/lua-runtime'
import type { RedisExecutionContext } from '../core/redis-context'
import { RedisResult } from '../core/redis-result'
import { RedisValue } from '../core/redis-value'
import { array, ok } from './helpers'

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
        return scriptDebug(args)
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
    const sha = ctx.server.scriptCache.load(args.script)
    return runLuaScript(args.script, keys, argv, ctx, sha)
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
    return runLuaScript(script, keys, argv, ctx, args.sha)
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
    throw new WrongNumberOfArgumentsError('script|flush')
  }

  const mode = args.rest[0]?.toString().toUpperCase()
  if (mode !== undefined && mode !== 'ASYNC' && mode !== 'SYNC') {
    throw new RedisSyntaxError()
  }

  ctx.server.scriptCache.flush()
  return ok()
}

function scriptKill(args: ScriptArgs): RedisResult {
  expectRestLength(args, 'script|kill', 0)
  return ok()
}

function scriptDebug(args: ScriptArgs): RedisResult {
  expectRestLength(args, 'script|debug', 1)
  const mode = args.rest[0].toString().toUpperCase()
  if (mode !== 'YES' && mode !== 'SYNC' && mode !== 'NO') {
    throw new RedisSyntaxError()
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
  sha: string,
): Promise<RedisResult> {
  const runtime = await getDefaultRedisLuaRuntime()

  try {
    const reply = runtime.eval(script, keys, argv, ctx, sha)
    return RedisResult.create(luaReplyToRedisValue(reply))
  } catch (err) {
    const message = err instanceof Error ? err.message : String(err)
    return luaRuntimeError(message)
  }
}

function luaRuntimeError(message: string): RedisResult {
  const match = /^([A-Z][A-Z0-9]*) (.+)$/.exec(message)
  if (!match) {
    return RedisResult.error(message, 'ERR')
  }

  return RedisResult.error(match[2], match[1])
}
