import { LuaEngine } from 'wasmoon'
import crypto from 'crypto'
import { Command, CommandResult, ExecutionContext } from '../../../../types'
import {
  ExpectedInteger,
  UnknownScriptCommand,
  WrongNumberOfArguments,
  WrongNumberOfKeys,
} from '../../../../core/errors'
import { LuaTransport } from '../../lua-transport'
import { DB } from '../../db'
import { defineCommand, CommandCategory } from '../metadata'
import { createSchemaCommand, SchemaCommandRegistration, t } from '../../schema'

const metadata = defineCommand('eval', {
  arity: -3,
  flags: { write: true, noscript: true, movablekeys: true },
  firstKey: 0,
  lastKey: 0,
  keyStep: 0,
  categories: [CommandCategory.SCRIPT],
})

type EvalArgs = [string, number, Buffer[]]

export const EvalCommandDefinition: SchemaCommandRegistration<EvalArgs> = {
  metadata,
  schema: t.tuple([t.string(), t.integer(), t.variadic(t.key())]),
  getKeys: (_rawCmd, args) => extractEvalKeys('eval', args),
  handler: async ([script, keyCount, rest], ctx) => {
    const { keys, args } = splitEvalArguments(keyCount, rest)
    const luaEngine = ctx.luaEngine
    const commands = ctx.commands

    if (!luaEngine) {
      throw new Error('Lua engine is not available for EVAL')
    }

    if (!commands) {
      throw new Error('Command registry is not available for EVAL')
    }

    return executeEvalScript(script, keys, args, {
      luaEngine,
      commands,
      executionContext: ctx.executionContext,
      signal: ctx.signal,
    })
  },
}

export function extractEvalKeys(commandName: string, args: Buffer[]): Buffer[] {
  if (args.length < 2) {
    throw new WrongNumberOfArguments(commandName)
  }

  const keysNum = Number(args[1].toString())

  if (isNaN(keysNum)) {
    throw new ExpectedInteger()
  }

  if (args.length - 2 < keysNum) {
    throw new WrongNumberOfKeys()
  }

  const keys: Buffer[] = []

  for (let i = 0; i < keysNum; i++) {
    keys.push(args[2 + i])
  }

  return keys
}

type EvalExecutionContext = {
  luaEngine: LuaEngine
  commands: Record<string, Command>
  executionContext?: ExecutionContext
  signal: AbortSignal
}

export async function executeEvalScript(
  script: string,
  keys: Buffer[],
  args: Buffer[],
  ctx: EvalExecutionContext,
): Promise<CommandResult> {
  const sha = crypto.createHash('sha1').update(script).digest('hex')
  const keyArgs = keys.map(key => `("${key.toString('hex')}"):fromhex()`)
  const scriptArgs = args.map(arg => `("${arg.toString('hex')}"):fromhex()`)
  const luaScript = buildLuaScript(script, keyArgs, scriptArgs)

  if (ctx.executionContext) {
    return runWithContext(
      luaScript,
      sha,
      ctx.luaEngine,
      ctx.commands,
      ctx.executionContext,
      ctx.signal,
    )
  }

  return runLegacy(luaScript, sha, ctx.luaEngine, ctx.commands, ctx.signal)
}

export function splitEvalArguments(
  keyCount: number,
  rest: Buffer[],
): { keys: Buffer[]; args: Buffer[] } {
  const resolvedKeyCount = keyCount > 0 ? keyCount : 0

  if (rest.length < resolvedKeyCount) {
    throw new WrongNumberOfKeys()
  }

  return {
    keys: rest.slice(0, resolvedKeyCount),
    args: rest.slice(resolvedKeyCount),
  }
}

async function runWithContext(
  luaScript: string,
  sha: string,
  lua: LuaEngine,
  commands: Record<string, Command>,
  executionContext: ExecutionContext,
  signal: AbortSignal,
): Promise<CommandResult> {
  const luaTransport = new LuaTransport()

  lua.global.set('redisCall', async (cmdName: string, luaArgs: string[]) => {
    const cmd = commands[cmdName.toLowerCase()]

    if (!cmd) {
      throw new UnknownScriptCommand(sha)
    }

    const argsBuffer = luaArgs.map(arg => Buffer.from(arg, 'hex'))

    luaTransport.reset()

    await executionContext.execute(
      luaTransport,
      Buffer.from(cmdName),
      argsBuffer,
      signal,
    )

    const response = luaTransport.getResponse()

    return convertResponseToHex(response)
  })

  const result = await lua.doString(luaScript)

  return { response: Buffer.from(result, 'hex') }
}

async function runLegacy(
  luaScript: string,
  sha: string,
  lua: LuaEngine,
  commands: Record<string, Command>,
  signal: AbortSignal,
): Promise<CommandResult> {
  lua.global.set('redisCall', async (cmdName: string, args: string[]) => {
    const rawCmd = Buffer.from(cmdName)
    const argsBuffer = args.map(arg => Buffer.from(arg, 'hex'))
    const cmd = commands[cmdName]

    if (!cmd) {
      throw new UnknownScriptCommand(sha)
    }

    const { response } = await cmd.run(rawCmd, argsBuffer, signal)

    return convertResponseToHex(response)
  })

  const res = await lua.doString(luaScript)

  return { response: Buffer.from(res, 'hex') }
}

function buildLuaScript(
  script: string,
  keys: string[],
  scriptArgs: string[],
): string {
  return `
      function string.fromhex(str)
          return (str:gsub('..', function (cc)
              return string.char(tonumber(cc, 16))
          end))
      end

      function string.tohex(str)
          return (str:gsub('.', function (c)
              return string.format('%02X', string.byte(c))
          end))
      end

      redisInstance = {}

      redisInstance.call = function (cmd, ...)
        local args = {...}

        for i, v in ipairs(args) do
          args[i] = v:tohex()
        end

        local res = redisCall(cmd, args):await()

        -- TODO res not always hex string
        return res:fromhex()
      end

      function run(ARGV, KEYS, redis)
        ${script}
      end

      -- TODO hex
      local args = { ${scriptArgs.join(',')} }
      -- TODO hex
      local keys = { ${keys.join(',')} }

      res = run(args, keys, redisInstance)
      -- TODO not always hexable
      return res:tohex()
    `
}

function convertResponseToHex(response: unknown): string {
  let bufferRes: Buffer

  if (response instanceof Buffer) {
    bufferRes = response
  } else if (
    response instanceof Object &&
    Object.hasOwn(response, 'toString')
  ) {
    bufferRes = Buffer.from(response.toString())
  } else if (typeof response === 'string') {
    bufferRes = Buffer.from(response)
  } else {
    throw new Error(`Unsupported input of type ${typeof response}`)
  }

  return bufferRes.toString('hex')
}

export default function (
  lua: LuaEngine,
  commands: Record<string, Command>,
  db?: DB,
  executionContext?: ExecutionContext,
): Command {
  if (!db) {
    throw new Error('DB is required for EVAL')
  }

  return createSchemaCommand(EvalCommandDefinition, {
    db,
    luaEngine: lua,
    commands,
    executionContext,
  })
}
