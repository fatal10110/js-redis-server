import {
  UnknownScriptCommand,
  UserFacedError,
  WrongNumberOfArguments,
} from '../../core/errors'
import { CapturingTransport } from '../../core/transports/capturing-transport'
import { Command, CommandResult } from '../../types'
import { LuaWasmEngine } from 'lua-redis-wasm'

type ReplyValue =
  | null
  | number
  | bigint
  | Buffer
  | { ok: Buffer }
  | { err: Buffer }
  | ReplyValue[]

type RedisHost = {
  redisCall: (args: Buffer[]) => ReplyValue
  redisPcall: (args: Buffer[]) => ReplyValue
  log: (level: number, message: Buffer) => void
}

type LuaEngine = {
  eval: (script: Buffer) => ReplyValue
  evalWithArgs: (
    script: Buffer,
    keys?: Array<Buffer | Uint8Array | string>,
    args?: Array<Buffer | Uint8Array | string>,
  ) => ReplyValue
}

export type LuaWasmModule = {
  create: (host: RedisHost) => LuaEngine
}

export interface LuaCommandContext {
  commands?: Record<string, Command>
  signal: AbortSignal
}

export type LuaRuntime = {
  eval: (script: Buffer, ctx: LuaCommandContext, sha: string) => ReplyValue
  evalWithArgs: (
    script: Buffer,
    keys: Buffer[],
    args: Buffer[],
    ctx: LuaCommandContext,
    sha: string,
  ) => ReplyValue
}

type LuaHostState = {
  ctx: LuaCommandContext | null
  sha: string
}

export async function createLuaRuntime(): Promise<LuaRuntime> {
  // TODO implement
  const hostState: LuaHostState = { ctx: null, sha: '' }
  const host: RedisHost = {
    redisCall: args => runLuaCommand(args, hostState),
    redisPcall: args => runLuaCommand(args, hostState),
    log: () => {},
  }
  const engine = await LuaWasmEngine.create({ host })

  return {
    eval: (script, ctx, sha) => {
      hostState.ctx = ctx
      hostState.sha = sha
      try {
        return engine.eval(script)
      } finally {
        hostState.ctx = null
        hostState.sha = ''
      }
    },
    evalWithArgs: (script, keys, args, ctx, sha) => {
      hostState.ctx = ctx
      hostState.sha = sha
      try {
        return engine.evalWithArgs(script, keys, args)
      } finally {
        hostState.ctx = null
        hostState.sha = ''
      }
    },
  }
}

function runLuaCommand(args: Buffer[], hostState: LuaHostState): ReplyValue {
  const ctx = hostState.ctx
  if (!ctx) {
    throw new Error('ERR Lua runtime is not initialized')
  }

  if (args.length === 0) {
    return new WrongNumberOfArguments('eval').toLuaError()
  }

  const rawCmd = args[0]
  const cmdName = rawCmd.toString().toLowerCase()
  const command = ctx.commands?.[cmdName]

  if (!command || !isAllowedInLua(command)) {
    return new UnknownScriptCommand(hostState.sha).toLuaError()
  }

  const capture = new CapturingTransport()
  try {
    command.run(rawCmd, args.slice(1), ctx.signal, capture)
  } catch (err) {
    if (err instanceof UserFacedError) {
      return err.toLuaError()
    }

    throw err
  }

  return commandResultToReplyValue(capture.getResults())
}

function isAllowedInLua(command: Command): boolean {
  const flags = command.metadata.flags
  return !flags.random && !flags.blocking && !flags.noscript && !flags.admin
}

function commandResultToReplyValue(result: CommandResult): ReplyValue {
  if (Array.isArray(result)) {
    return result.map(commandResultToReplyValue)
  }

  if (typeof result === 'string') {
    return Buffer.from(result)
  }

  if (result instanceof UserFacedError) {
    return result.toLuaError()
  }

  if (result instanceof Error) {
    throw result
  }

  return result
}
