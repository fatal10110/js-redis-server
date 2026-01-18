import {
  ScriptCallNoCommand,
  UnknownScriptCommand,
  UserFacedError,
} from '../../core/errors'
import { CapturingTransport } from '../../core/transports/capturing-transport'
import { Command, CommandResult } from '../../types'
import { ReplyValue, load, LuaWasmModule, LuaEngine } from 'lua-redis-wasm'

export interface LuaCommandContext {
  luaCommands?: Record<string, Command>
  signal: AbortSignal
}

type LuaHostState = {
  ctx: LuaCommandContext | null
  sha: string
}

export class LuaRuntime {
  private readonly hostState: LuaHostState = { ctx: null, sha: '' }
  private engine: LuaEngine

  constructor(module: LuaWasmModule) {
    this.engine = module.create({
      redisCall: args => this.runLuaCommand(args, this.hostState),
      redisPcall: args => this.runLuaCommand(args, this.hostState),
      log: () => {},
    })
  }

  eval(script: Buffer, ctx: LuaCommandContext, sha: string) {
    this.hostState.ctx = ctx
    this.hostState.sha = sha
    try {
      return this.engine.eval(script)
    } finally {
      this.hostState.ctx = null
      this.hostState.sha = ''
    }
  }

  evalWithArgs(
    script: Buffer,
    keys: Buffer[],
    args: Buffer[],
    ctx: LuaCommandContext,
    sha: string,
  ) {
    this.hostState.ctx = ctx
    this.hostState.sha = sha
    try {
      return this.engine.evalWithArgs(script, keys, args)
    } finally {
      this.hostState.ctx = null
      this.hostState.sha = ''
    }
  }

  private runLuaCommand(args: Buffer[], hostState: LuaHostState): ReplyValue {
    const ctx = hostState.ctx
    if (!ctx) {
      throw new Error('ERR Lua runtime is not initialized')
    }

    if (args.length === 0) {
      return new ScriptCallNoCommand().toLuaError()
    }

    const rawCmd = args[0]
    const cmdName = rawCmd.toString().toLowerCase()
    const command = ctx.luaCommands?.[cmdName]

    if (!command) {
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
}

export async function createLuaRuntime(): Promise<LuaRuntime> {
  const module = await load()
  return new LuaRuntime(module)
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
