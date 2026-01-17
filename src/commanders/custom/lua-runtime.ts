import { UnknownScriptCommand, UserFacedError } from '../../core/errors'
import { CapturingTransport } from '../../core/transports/capturing-transport'
import { Command, CommandResult } from '../../types'

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

export function createLuaRuntime(module: LuaWasmModule): LuaRuntime {
  // TODO implement
  const hostState: LuaHostState = { ctx: null, sha: '' }
  const host: RedisHost = {
    redisCall: args => runLuaCommand(args, hostState, false),
    redisPcall: args => runLuaCommand(args, hostState, true),
    log: () => {},
  }
  const engine = module.create(host)

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

function runLuaCommand(
  args: Buffer[],
  hostState: LuaHostState,
  isPcall: boolean,
): ReplyValue {
  const ctx = hostState.ctx
  if (!ctx) {
    return handleLuaError(
      new Error('ERR Lua runtime is not initialized'),
      isPcall,
    )
  }

  if (args.length === 0) {
    return handleLuaError(
      new Error("ERR wrong number of arguments for 'redis.call' command"),
      isPcall,
    )
  }

  const rawCmd = args[0]
  const cmdName = rawCmd.toString().toLowerCase()
  const command = ctx.commands?.[cmdName]

  if (!command || !isAllowedInLua(command)) {
    return handleLuaError(new UnknownScriptCommand(hostState.sha), isPcall)
  }

  const capture = new CapturingTransport()
  try {
    command.run(rawCmd, args.slice(1), ctx.signal, capture)
  } catch (err) {
    return handleLuaError(err, isPcall)
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
    const err = normalizeRedisError(result)
    return { err: encodeErrorBuffer(err) }
  }

  return result
}

function handleLuaError(err: unknown, isPcall: boolean): ReplyValue {
  const normalized = normalizeRedisError(err)
  const buffer = encodeErrorBuffer(normalized)

  if (isPcall) {
    return { err: buffer }
  }

  throw normalized
}

function encodeErrorBuffer(err: Error): Buffer {
  const name = err.name && err.name !== 'Error' ? err.name : 'ERR'
  const message = err.message ?? ''
  return Buffer.from(`${name} ${message}`.trim())
}

function normalizeRedisError(err: unknown): Error {
  if (err instanceof UserFacedError) {
    return err
  }

  const base = err instanceof Error ? err : new Error(String(err))
  const message = base.message ?? ''
  const match = message.match(/^([A-Z]+)\\s+(.*)$/)

  if (match) {
    const normalized = new Error(match[2])
    normalized.name = match[1]
    return normalized
  }

  if (!base.name || base.name === 'Error') {
    base.name = 'ERR'
  }

  return base
}
