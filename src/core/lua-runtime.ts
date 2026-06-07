import {
  load,
  type LuaEngine,
  type LuaWasmModule,
  type ReplyValue,
} from 'lua-redis-wasm'
import type { CommandPlan } from './command-definition'
import {
  RedisCommandError,
  ScriptCallNoCommandError,
  ScriptUnknownCommandError,
  UnknownRedisCommandError,
} from './redis-error'
import type { RedisExecutionContext } from './redis-context'
import { RedisValue } from './redis-value'

type LuaHostState = {
  ctx: RedisExecutionContext | null
  sha: string
}

export type LuaReplyValue = ReplyValue

export class RedisLuaRuntime {
  private readonly hostState: LuaHostState = { ctx: null, sha: '' }
  private readonly engine: LuaEngine

  constructor(module: LuaWasmModule) {
    this.engine = module.create({
      redisCall: args => this.runRedisCommand(args),
      redisPcall: args => this.runRedisCommand(args),
      log: () => {},
    })
  }

  eval(
    script: Buffer,
    keys: readonly Buffer[],
    args: readonly Buffer[],
    ctx: RedisExecutionContext,
    sha: string,
  ): ReplyValue {
    if (this.hostState.ctx) {
      throw new RedisCommandError('Lua runtime is already executing a script')
    }

    this.hostState.ctx = ctx
    this.hostState.sha = sha

    try {
      return this.engine.evalWithArgs(script, [...keys], [...args])
    } finally {
      this.hostState.ctx = null
      this.hostState.sha = ''
    }
  }

  private runRedisCommand(args: Buffer[]): ReplyValue {
    const ctx = this.hostState.ctx
    if (!ctx) {
      throw new Error('ERR Lua runtime is not initialized')
    }

    if (args.length === 0) {
      return redisErrorToLuaReply(new ScriptCallNoCommandError())
    }

    let plan: CommandPlan
    try {
      plan = ctx.executor.plan(args[0], args.slice(1))
    } catch (err) {
      if (err instanceof UnknownRedisCommandError) {
        return redisErrorToLuaReply(
          new ScriptUnknownCommandError(this.hostState.sha),
        )
      }

      if (err instanceof RedisCommandError) {
        return redisErrorToLuaReply(err)
      }

      throw err
    }

    if (plan.flags.includes('noscript')) {
      return redisErrorToLuaReply(
        new ScriptUnknownCommandError(this.hostState.sha),
      )
    }

    const result = ctx.executor.executePlanSync(plan, ctx)
    return redisValueToLuaReply(result.value)
  }
}

let defaultRuntimePromise: Promise<RedisLuaRuntime> | null = null

export async function getDefaultRedisLuaRuntime(): Promise<RedisLuaRuntime> {
  defaultRuntimePromise ??= createRedisLuaRuntime()
  return defaultRuntimePromise
}

export async function createRedisLuaRuntime(): Promise<RedisLuaRuntime> {
  const module = await load()
  return new RedisLuaRuntime(module)
}

export function luaReplyToRedisValue(value: ReplyValue): RedisValue {
  if (value === null || value === undefined) {
    return RedisValue.null()
  }

  if (typeof value === 'number' || typeof value === 'bigint') {
    return RedisValue.integer(value)
  }

  if (Buffer.isBuffer(value)) {
    return RedisValue.bulkString(value)
  }

  if (Array.isArray(value)) {
    return RedisValue.array(value.map(luaReplyToRedisValue))
  }

  if ('ok' in value) {
    return RedisValue.simpleString(value.ok.toString())
  }

  if ('err' in value) {
    return luaErrorToRedisValue(value.err.toString())
  }

  return RedisValue.bulkString(Buffer.from(String(value)))
}

function redisValueToLuaReply(value: RedisValue): ReplyValue {
  switch (value.kind) {
    case 'simple-string':
      return { ok: Buffer.from(value.value) }
    case 'bulk-string':
      return value.value
    case 'integer':
      return value.value
    case 'double':
      return Buffer.from(formatNumber(value.value))
    case 'boolean':
      return value.value ? 1 : 0
    case 'big-number':
      return Buffer.from(value.value.toString())
    case 'verbatim':
      return value.value
    case 'array':
      return value.items.map(redisValueToLuaReply)
    case 'set':
      return value.items.map(redisValueToLuaReply)
    case 'map':
      return value.entries.flatMap(([key, entryValue]) => [
        redisValueToLuaReply(key),
        redisValueToLuaReply(entryValue),
      ])
    case 'push':
      return [Buffer.from(value.name), ...value.items.map(redisValueToLuaReply)]
    case 'null':
    case 'null-array':
      return null
    case 'error':
      return {
        err: Buffer.from(formatRedisErrorValue(value)),
      }
  }
}

function redisErrorToLuaReply(err: RedisCommandError): ReplyValue {
  return {
    err: Buffer.from(formatRedisError(err.message, err.code)),
  }
}

function luaErrorToRedisValue(message: string): RedisValue {
  const match = /^([A-Z][A-Z0-9]*) (.+)$/.exec(message)
  if (!match) {
    return RedisValue.error(message, 'ERR')
  }

  return RedisValue.error(match[2], match[1])
}

function formatRedisErrorValue(
  value: Extract<RedisValue, { kind: 'error' }>,
): string {
  return formatRedisError(value.message, value.code)
}

function formatRedisError(message: string, code?: string): string {
  const sanitizedMessage = sanitizeErrorText(message)
  if (!code) {
    return sanitizedMessage
  }

  const sanitizedCode = sanitizeErrorText(code)
  if (sanitizedMessage.startsWith(`${sanitizedCode} `)) {
    return sanitizedMessage
  }

  return `${sanitizedCode} ${sanitizedMessage}`
}

function formatNumber(value: number): string {
  if (Number.isNaN(value)) {
    return 'nan'
  }

  if (value === Infinity) {
    return 'inf'
  }

  if (value === -Infinity) {
    return '-inf'
  }

  if (Object.is(value, -0)) {
    return '-0'
  }

  return value.toString()
}

function sanitizeErrorText(value: string): string {
  return value.replace(/[\r\n]+/g, ' ')
}
