import {
  load,
  type CompatProfile,
  type LoadOptions,
  type LuaEngine,
  type LuaWasmModule,
  type ReplyValue,
} from 'lua-redis-wasm'
import type { CompatibilityProfile } from './compatibility/profile'
import type { CommandPlan } from './command-definition'
import {
  RedisCommandError,
  ScriptCallNoCommandError,
  ScriptNotAllowedCommandError,
  ScriptUnknownCommandError,
  UnknownRedisCommandError,
} from './redis-error'
import type { RedisExecutionContext } from './redis-context'
import { RedisValue } from './redis-value'

type LuaHostState = {
  ctx: RedisExecutionContext | null
  readOnly: boolean
}

export type LuaReplyValue = ReplyValue

export class RedisLuaRuntime {
  private readonly hostState: LuaHostState = {
    ctx: null,
    readOnly: false,
  }
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
    options?: { readOnly?: boolean },
  ): ReplyValue {
    if (this.hostState.ctx) {
      throw new RedisCommandError('Lua runtime is already executing a script')
    }

    this.hostState.ctx = ctx
    this.hostState.readOnly = options?.readOnly ?? false

    try {
      return this.engine.evalWithArgs(script, [...keys], [...args])
    } finally {
      this.hostState.ctx = null
      this.hostState.readOnly = false
    }
  }

  // Host callback for redis.call()/redis.pcall(). Both modes share the same
  // dispatch: the engine decides whether an error aborts the script (call) or is
  // returned as a value (pcall) and decorates it with the script sha accordingly.
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
        return redisErrorToLuaReply(new ScriptUnknownCommandError())
      }

      if (err instanceof RedisCommandError) {
        return redisErrorToLuaReply(err)
      }

      throw err
    }

    if (plan.flags.includes('noscript')) {
      return redisErrorToLuaReply(new ScriptNotAllowedCommandError())
    }

    if (this.hostState.readOnly && plan.flags.includes('write')) {
      return redisErrorToLuaReply(
        new RedisCommandError(
          'Write commands are not allowed from read-only scripts.',
        ),
      )
    }

    const result = ctx.executor.executePlanSync(
      plan,
      createLuaMonitorContext(ctx),
    )
    return redisValueToLuaReply(normalizeScriptCommandValue(result.value))
  }
}

function createLuaMonitorContext(
  ctx: RedisExecutionContext,
): RedisExecutionContext {
  return {
    get db() {
      return ctx.db
    },
    server: ctx.server,
    session: ctx.session,
    executor: ctx.executor,
    ...(ctx.transactionReplay
      ? { transactionReplay: ctx.transactionReplay }
      : {}),
    ...(ctx.nodeRole ? { nodeRole: ctx.nodeRole } : {}),
    monitor: {
      ...ctx.monitor,
      defer: true,
      clientAddress: 'lua',
    },
    signal: ctx.signal,
    park: ctx.park,
  }
}

// Each RedisLuaRuntime gets its own freshly-loaded WASM module + LuaEngine +
// hostState. A LuaWasmModule is single-use (module.create() consumes it), so it
// cannot be shared between engines. Scoping a runtime per RedisServerState (see
// RedisServerState.getLuaRuntime) keeps each logical node's script re-entrancy
// guard isolated, so concurrent EVALs on independent server/cluster nodes never
// collide (issue #130).
let luaWasmLoadOptions: LoadOptions = {}

/**
 * Override where the Lua WASM module + Emscripten glue are loaded from — e.g. a
 * CDN URL (`{ modulePath, wasmPath }`) or preloaded bytes (`{ wasmBytes }`).
 * Affects every runtime created afterwards, so set it once before the first EVAL.
 * Mainly useful in browser bundles that want the `.wasm` served from a CDN
 * instead of emitted as a local asset.
 */
export function setLuaWasmLoadOptions(options: LoadOptions): void {
  luaWasmLoadOptions = options
}

export async function createRedisLuaRuntime(
  profile?: CompatibilityProfile,
): Promise<RedisLuaRuntime> {
  const module = await load({
    ...luaWasmLoadOptions,
    ...(profile ? { profile: toLuaCompatProfile(profile) } : {}),
  })
  return new RedisLuaRuntime(module)
}

/**
 * Map a server compatibility profile to the Lua engine's compat profile. The
 * engine collapses aliases (redis-7.0 == 7.2, redis-7.4 == 8.0, valkey-8.0 ==
 * 9.0), so only the behavioral groups matter — they gate whether the sandbox
 * keeps `print` (6.2 only), exposes `os` (7.4+), and aliases `server` (valkey).
 */
function toLuaCompatProfile(profile: CompatibilityProfile): CompatProfile {
  if (profile.flavor === 'valkey') {
    return 'valkey-8.0'
  }
  const [major, minor] = profile.version.split('.').map(n => parseInt(n, 10))
  if (major < 7) {
    return 'redis-6.2'
  }
  if (major === 7 && minor < 4) {
    return 'redis-7.0'
  }
  return 'redis-8.0'
}

export function luaReplyToRedisValue(value: ReplyValue): RedisValue {
  if (value === null || value === undefined) {
    return RedisValue.null()
  }

  if (typeof value === 'number' || typeof value === 'bigint') {
    return RedisValue.integer(value)
  }

  // RESP3 boolean reply (redis.setresp(3) + a Lua boolean).
  if (typeof value === 'boolean') {
    return RedisValue.boolean(value)
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
    // The engine supplies an explicit code (or none, for verbatim returned error
    // tables); the host does not infer one.
    return RedisValue.error(value.err.toString(), value.code?.toString())
  }

  // RESP3 reply shapes the engine produces under redis.setresp(3).
  if ('double' in value) {
    return RedisValue.double(value.double)
  }

  if ('big_number' in value) {
    return RedisValue.bigNumber(BigInt(value.big_number.toString()))
  }

  if ('verbatim_string' in value) {
    return RedisValue.verbatim(
      value.verbatim_string.format.toString(),
      value.verbatim_string.string,
    )
  }

  if ('map' in value) {
    return RedisValue.map(
      value.map.map(([k, v]) => [
        luaReplyToRedisValue(k),
        luaReplyToRedisValue(v),
      ]),
    )
  }

  if ('set' in value) {
    return RedisValue.set(value.set.map(luaReplyToRedisValue))
  }

  return RedisValue.bulkString(Buffer.from(String(value)))
}

/**
 * Renders a script-aborting error into its final Redis wire message. The engine
 * classifies these errors and attaches metadata — `{ line, sha }` always, plus a
 * machine `kind`/`name` for the errors it originates itself — but composes no
 * user-facing prose, so the host owns the wording and the
 * `... script: <sha>, on @user_script:<line>.` decoration.
 *
 * Errors carrying their own message (Lua runtime errors, propagated command
 * errors, and global writes — which Lua's native readonly table rejects with
 * "Attempt to modify a readonly table") have no kind and pass through. Replies
 * without metadata (returned error tables, redis.error_reply, host-side limit
 * errors) are emitted verbatim, matching real Redis.
 */
export function renderScriptError(value: ReplyValue): ReplyValue {
  if (
    value === null ||
    typeof value !== 'object' ||
    Array.isArray(value) ||
    Buffer.isBuffer(value) ||
    !('err' in value)
  ) {
    return value
  }
  const meta = value.meta
  if (!meta) {
    return value
  }

  const { line, sha, kind, name } = meta
  let body: string
  switch (kind) {
    case 'global-read':
      body = `user_script:${line}: Script attempted to access nonexistent global variable '${name}'`
      break
    case 'command-arg-type':
      // Raised by redis.call/pcall without a script-position prefix.
      body = 'Lua redis lib command arguments must be strings or integers'
      break
    default:
      body = value.err.toString('utf8')
  }

  const message = `${body} script: ${sha}, on @user_script:${line}.`
  return { err: Buffer.from(message, 'utf8'), code: value.code }
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
    case 'map-pairs':
      return value.entries.map(([key, entryValue]) => [
        redisValueToLuaReply(key),
        redisValueToLuaReply(entryValue),
      ])
    case 'flat-pairs':
      // EVAL uses RESP2 semantics — WITHSCORES is a flat array to scripts.
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
        err: Buffer.from(value.message),
        code: value.code ? Buffer.from(value.code) : undefined,
      }
  }
}

// A command run from a script cannot redirect the client, so a MOVED reply is
// surfaced to the script as a generic error instead of a cluster redirect.
function normalizeScriptCommandValue(value: RedisValue): RedisValue {
  if (value.kind === 'error' && value.code === 'MOVED') {
    return RedisValue.error(
      'Script attempted to access a non local key in a cluster node',
      'ERR',
    )
  }

  return value
}

function redisErrorToLuaReply(err: RedisCommandError): ReplyValue {
  return {
    err: Buffer.from(err.message),
    code: Buffer.from(err.code),
  }
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
