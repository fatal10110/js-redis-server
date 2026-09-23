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
import { formatRedisDouble, type DoubleFormatProfile } from './double-format'
import {
  errorReplyBytes,
  RedisCommandError,
  ScriptCallNoCommandError,
  ScriptNotAllowedCommandError,
  ScriptUnknownCommandError,
  UnknownRedisCommandError,
} from './redis-error'
import type { RedisExecutionContext } from './redis-context'
import { RedisValue } from './redis-value'
import type { RespVersion } from './resp-encoder'

type LuaHostState = {
  ctx: RedisExecutionContext | null
  readOnly: boolean
  // The protocol the running script selected with `redis.setresp()`. It picks
  // the shape `redis.call` replies take in Lua — not the client's protocol.
  resp: RespVersion
}

export type LuaReplyValue = ReplyValue

export class RedisLuaRuntime {
  private readonly hostState: LuaHostState = {
    ctx: null,
    readOnly: false,
    resp: 2,
  }
  private readonly engine: LuaEngine

  constructor(module: LuaWasmModule) {
    this.engine = module.create({
      redisCall: args => this.runRedisCommand(args),
      redisPcall: args => this.runRedisCommand(args),
      log: () => {},
      onSetResp: version => {
        this.hostState.resp = version
      },
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
    this.hostState.resp = 2

    try {
      return this.engine.evalWithArgs(script, [...keys], [...args])
    } finally {
      this.hostState.ctx = null
      this.hostState.readOnly = false
      this.hostState.resp = 2
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

    if (plan.definition.flags.includes('noscript')) {
      return redisErrorToLuaReply(new ScriptNotAllowedCommandError())
    }

    if (this.hostState.readOnly && plan.definition.flags.includes('write')) {
      return redisErrorToLuaReply(
        new RedisCommandError(
          'Write commands are not allowed from read-only scripts.',
        ),
      )
    }

    const result = ctx.executor.executePlanSync(plan, createLuaCallContext(ctx))
    return redisValueToLuaReply(
      normalizeScriptCommandValue(result.value),
      this.hostState.resp,
      ctx.server.profile,
    )
  }
}

/**
 * The execution context every `redis.call`/`redis.pcall` runs under: the
 * caller's context with the script's own monitor sink and the `inScript`
 * flag, which commands whose reply must be reproducible branch on.
 */
function createLuaCallContext(
  ctx: RedisExecutionContext,
): RedisExecutionContext {
  return {
    get db() {
      return ctx.db
    },
    server: ctx.server,
    session: ctx.session,
    executor: ctx.executor,
    inScript: true,
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
    // `value.err` is already bytes; decoding it here would undo the
    // byte-exact echo a command like `CONFIG <raw bytes>` produced.
    return RedisValue.error(value.err, value.code?.toString())
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
  let body: Buffer
  switch (kind) {
    case 'global-read':
      body = Buffer.from(
        `user_script:${line}: Script attempted to access nonexistent global variable '${name}'`,
      )
      break
    case 'command-arg-type':
      // Raised by redis.call/pcall without a script-position prefix.
      body = Buffer.from(
        'Lua redis lib command arguments must be strings or integers',
      )
      break
    default:
      // Kept as bytes: a propagated command error or a Lua runtime error can
      // carry raw client bytes (`error(ARGV[1])`, a nested unknown-subcommand
      // echo), and a UTF-8 round trip would turn them into U+FFFD.
      body = value.err
  }

  return {
    err: Buffer.concat([
      body,
      Buffer.from(` script: ${sha}, on @user_script:${line}.`),
    ]),
    code: value.code,
  }
}

/**
 * Convert a `redis.call`/`redis.pcall` reply into the value the script sees.
 * Like real Redis, the shape follows the protocol the script selected with
 * `redis.setresp()`: at RESP2 every reply is flattened to what a RESP2 client
 * would read, while at RESP3 maps and doubles reach Lua as their typed tables
 * (`{map=…}`, `{double=…}`) — the shapes `encodeResp3` writes to the wire,
 * except null (see {@link resp3TypedLuaReply}).
 *
 * At RESP2 a double reaches Lua as the text the served profile spells it
 * with (#451); without a profile, the default profile's spelling.
 *
 * Exported for unit tests only.
 */
export function redisValueToLuaReply(
  value: RedisValue,
  resp: RespVersion,
  profile?: DoubleFormatProfile,
): ReplyValue {
  const toLua = (item: RedisValue) => redisValueToLuaReply(item, resp, profile)
  if (resp === 3) {
    const typed = resp3TypedLuaReply(value, toLua)
    if (typed !== undefined) {
      return typed
    }
  }

  switch (value.kind) {
    case 'simple-string':
      return { ok: Buffer.from(value.value) }
    case 'bulk-string':
      return value.value
    case 'integer':
      return value.value
    case 'double':
      return Buffer.from(value.text ?? formatRedisDouble(value.value, profile))
    case 'boolean':
      return value.value ? 1 : 0
    case 'big-number':
      return Buffer.from(value.value.toString())
    case 'verbatim':
      return value.value
    case 'array':
      return value.items.map(toLua)
    case 'set':
      return value.items.map(toLua)
    case 'map':
      return value.entries.flatMap(([key, entryValue]) => [
        toLua(key),
        toLua(entryValue),
      ])
    case 'map-pairs':
      return value.entries.map(([key, entryValue]) => [
        toLua(key),
        toLua(entryValue),
      ])
    case 'flat-pairs':
      // At RESP2 a WITHSCORES reply is a flat array to scripts.
      return value.entries.flatMap(([key, entryValue]) => [
        toLua(key),
        toLua(entryValue),
      ])
    case 'push':
      return [Buffer.from(value.name), ...value.items.map(toLua)]
    case 'null':
    case 'null-array':
      return null
    case 'error':
      return {
        err: value.messageBytes ?? Buffer.from(value.message),
        code: value.code ? Buffer.from(value.code) : undefined,
      }
  }
}

/**
 * The RESP3 reply kinds whose Lua shape differs from RESP2, mirroring
 * `encodeResp3`; `undefined` for every kind both protocols convert alike. (A
 * RESP3 null still reaches Lua as `false`, not `nil`: the engine decodes every
 * null that way.)
 *
 * Through `redis.call` only `double`, `map`, `map-pairs` and `flat-pairs` are
 * reachable today. No command emits `set`, `boolean`, `big-number` or
 * `verbatim` yet (real Redis sends SMEMBERS as a set and INFO as a verbatim
 * string), so those branches just mirror `encodeResp3` for when one does.
 */
function resp3TypedLuaReply(
  value: RedisValue,
  toLua: (item: RedisValue) => ReplyValue,
): ReplyValue | undefined {
  switch (value.kind) {
    case 'double':
      return { double: value.value }
    case 'boolean':
      return value.value
    case 'big-number':
      return { big_number: Buffer.from(value.value.toString()) }
    case 'verbatim':
      return {
        verbatim_string: {
          format: Buffer.from(value.format),
          string: value.value,
        },
      }
    case 'set':
      return { set: value.items.map(toLua) }
    case 'map':
    case 'map-pairs':
      return {
        map: value.entries.map(([key, entryValue]) => [
          toLua(key),
          toLua(entryValue),
        ]),
      }
    case 'flat-pairs':
      return value.entries.map(([key, entryValue]) => [
        toLua(key),
        toLua(entryValue),
      ])
    default:
      return undefined
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
    err: errorReplyBytes(err),
    code: Buffer.from(err.code),
  }
}
