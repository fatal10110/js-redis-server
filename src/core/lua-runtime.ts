import {
  load,
  type CompatProfile,
  type LoadOptions,
  type LuaEngine,
  type LuaWasmModule,
  type ReplyValue,
} from 'lua-redis-wasm'
import type { CompatibilityProfile } from './compatibility/profile'
import { containerSubcommandExists } from './compatibility/subcommand-gates'
import { asciiLowerCase } from './ascii-case'
import type { CommandDefinition, CommandPlan } from './command-definition'
import { failsTableArity, lookupTableArity } from './command-arity'
import { formatRedisDouble, type DoubleFormatProfile } from './double-format'
import {
  errorReplyBytes,
  RedisCommandError,
  UnknownRedisCommandError,
  UnknownSubcommandError,
  errors,
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
  // The last error a `redis.call` command handed back to the engine during
  // the running eval. A failing redis.call raises it, so when the script aborts
  // with this exact error the abort came from the command rather than from Lua
  // itself — a distinction the engine's reply does not carry, but the pre-7.0
  // abort decoration depends on (see renderScriptError). Cleared when the eval
  // ends.
  private lastRedisCallError: { err: Buffer; code?: Buffer } | null = null
  // The last script-level rejection a `redis.call` got during the running
  // eval (see scriptRejection). Cleared when the eval ends.
  private lastScriptRejection: Buffer | null = null

  constructor(module: LuaWasmModule) {
    this.engine = module.create({
      redisCall: args => this.recordRedisCallError(this.runRedisCommand(args)),
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
    return this.evalScript(script, keys, args, ctx, options).reply
  }

  /**
   * Runs a script like `eval`, and also reports what raised the reply when it
   * is a script abort: a command's error through `redis.call`
   * (`raisedByRedisCall`), or a script-level rejection such as an unknown or
   * not-allowed command (`raisedByScriptRejection`). Neither is set for a Lua
   * runtime error or an engine error.
   */
  evalScript(
    script: Buffer,
    keys: readonly Buffer[],
    args: readonly Buffer[],
    ctx: RedisExecutionContext,
    options?: { readOnly?: boolean },
  ): {
    reply: ReplyValue
    raisedByRedisCall: boolean
    raisedByScriptRejection: boolean
  } {
    if (this.hostState.ctx) {
      throw new RedisCommandError('Lua runtime is already executing a script')
    }

    this.hostState.ctx = ctx
    this.hostState.readOnly = options?.readOnly ?? false
    this.hostState.resp = 2
    this.lastRedisCallError = null
    this.lastScriptRejection = null

    try {
      const reply = this.engine.evalWithArgs(script, [...keys], [...args])
      return {
        reply,
        raisedByRedisCall: this.isRedisCallAbort(reply),
        raisedByScriptRejection: this.isScriptRejectionAbort(reply),
      }
    } finally {
      this.hostState.ctx = null
      this.hostState.readOnly = false
      this.hostState.resp = 2
      this.lastRedisCallError = null
      this.lastScriptRejection = null
    }
  }

  private isRedisCallAbort(reply: ReplyValue): boolean {
    const callError = this.lastRedisCallError
    if (!callError || !isErrorReply(reply) || !reply.meta) {
      return false
    }
    return (
      reply.err.equals(callError.err) &&
      (reply.code?.toString() ?? '') === (callError.code?.toString() ?? '')
    )
  }

  private recordRedisCallError(reply: ReplyValue): ReplyValue {
    if (!isErrorReply(reply)) {
      return reply
    }
    if (scriptRejections.has(reply)) {
      this.lastScriptRejection = reply.err
    } else {
      this.lastRedisCallError = { err: reply.err, code: reply.code }
    }
    return reply
  }

  /**
   * Whether the reply is a script abort raised by a `redis.call` rejection,
   * which {@link renderScriptError} gives 6.2's inner position. Matched on the
   * message alone, since the engine may add a default code to an abort that
   * had none.
   */
  private isScriptRejectionAbort(reply: ReplyValue): boolean {
    const rejection = this.lastScriptRejection
    return (
      rejection !== null &&
      isErrorReply(reply) &&
      reply.meta !== undefined &&
      !reply.meta.kind &&
      reply.err.equals(rejection)
    )
  }

  // Host callback for redis.call()/redis.pcall(). Both modes share the same
  // dispatch: the engine decides whether an error aborts the script (call) or is
  // returned as a value (pcall) and decorates it with the script sha accordingly.
  private runRedisCommand(args: Buffer[]): ReplyValue {
    const ctx = this.hostState.ctx
    if (!ctx) {
      throw new Error('ERR Lua runtime is not initialized')
    }

    // Every profile-dependent choice for a script reads the server's profile,
    // the one the runtime was created with and renderScriptError decorates
    // with. The executor resolves its own from the same spec in every builder.
    const profile = ctx.server.profile
    if (args.length === 0) {
      return scriptRejection(
        'no-command',
        errors.scriptCallNoCommand(profile),
        profile,
      )
    }

    // Real Redis checks a script's call in this order: command lookup, the
    // command-table arity, then noscript / read-only, and only then does the
    // command run and check its own arguments. `plan()` folds the first two
    // and the last into one parse, so an error it throws past lookup is held
    // back until the noscript / read-only checks have had their say.
    let plan: CommandPlan | null = null
    let commandError: RedisCommandError | null = null
    try {
      plan = ctx.executor.plan(args[0], args.slice(1))
    } catch (err) {
      // From 7.0 an unknown container subcommand fails the same command
      // lookup as an unknown command (#439). Only `plan()`'s lookup throws it
      // at plan time; a container that rejects its subcommand while running
      // (6.2) returns the error as an ordinary command reply below.
      if (
        err instanceof UnknownRedisCommandError ||
        err instanceof UnknownSubcommandError
      ) {
        return scriptRejection(
          'unknown-command',
          errors.scriptUnknownCommand(profile),
          profile,
        )
      }

      if (!(err instanceof RedisCommandError)) {
        throw err
      }
      commandError = err
    }

    const definition =
      plan?.definition ?? ctx.executor.getCommandDefinition(args[0].toString())
    if (!definition) {
      // plan() got past lookup, so it threw: keep its error.
      return redisErrorToLuaReply(commandError as RedisCommandError)
    }

    // Only a count the command table itself rejects is the scripting layer's
    // arity error, and it comes before noscript / read-only. A count the table
    // accepts but the command refuses (HSET's field/value pairs, an odd MSET)
    // is the command's own error, `ERR` code and all, as when a client sends
    // it.
    const lookup = lookupTableArity(definition, args.slice(1), profile)
    if (failsTableArity(lookup.arity, args.length)) {
      return scriptRejection(
        'wrong-arity',
        errors.scriptWrongArity(profile),
        profile,
      )
    }

    const refusal = noscriptRefusal(definition, args.slice(1), profile)
    if (refusal) {
      return scriptRejection(
        refusal,
        refusal === 'unknown-command'
          ? errors.scriptUnknownCommand(profile)
          : errors.scriptNotAllowedCommand(profile),
        profile,
      )
    }

    // Read-only scripts (EVAL_RO) are 7.0+, so this has no 6.2 wording.
    if (this.hostState.readOnly && definition.flags.includes('write')) {
      return scriptRejection(
        'read-only-write',
        new RedisCommandError(
          'Write commands are not allowed from read-only scripts.',
        ),
        profile,
      )
    }

    if (!plan) {
      // plan() threw, so commandError is set.
      return redisErrorToLuaReply(commandError as RedisCommandError)
    }

    const result = ctx.executor.executePlanSync(plan, createLuaCallContext(ctx))
    return redisValueToLuaReply(
      normalizeScriptCommandValue(result.value),
      this.hostState.resp,
      profile,
    )
  }
}

/**
 * The rejection a script's `redis.call`/`redis.pcall` gets for a `noscript`
 * command, or `null` when the command may run.
 *
 * On Redis 6.2 `noscript` is a property of the whole command, so a flagged
 * container (CLIENT, CONFIG, ACL, SCRIPT) refuses every subcommand, unknown
 * ones included. From 7.0 a script resolves `container|subcommand` through
 * the command table and the flag lives on each subcommand. An unknown
 * subcommand has already failed that lookup in `CommandExecutor.plan()`
 * (against the *real* table, so a real subcommand this server lacks, like
 * `CLIENT PAUSE`, still gets here and is refused), and no container's HELP
 * carries the flag, so `<container> HELP` runs.
 */
function noscriptRefusal(
  definition: CommandDefinition<unknown>,
  rawArgs: readonly Buffer[],
  profile: CompatibilityProfile,
): 'unknown-command' | 'not-allowed' | null {
  if (!definition.flags.includes('noscript')) {
    return null
  }

  if (!profile.has('script.per-subcommand-noscript')) {
    // 6.2 has no QUIT table entry, so its lookup fails before any flag check.
    if (
      definition.name === 'quit' &&
      !profile.has('command.quit-table-entry')
    ) {
      return 'unknown-command'
    }
    return 'not-allowed'
  }

  if (rawArgs.length === 0) {
    return 'not-allowed'
  }

  // Only a container has a HELP subcommand: `SUBSCRIBE help` is a channel.
  const subcommand = rawArgs[0]
  const isContainerHelp =
    asciiLowerCase(subcommand.toString()) === 'help' &&
    containerSubcommandExists(definition.name, subcommand, profile) === true
  return isContainerHelp ? null : 'not-allowed'
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
 * user-facing prose, so the host owns the wording and the decoration:
 * `<error> script: <sha>, on @user_script:<line>.` from Redis 7.0 / Valkey 7.2,
 * `Error running script (call to f_<sha>): @user_script:<line>: <error>` before
 * that (`script.abort-error-suffix`).
 *
 * Errors carrying their own message (Lua runtime errors, propagated command
 * errors, and global writes — which Lua's native readonly table rejects with
 * "Attempt to modify a readonly table") have no kind and pass through. Replies
 * without metadata (returned error tables, redis.error_reply, host-side limit
 * errors) are emitted verbatim, matching real Redis.
 */
export function renderScriptError(
  value: ReplyValue,
  options: {
    /** Picks the decoration (`script.abort-error-suffix`). */
    profile: CompatibilityProfile
    /** The abort is a command's error raised through redis.call (RedisLuaRuntime.evalScript). */
    raisedByRedisCall: boolean
    /**
     * The abort is a script-level rejection of a redis.call (unknown command,
     * not allowed, wrong arity, no command; RedisLuaRuntime.evalScript).
     */
    raisedByScriptRejection?: boolean
  },
): ReplyValue {
  if (!isErrorReply(value)) {
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
      body = Buffer.from(errors.scriptArgumentType(options.profile).message)
      break
    default:
      // Kept as bytes: a propagated command error or a Lua runtime error can
      // carry raw client bytes (`error(ARGV[1])`, a nested unknown-subcommand
      // echo), and a UTF-8 round trip would turn them into U+FFFD.
      body = value.err
  }

  if (!options.profile.has('script.abort-error-suffix')) {
    // 6.2 raises script-level rejections through `luaPushError`, which adds
    // its own `@user_script: <line>: ` inside the abort decoration.
    const rejection =
      options.raisedByScriptRejection === true || kind === 'command-arg-type'
    return {
      err: Buffer.concat([
        Buffer.from(
          `Error running script (call to f_${sha}): @user_script:${line}: `,
        ),
        rejection
          ? Buffer.concat([
              Buffer.from(`@user_script: ${line}: `),
              kind === 'command-arg-type'
                ? Buffer.from(
                    'Lua redis() command arguments must be strings or integers',
                  )
                : body,
            ])
          : legacyScriptErrorBody(body, value.code, options.raisedByRedisCall),
      ]),
      code: Buffer.from('ERR'),
    }
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
 * The pre-7.0 abort body is the raw Lua error string: a failing redis.call
 * raises its whole `<CODE> <message>` reply, so the code is folded back in
 * (the reply itself is always `-ERR`). A Lua runtime error keeps its text as
 * is; the engine reports those under a default `ERR` code, so only a code it
 * split off the message itself (`error('WRONGTYPE x', 0)`) is restored.
 * Script-level rejections (unknown / not-allowed command, wrong arity, ...)
 * are not command replies; {@link renderScriptError} renders those itself.
 */
function legacyScriptErrorBody(
  body: Buffer,
  code: Buffer | undefined,
  raisedByRedisCall: boolean,
): Buffer {
  if (!code || (!raisedByRedisCall && code.toString() === 'ERR')) {
    return body
  }
  return Buffer.concat([code, Buffer.from(' '), body])
}

function isErrorReply(
  value: ReplyValue,
): value is Extract<ReplyValue, { err: Buffer }> {
  return (
    value !== null &&
    typeof value === 'object' &&
    !Array.isArray(value) &&
    !Buffer.isBuffer(value) &&
    'err' in value
  )
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

// Errors the scripting layer itself raises before a command runs (no command
// given, unknown command, wrong arity, not allowed from scripts, write from a
// read-only script). Real Redis 6.2 renders these through `luaPushError`: an
// inner `@user_script: <line>: ` position, no error code, and 6.2's own
// wording, not a command's `<CODE> <message>` reply. So they are kept out of
// RedisLuaRuntime.lastRedisCallError and never have a code folded in.
const scriptRejections = new WeakSet<object>()

type ScriptRejectionKind =
  | 'no-command'
  | 'unknown-command'
  | 'not-allowed'
  | 'wrong-arity'
  | 'read-only-write'

/**
 * Redis 6.2's wording for each rejection, verified byte for byte against
 * redis-server 6.2.24 (`redis.pcall` gets the same text; the no-command one
 * names `redis.call()` either way). Read-only scripts are 7.0+, so that
 * rejection has no 6.2 form.
 */
const LEGACY_SCRIPT_REJECTIONS: Record<
  Exclude<ScriptRejectionKind, 'read-only-write'>,
  string
> = {
  'no-command': 'Please specify at least one argument for redis.call()',
  'unknown-command': 'Unknown Redis command called from Lua script',
  'not-allowed': 'This Redis command is not allowed from scripts',
  'wrong-arity': 'Wrong number of args calling Redis command From Lua script',
}

/**
 * A rejection reply, in the profile's wording. On 6.2 it has no code. It also
 * lacks the inner `@user_script: <line>: ` position, because the engine does
 * not tell the host which line made the call. A `redis.call` rejection
 * aborts the script, and the abort carries the line, so
 * {@link renderScriptError} adds the position there. A `redis.pcall`
 * rejection is returned to the script without it (a known gap).
 */
function scriptRejection(
  kind: ScriptRejectionKind,
  current: RedisCommandError,
  profile: CompatibilityProfile,
): ReplyValue {
  const reply =
    kind !== 'read-only-write' && !profile.has('script.abort-error-suffix')
      ? { err: Buffer.from(LEGACY_SCRIPT_REJECTIONS[kind]) }
      : redisErrorToLuaReply(current)
  scriptRejections.add(reply as object)
  return reply
}

function redisErrorToLuaReply(err: RedisCommandError): ReplyValue {
  return {
    err: errorReplyBytes(err),
    code: Buffer.from(err.code),
  }
}
