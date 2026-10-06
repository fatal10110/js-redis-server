import {
  load,
  WasmFault,
  type CompatProfile,
  type LoadOptions,
  type LuaEngine,
  type LuaWasmModule,
  type RedisCallContext,
  type RedisProps,
  type ReplyError,
  type ReplyValue,
} from 'lua-redis-wasm'
import {
  VALKEY_REDIS_COMPAT_VERSION,
  type CompatibilityProfile,
} from './compatibility/profile'
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

const EMPTY_SCRIPT = Buffer.alloc(0)

export class RedisLuaRuntime {
  private readonly hostState: LuaHostState = {
    ctx: null,
    readOnly: false,
    resp: 2,
  }
  private readonly engine: LuaEngine
  private broken = false

  constructor(module: LuaWasmModule) {
    this.engine = module.create({
      redisCall: (args, call) => this.runRedisCommand(args, call),
      redisPcall: (args, call) => this.runRedisCommand(args, call),
      log: () => {},
      onSetResp: version => {
        this.hostState.resp = version
      },
    })
  }

  /**
   * False once the engine can no longer run scripts. Since lua-redis-wasm
   * 2.0 an exception that escapes the WASM module (a `WasmFault`, an
   * Emscripten abort, a trap) leaves the engine unusable: every later call
   * throws `LuaEngine is unusable: ...`. `RedisServerState.getLuaRuntime()`
   * then replaces this runtime with a fresh one (#539).
   */
  get usable(): boolean {
    return !this.broken
  }

  /**
   * Runs a script. A script-aborting error comes back as the engine reports
   * it, with its `meta`; {@link renderScriptError} turns it into the reply.
   */
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
      return this.guard(() =>
        this.engine.evalWithArgs(script, [...keys], [...args]),
      )
    } finally {
      this.hostState.ctx = null
      this.hostState.readOnly = false
      this.hostState.resp = 2
    }
  }

  /**
   * Compiles a script without running it, for `SCRIPT LOAD`: `null` when it
   * is valid Lua, else the compile error (`meta.kind` `compile`) that `eval`
   * would abort with.
   */
  compile(script: Buffer): ReplyError | null {
    return this.guard(() => this.engine.compile(script))
  }

  /** Releases the engine. The runtime cannot run scripts afterwards. */
  dispose(): void {
    this.broken = true
    try {
      this.engine.dispose()
    } catch {
      // Only a running script makes dispose() throw; the engine is dropped
      // with this runtime either way.
    }
  }

  /**
   * Runs an engine call and, when it throws, finds out whether the engine
   * survived. A `WasmFault` always leaves it unusable. Any other throw (a
   * trap rethrown as is, the heap-size `RangeError`, which leaves it usable)
   * is told apart by asking the engine for an empty compile, which throws
   * once the engine is unusable.
   */
  private guard<T>(call: () => T): T {
    try {
      return call()
    } catch (err) {
      if (err instanceof WasmFault || !this.engineResponds()) {
        this.broken = true
      }
      throw err
    }
  }

  private engineResponds(): boolean {
    try {
      this.engine.compile(EMPTY_SCRIPT)
      return true
    } catch {
      return false
    }
  }

  // Host callback for redis.call()/redis.pcall(). Both modes share the same
  // dispatch: the engine decides whether an error aborts the script (call) or is
  // returned as a value (pcall) and decorates it with the script sha accordingly.
  // `call` is the calling Lua frame, which 6.2's rejections name.
  private runRedisCommand(
    args: Buffer[],
    call: RedisCallContext | undefined,
  ): ReplyValue {
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
        call,
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
          call,
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
        call,
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
        call,
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
        call,
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

// Each RedisLuaRuntime gets its own WASM instance + LuaEngine + hostState
// (lua-redis-wasm compiles the WebAssembly module once per process; every
// load() after the first only instantiates it). A LuaWasmModule is single-use
// (module.create() consumes it), so it cannot be shared between engines.
// Scoping a runtime per RedisServerState (see RedisServerState.getLuaRuntime)
// keeps each logical node's script re-entrancy guard isolated, so concurrent
// EVALs on independent server/cluster nodes never collide (issue #130).
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
    ...(profile ? toLuaCompat(profile) : {}),
    redisProps: {
      ...luaRedisProps(profile),
      ...luaWasmLoadOptions.redisProps,
    },
  })
  return new RedisLuaRuntime(module)
}

/**
 * Map a server compatibility profile to the closest Lua engine profile. It
 * picks the sandbox (`print` on 6.2 only, `os` from 7.4, the `server` alias
 * on Valkey), the error model (6.2's string errors) and the `redis.log` and
 * argument-type wording. Valkey 7.2 is a fork of Redis 7.2.4 and its scripts
 * behave like 7.2's (no `os`, `Invalid debug level.`, `redis.log()` and
 * `Lua redis lib ...` wording) except that it already has the `server` alias
 * (checked against valkey-server 7.2.14).
 */
function toLuaCompat(
  profile: CompatibilityProfile,
): Pick<LoadOptions, 'profile' | 'compat'> {
  const [major, minor] = profile.version.split('.').map(n => parseInt(n, 10))
  if (profile.flavor === 'valkey') {
    if (major < 8) {
      return {
        profile: 'redis-7.2',
        compat: { serverAlias: true, ...luaWasmLoadOptions.compat },
      }
    }
    return { profile: major < 9 ? 'valkey-8.0' : 'valkey-9.0' }
  }
  return { profile: toRedisLuaProfile(major, minor) }
}

function toRedisLuaProfile(major: number, minor: number): CompatProfile {
  if (major < 7) {
    return 'redis-6.2'
  }
  if (major === 7) {
    return minor < 2 ? 'redis-7.0' : minor < 4 ? 'redis-7.2' : 'redis-7.4'
  }
  return 'redis-8.0'
}

/**
 * The version-specific `redis.*` members the engine leaves to the host. Every
 * version has the replication constants and `set_repl` /
 * `replicate_commands`, which only matter to a replica (a no-op and `true`
 * since 7.0, where scripts always replicate their effects), and the debugger
 * hooks, which do nothing outside a `SCRIPT DEBUG` session. 7.0 added
 * `REDIS_VERSION` / `REDIS_VERSION_NUM`; Valkey reports the Redis version it
 * forked from there (7.2.4, as its INFO does) and its own under
 * `VALKEY_VERSION` / `VALKEY_VERSION_NUM`, with `SERVER_NAME`. Values checked
 * against redis-server 6.2.24, 7.0.15, 7.2.16, 7.4.11, 8.0.6 and
 * valkey-server 7.2.14, 8.0.11, 9.0.6.
 *
 * The stubs ignore their arguments, so `set_repl`'s own argument errors are
 * not reproduced.
 */
function luaRedisProps(profile?: CompatibilityProfile): RedisProps {
  const props: RedisProps = {
    REPL_NONE: { value: 0 },
    REPL_AOF: { value: 1 },
    REPL_SLAVE: { value: 2 },
    REPL_REPLICA: { value: 2 },
    REPL_ALL: { value: 3 },
    set_repl: { returns: null },
    replicate_commands: { returns: true },
    breakpoint: { returns: false },
    debug: { returns: null },
  }
  if (!profile?.has('script.redis-version-props')) {
    return props
  }

  const redisVersion =
    profile.flavor === 'valkey' ? VALKEY_REDIS_COMPAT_VERSION : profile.version
  props.REDIS_VERSION = { value: redisVersion }
  props.REDIS_VERSION_NUM = { value: luaVersionNum(redisVersion) }
  if (profile.flavor === 'valkey') {
    props.SERVER_NAME = { value: 'valkey' }
    props.VALKEY_VERSION = { value: profile.version }
    props.VALKEY_VERSION_NUM = { value: luaVersionNum(profile.version) }
  }
  return props
}

/** The version as Redis encodes `REDIS_VERSION_NUM`: one byte per part. */
function luaVersionNum(version: string): number {
  const [major = 0, minor = 0, patch = 0] = version
    .split('.')
    .map(n => parseInt(n, 10))
  return major * 0x10000 + minor * 0x100 + patch
}

/**
 * Converts a script's return value (or a rendered script error) to a reply.
 * `profile` is the server's: before 7.0 a returned `{big_number=}` or
 * `{verbatim_string=}` table is not a typed reply but an ordinary table with
 * no array part, which Redis 6.2 sends as an empty array, at any depth
 * (`script.big-number-verbatim-returns`). The engine converts those tables on
 * every profile and drops the rest of the table, so a 6.2 table that also has
 * an array part or a `set`/`map` field is not reproduced. Without a profile
 * the 7.0+ conversion applies.
 */
export function luaReplyToRedisValue(
  value: ReplyValue,
  profile?: CompatibilityProfile,
): RedisValue {
  const convert = (inner: ReplyValue) => luaReplyToRedisValue(inner, profile)
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
    return RedisValue.array(value.map(convert))
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

  if (
    ('big_number' in value || 'verbatim_string' in value) &&
    profile &&
    !profile.has('script.big-number-verbatim-returns')
  ) {
    return RedisValue.array([])
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
    return RedisValue.map(value.map.map(([k, v]) => [convert(k), convert(v)]))
  }

  if ('set' in value) {
    return RedisValue.set(value.set.map(convert))
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
 * that (`script.abort-error-suffix`), always with the `ERR` code.
 *
 * A script that does not compile never ran, so it has neither decoration:
 * every version replies `-ERR Error compiling script (new function): <Lua's
 * message>`.
 *
 * Errors carrying their own message (Lua runtime errors, propagated command
 * errors, script-level rejections of a redis.call, and global writes — which
 * Lua's native readonly table rejects with "Attempt to modify a readonly
 * table") have no kind and pass through. On 6.2 the engine hands over the
 * whole Lua error string, a failing command's `<CODE> ` included, which is
 * the body 6.2 decorates. Replies without metadata (returned error tables,
 * redis.error_reply, host-side limit errors) are emitted verbatim, matching
 * real Redis.
 */
export function renderScriptError(
  value: ReplyValue,
  options: {
    /** Picks the decoration (`script.abort-error-suffix`). */
    profile: CompatibilityProfile
    /**
     * The SHA to name, when the engine ran a rewritten body: a shebang
     * script runs with its shebang line blanked, but Redis names the SHA of
     * the script the client sent.
     */
    sha?: string
  },
): ReplyValue {
  if (!isErrorReply(value)) {
    return value
  }
  const meta = value.meta
  if (!meta) {
    return value
  }

  const { line, kind, name } = meta
  const sha = options.sha ?? meta.sha
  if (kind === 'compile') {
    return {
      err: Buffer.concat([
        Buffer.from('Error compiling script (new function): '),
        value.err,
      ]),
      code: Buffer.from('ERR'),
    }
  }

  const legacy = !options.profile.has('script.abort-error-suffix')
  let body: Buffer
  switch (kind) {
    case 'global-read':
      body = Buffer.from(
        `user_script:${line}: Script attempted to access nonexistent global variable '${name}'`,
      )
      break
    case 'command-arg-type':
      // Raised by redis.call without a script-position prefix, except on 6.2,
      // where `luaPushError` adds the calling line.
      body = Buffer.from(
        legacy
          ? `@user_script: ${line}: ${LEGACY_ARGUMENT_TYPE}`
          : errors.scriptArgumentType(options.profile).message,
      )
      break
    default:
      // Kept as bytes: a propagated command error or a Lua runtime error can
      // carry raw client bytes (`error(ARGV[1])`, a nested unknown-subcommand
      // echo), and a UTF-8 round trip would turn them into U+FFFD.
      body = value.err
  }

  if (legacy) {
    return {
      err: Buffer.concat([
        Buffer.from(
          `Error running script (call to f_${sha}): @user_script:${line}: `,
        ),
        body,
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

/** Whether `eval` aborted because the script does not compile. */
export function isCompileError(value: ReplyValue): boolean {
  return isErrorReply(value) && value.meta?.kind === 'compile'
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
 * null reaches Lua as `nil` at either protocol, as in Redis.)
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
// read-only script).
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

/** Redis 6.2's wording for a redis.call argument that is not a string or number. */
const LEGACY_ARGUMENT_TYPE =
  'Lua redis() command arguments must be strings or integers'

/**
 * A rejection reply, in the profile's wording. Real Redis 6.2 builds these
 * with `luaPushError`: no error code, and the calling frame's position in
 * front, `<source>: <line>: ` (`@user_script: 2: ` for a call on the script's
 * second line, `=[C]: -1: ` for `pcall(redis.call, ...)`), for redis.call and
 * redis.pcall alike. The engine names that frame in `call`; without one the
 * position is left out, as Redis does when it cannot find the frame.
 */
function scriptRejection(
  kind: ScriptRejectionKind,
  current: RedisCommandError,
  profile: CompatibilityProfile,
  call: RedisCallContext | undefined,
): ReplyValue {
  if (kind === 'read-only-write' || profile.has('script.abort-error-suffix')) {
    return redisErrorToLuaReply(current)
  }

  const message = Buffer.from(LEGACY_SCRIPT_REJECTIONS[kind])
  const source = call?.source
  return {
    err:
      source && source.length > 0
        ? Buffer.concat([source, Buffer.from(`: ${call.line}: `), message])
        : message,
  }
}

function redisErrorToLuaReply(err: RedisCommandError): ReplyValue {
  return {
    err: errorReplyBytes(err),
    code: Buffer.from(err.code),
  }
}
