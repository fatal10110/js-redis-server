import { asciiLowerCase, asciiUpperCase } from './ascii-case'
import { CommandDefinition, CommandPlan } from './command-definition'
import { CommandRegistry } from './command-registry'
import { failsTableArity, lookupTableArity } from './command-arity'
import { parseCommandArgs } from './command-schema'
import { rawCommandKeys } from './key-specs'
import { announceKeyMisses } from './keyspace-read-lookups'
import type { ExecutionPolicy } from './execution-policies'
import {
  ExecCommandAbortError,
  RedisCommandError,
  UnknownRedisCommandError,
  WrongNumberOfArgumentsError,
} from './redis-error'
import type { RedisExecutionContext } from './redis-context'
import { RedisResult } from './redis-result'
import type { RedisDatabase } from '../state/database'
import type { RedisMonitorCommandEvent } from '../state/monitor-feed'
import { monitorTimestampMicros } from './clock'
import {
  resolveCompatibilityProfile,
  type CompatibilityProfile,
} from './compatibility'
import { commandTableEntry } from './compatibility/command-table'
import {
  containerSubcommandExists,
  containerSubcommandIsAdmin,
} from './compatibility/subcommand-gates'
import { unknownSubcommandError } from './subcommand-errors'

export type CommandExecutorOptions = {
  registry: CommandRegistry
  policies?: readonly ExecutionPolicy[]
  profile?: CompatibilityProfile
}

/**
 * Central command pipeline shared by every client session.
 *
 * Responsibilities:
 *  1. Resolve a raw command name to a {@link CommandDefinition} (case-insensitive).
 *  2. Parse raw argument buffers into typed args and extract routing keys,
 *     producing a {@link CommandPlan}.
 *  3. Run the configured {@link ExecutionPolicy} chain before the command's own
 *     `execute`, giving policies (auth, cluster, transaction, ...) a chance to
 *     short-circuit it — queue, redirect, or reject — with their own result.
 *
 * Two execution paths exist on purpose:
 *  - {@link executePlan} / {@link executeRaw} — async, used for real network
 *    clients; may await async commands.
 *  - {@link executePlanSync} — synchronous mirror used by the Lua runtime, where
 *    `redis.call` must complete in a single tick. Promises are rejected rather
 *    than awaited.
 *
 * The executor is stateless per-call: all mutable state lives on the
 * {@link RedisExecutionContext} (and the session it carries).
 */
export class CommandExecutor {
  private readonly registry: CommandRegistry
  private readonly policies: readonly ExecutionPolicy[]
  readonly profile: CompatibilityProfile

  constructor(options: CommandExecutorOptions) {
    this.registry = options.registry
    this.policies = options.policies ?? []
    this.profile = options.profile ?? resolveCompatibilityProfile()
  }

  getCommandDefinition(name: string): CommandDefinition<unknown> | undefined {
    return this.registry.get(name)
  }

  getCommandDefinitions(): readonly CommandDefinition<unknown>[] {
    return this.registry.getAll()
  }

  /**
   * Resolve a raw command + args into a {@link CommandPlan} without executing it.
   * The name is handed to the registry unfolded; `registry.get` does the
   * case-insensitive match, folding ASCII only as real Redis does — so e.g.
   * U+212A KELVIN SIGN + "eys" is an unknown command, not KEYS (#382).
   *
   * @throws {UnknownRedisCommandError} if no command is registered under the name.
   * @throws {UnknownSubcommandError} if a container's subcommand fails lookup
   *   (see {@link resolveTableEntry}).
   *
   * `options.inScript` plans a script's `redis.call`/`redis.pcall`: the
   * parser of a command that refuses an option from scripts sees it (XREAD
   * BLOCK up to 7.2, #563).
   */
  plan(
    rawCommand: Buffer | string,
    rawArgs: readonly Buffer[],
    options: { inScript?: boolean } = {},
  ): CommandPlan {
    return this.createPlan(
      this.lookup(rawCommand, rawArgs),
      rawCommand,
      rawArgs,
      options,
    )
  }

  /**
   * Command lookup, as Redis's `processCommand` does it for every caller (a
   * client, MULTI, a script, COMMAND GETKEYS): the definition, from 7.0 the
   * container subcommand (see {@link resolveTableEntry}), then the
   * command-table arity of the entry lookup resolved to — the
   * `container|subcommand` entry when there is one (`CLIENT REPLY` is `wrong
   * number of arguments for 'client|reply' command` before CLIENT sees it),
   * otherwise the command's own (`XREAD COUNT` never reaches XREAD's parser).
   */
  private lookup(
    rawCommand: Buffer | string,
    rawArgs: readonly Buffer[],
  ): CommandDefinition<unknown> {
    const { definition, subcommandKnown } = this.resolveTableEntry(
      rawCommand,
      rawArgs,
    )

    if (!definition) {
      throw new UnknownRedisCommandError(rawCommand, rawArgs, this.profile)
    }

    if (subcommandKnown === false) {
      throw unknownSubcommandError(
        asciiUpperCase(definition.name),
        rawArgs[0],
        this.profile,
      )
    }
    const table = lookupTableArity(definition, rawArgs, this.profile)
    if (failsTableArity(table.arity, rawArgs.length + 1)) {
      throw new WrongNumberOfArgumentsError(table.name)
    }
    return definition
  }

  /**
   * The command-table lookup shared by {@link lookup} and
   * {@link lookupCommandName}: the definition (`undefined` for an unknown
   * command) and, from 7.0, whether the first argument names a subcommand of
   * the container (`subcommandKnown`).
   *
   * Redis 7.0 put container subcommands (`config|get`, `xgroup|create`, ...)
   * in the command table, so from 7.0 command lookup resolves the subcommand
   * too and an unknown one fails right there — ahead of arity, the schema,
   * routing keys and every policy. Doing it in lookup, the one place every
   * command is planned, is what makes MULTI refuse to queue it (#435), keeps
   * XGROUP / XINFO from looking their key up first (#436) and gives a
   * script's `redis.call` the unknown-command error (#439).
   *
   * 6.2 has no such lookup (`subcommandKnown` is `undefined`); there each
   * container rejects the subcommand when it runs. It is `undefined` too for
   * a call without arguments and for a command that is not a container. The
   * table is the *real* one (`subcommand-gates.ts`), so a real subcommand
   * this server does not implement passes and is rejected by its container
   * at execute time, as before.
   */
  private resolveTableEntry(
    rawCommand: Buffer | string,
    rawArgs: readonly Buffer[],
  ): {
    definition: CommandDefinition<unknown> | undefined
    subcommandKnown: boolean | undefined
  } {
    const definition = this.registry.get(rawCommand.toString())
    if (
      !definition ||
      rawArgs.length === 0 ||
      !this.profile.has('error.unknown-subcommand-dispatch-timing')
    ) {
      return { definition, subcommandKnown: undefined }
    }

    return {
      definition,
      subcommandKnown: containerSubcommandExists(
        definition.name,
        rawArgs[0],
        this.profile,
      ),
    }
  }

  /**
   * The command-table name lookup resolves a raw command to, as Redis keeps
   * it in `c->lastcmd` for CLIENT LIST's `cmd=`: the command's name, from
   * 7.0 `container|subcommand` for a container called with a subcommand, and
   * `null` when lookup fails (an unknown command, or from 7.0 an unknown
   * subcommand). It does not check arity: Redis records the command before
   * its arity check.
   */
  lookupCommandName(
    rawCommand: Buffer | string,
    rawArgs: readonly Buffer[],
  ): string | null {
    const { definition, subcommandKnown } = this.resolveTableEntry(
      rawCommand,
      rawArgs,
    )
    if (!definition || subcommandKnown === false) {
      return null
    }

    return subcommandKnown
      ? `${definition.name}|${asciiLowerCase(rawArgs[0].toString('latin1'))}`
      : definition.name
  }

  /**
   * Plan and execute a raw command in one step — the normal entry point for a
   * network client.
   *
   * Errors thrown during *planning* (unknown command, arity) are caught here
   * and converted into a RESP error reply. Every command is planned the way
   * Redis's `processCommand` sees it (see {@link planForQueue}): the
   * command's own argument check runs in `call()`, after the policy chain,
   * so its error is the plan's `deferredError`. Execution-time errors are
   * handled inside {@link executePlan}.
   */
  async executeRaw(
    rawCommand: Buffer | string,
    rawArgs: readonly Buffer[],
    ctx: RedisExecutionContext,
  ): Promise<RedisResult> {
    try {
      // processCommand refuses a command before its parser runs: a queued
      // command waits for EXEC, a MONITOR connection is refused the keyspace,
      // a cluster routes it (getNodeByQuery: MOVED, CROSSSLOT, CLUSTERDOWN),
      // so `EVAL s 1x k` with `k` on another node is MOVED, not the numkeys
      // error. A command that fails its own argument check still reaches
      // call(), which feeds it to MONITOR (`EVAL s -1 a`, #519).
      const plan = this.planForQueue(rawCommand, rawArgs)
      return await this.executePlan(plan, ctx)
    } catch (err) {
      if (err instanceof RedisCommandError) {
        return this.rawCommandErrorResult(err, rawCommand, ctx)
      }

      throw err
    }
  }

  /**
   * Plan a command sent inside MULTI. Real Redis refuses one at queue time
   * only when `processCommand` does: an unknown command or (7.0+) subcommand,
   * or an argument count the command table's arity rejects — from 7.0 the
   * `container|subcommand` entry's. Those throw here, and the caller dirties
   * the transaction so EXEC aborts.
   *
   * Any other error the parser raises is the command's own argument check,
   * which Redis runs when EXEC calls the command. The command is queued as a
   * plan carrying that error as `deferredError`, raised after the policy
   * chain, so the error fills its slot in EXEC's reply. Such a plan's `args`
   * are unusable; its routing keys come from the command's key specs over the
   * raw arguments ({@link rawCommandKeys}), as Redis routes a queued command
   * without running its parser. The transaction commands themselves (EXEC,
   * DISCARD, ...) run at once, so their parse errors are answered now.
   *
   * A cluster plans every command this way, a script's `redis.call` included
   * (`options.inScript`, as for {@link plan}): Redis routes it by these keys
   * before the command checks its arguments.
   */
  planForQueue(
    rawCommand: Buffer | string,
    rawArgs: readonly Buffer[],
    options: { inScript?: boolean } = {},
  ): CommandPlan {
    const definition = this.lookup(rawCommand, rawArgs)

    try {
      return this.createPlan(definition, rawCommand, rawArgs, options)
    } catch (err) {
      if (
        !(err instanceof RedisCommandError) ||
        definition.flags.includes('transaction')
      ) {
        throw err
      }

      return {
        definition,
        args: undefined,
        keys: rawCommandKeys(definition, rawCommand, rawArgs, this.profile),
        rawCommand: Buffer.from(rawCommand),
        rawArgs: rawArgs.map(arg => Buffer.from(arg)),
        deferredError: err,
      }
    }
  }

  private rawCommandErrorResult(
    err: RedisCommandError,
    rawCommand: Buffer | string,
    ctx: RedisExecutionContext,
  ): RedisResult {
    // EXEC itself with bad arity (e.g. `EXEC foo`) discards the transaction
    // immediately and replies EXECABORT, matching Redis' execCommandAbort —
    // distinct from a *queued* command's arity error, which only dirties the
    // transaction for a later, well-formed EXEC.
    if (
      err instanceof WrongNumberOfArgumentsError &&
      ctx.session.mode === 'transaction' &&
      asciiLowerCase(rawCommand.toString()) === 'exec'
    ) {
      ctx.session.discardTransaction()
      const abortError = new ExecCommandAbortError(err.message)
      return RedisResult.error(abortError.message, abortError.code)
    }

    ctx.session.markTransactionDirty()
    return RedisResult.fromError(err)
  }

  /**
   * Run a pre-built plan through the full async pipeline.
   *
   * Order of operations:
   *  1. `beforeExecute` for each policy. The first policy that returns a result
   *     short-circuits execution (e.g. the transaction policy queues the command
   *     and returns "+QUEUED"; the cluster policy returns a MOVED/CROSSSLOT
   *     error). A short-circuit error during MULTI also dirties the transaction.
   *  2. The command's own `execute`, awaited.
   *
   * Execution-time {@link RedisCommandError}s become RESP error replies (and
   * dirty an open transaction when appropriate). Non-Redis errors propagate.
   */
  async executePlan(
    plan: CommandPlan,
    ctx: RedisExecutionContext,
  ): Promise<RedisResult> {
    const monitorCtx = createMonitorDeferredContext(ctx)
    const monitor: MonitorDispatch = {}
    const result = await this.executePlanInternal(plan, monitorCtx, monitor)
    publishMonitorEvent(plan, monitorCtx, result, monitor)
    flushDeferredMonitorEvents(monitorCtx)
    return result
  }

  private async executePlanInternal(
    plan: CommandPlan,
    ctx: RedisExecutionContext,
    monitor: MonitorDispatch,
  ): Promise<RedisResult> {
    try {
      for (const policy of this.policies) {
        const policyResult = await policy.beforeExecute?.(plan, ctx)
        if (policyResult) {
          monitor.rejected = policy.rejectsBeforeCall === true
          return applyPolicyShortCircuit(plan, ctx, policyResult)
        }
      }

      this.stampMonitorAtDispatch(plan, ctx, monitor)

      if (plan.deferredError) {
        throw plan.deferredError
      }

      announceKeyMisses(plan, ctx)
      try {
        return await plan.definition.execute(
          plan.args,
          withMutationOrigin(plan, ctx),
        )
      } finally {
        rememberTrackedKeys(plan, ctx)
      }
    } catch (err) {
      return executionErrorResult(plan, ctx, err)
    }
  }

  /**
   * Synchronous counterpart to {@link executePlan}, used by the Lua runtime for
   * `redis.call` / `redis.pcall`. Lua expects each nested command to resolve
   * immediately, so anything that would require awaiting — a command that
   * returns a promise, or an async policy hook — is
   * rejected with a {@link RedisCommandError} instead of being awaited. Async
   * command definitions are rejected before invocation so they cannot leave
   * orphaned work running after the script error (see
   * {@link assertSyncCommandDefinition} and {@link assertSyncCommandResult}).
   *
   * The policy chain and transaction-dirty handling otherwise mirror the async
   * path exactly.
   */
  executePlanSync(plan: CommandPlan, ctx: RedisExecutionContext): RedisResult {
    const monitorCtx = createMonitorDeferredContext(ctx)
    const monitor: MonitorDispatch = {}
    const result = this.executePlanSyncInternal(plan, monitorCtx, monitor)
    publishMonitorEvent(plan, monitorCtx, result, monitor)
    flushDeferredMonitorEvents(monitorCtx)
    return result
  }

  private executePlanSyncInternal(
    plan: CommandPlan,
    ctx: RedisExecutionContext,
    monitor: MonitorDispatch,
  ): RedisResult {
    try {
      for (const policy of this.policies) {
        const policyResult = policy.beforeExecute?.(plan, ctx)
        if (isThenable(policyResult)) {
          throw new RedisCommandError(
            `Execution policy '${policy.name}' beforeExecute hook cannot run asynchronously from scripts`,
          )
        }
        if (policyResult) {
          monitor.rejected = policy.rejectsBeforeCall === true
          return applyPolicyShortCircuit(plan, ctx, policyResult)
        }
      }

      this.stampMonitorAtDispatch(plan, ctx, monitor)

      if (plan.deferredError) {
        throw plan.deferredError
      }

      assertSyncCommandDefinition(plan)
      announceKeyMisses(plan, ctx)

      try {
        return assertSyncCommandResult(
          plan,
          plan.definition.execute(plan.args, withMutationOrigin(plan, ctx)),
        )
      } finally {
        rememberTrackedKeys(plan, ctx)
      }
    } catch (err) {
      return executionErrorResult(plan, ctx, err)
    }
  }

  /**
   * EVAL / EVALSHA / FCALL and their `_RO` forms feed MONITOR from inside
   * their own proc, before the script runs (`replicationFeedMonitors` at the
   * top of `evalCommand` / `fcallCommandGeneric`), which is why the real
   * command table flags exactly them `skip_monitor`: `call()` must not feed
   * them a second time. So their line is stamped, with the database selected,
   * at dispatch, and reads no later than the `[0 lua]` lines the script
   * produces (#433). Every other command is fed by `call()` after it ran.
   * Verified against 6.2.14 - 8.0.0 and Valkey 8.0 / 9.0 source and a live
   * redis-server 7.0.15.
   */
  private stampMonitorAtDispatch(
    plan: CommandPlan,
    ctx: RedisExecutionContext,
    monitor: MonitorDispatch,
  ): void {
    if (!monitorFeedActive(plan, ctx)) {
      return
    }

    const entry = commandTableEntry(plan.definition.name, this.profile)
    if (!entry?.flags.includes('skip_monitor')) {
      return
    }

    monitor.event = createMonitorEvent(plan, ctx)
  }

  /**
   * Build a {@link CommandPlan} from a resolved definition: parse the raw buffers
   * against the command's schema (may throw arity/type errors) and extract the
   * routing keys used for cluster slot validation. The definition rides along on
   * the plan, so policies read flags off `plan.definition.flags`.
   */
  private createPlan<TArgs>(
    definition: CommandDefinition<TArgs>,
    rawCommand: Buffer | string,
    rawArgs: readonly Buffer[],
    options: { inScript?: boolean } = {},
  ): CommandPlan<TArgs> {
    const args = parseCommandArgs(
      definition.schema,
      rawArgs,
      definition.name,
      this.profile,
      { rawCommand: Buffer.from(rawCommand), inScript: options.inScript },
    )
    const keys = definition.keys(args)

    return {
      definition,
      args,
      keys,
      rawCommand: Buffer.from(rawCommand),
      rawArgs: rawArgs.map(arg => Buffer.from(arg)),
    }
  }
}

/**
 * What one command's run tells MONITOR beyond its result: whether a policy
 * refused it the way `processCommand` does (never fed), and the line already
 * stamped at dispatch for a command Redis feeds from inside its proc.
 */
type MonitorDispatch = {
  rejected?: boolean
  event?: RedisMonitorCommandEvent
}

function createMonitorEvent(
  plan: CommandPlan,
  ctx: RedisExecutionContext,
): RedisMonitorCommandEvent {
  return {
    timestampMicros: monitorTimestampMicros(),
    // A script's own commands carry the database the script selected
    // (#566).
    database: ctx.scriptDatabase?.selected ?? ctx.session.selectedDatabase,
    clientId: ctx.session.id,
    clientAddress: ctx.monitor?.clientAddress ?? ctx.session.clientAddress,
    command: Buffer.from(plan.rawCommand),
    args: redactMonitorArgs(plan),
  }
}

function publishMonitorEvent(
  plan: CommandPlan,
  ctx: RedisExecutionContext,
  result: RedisResult,
  monitor: MonitorDispatch,
): void {
  if (monitor.rejected || !shouldPublishMonitorEvent(plan, ctx, result)) {
    return
  }

  const event = monitor.event ?? createMonitorEvent(plan, ctx)

  if (ctx.monitor?.defer && ctx.monitor.deferredEvents) {
    ctx.monitor.deferredEvents.push(event)
    return
  }

  ctx.server.monitorFeed.publish(event)
}

function createMonitorDeferredContext(
  ctx: RedisExecutionContext,
): RedisExecutionContext {
  if (ctx.monitor?.disabled || ctx.monitor?.defer) {
    return ctx
  }

  if (ctx.server.monitorFeed.subscriberCount === 0) {
    return ctx
  }

  // Override `monitor` on a prototype link rather than copying the context:
  // `db` is a *live getter* on the session context (a queued `SELECT N` runs
  // mid-EXEC and every later command must resolve the currently selected
  // database — issue #94), and a spread would freeze it to the database that
  // was selected when this context was built.
  //
  // INVARIANT: the returned context must never be spread (`{...ctx}`) or
  // key-enumerated (`Object.keys`/`assign`/JSON). Every field except `monitor`
  // lives on the prototype, so enumerating own properties yields `{ monitor }`
  // and silently loses the rest. Nothing in `src/` does this today; read
  // through the context instead of copying it.
  return Object.create(ctx, {
    monitor: {
      value: { ...ctx.monitor, deferredEvents: [] },
      enumerable: true,
      writable: true,
      configurable: true,
    },
  }) as RedisExecutionContext
}

function flushDeferredMonitorEvents(ctx: RedisExecutionContext): void {
  const events = ctx.monitor?.deferredEvents
  if (!events || ctx.monitor?.defer) {
    return
  }

  for (const event of events) {
    ctx.server.monitorFeed.publish(event)
  }

  events.length = 0
}

function shouldPublishMonitorEvent(
  plan: CommandPlan,
  ctx: RedisExecutionContext,
  result: RedisResult,
): boolean {
  if (!monitorFeedActive(plan, ctx)) {
    return false
  }

  if (isQueuedTransactionCommand(plan, ctx, result)) {
    return false
  }

  if (
    result.value.kind === 'error' &&
    CLUSTER_PRE_EXECUTION_ERROR_CODES.has(result.value.code ?? '')
  ) {
    return false
  }

  return true
}

/** Whether anyone is monitoring and this command may be shown at all. */
function monitorFeedActive(
  plan: CommandPlan,
  ctx: RedisExecutionContext,
): boolean {
  return (
    !ctx.monitor?.disabled &&
    ctx.server.monitorFeed.subscriberCount > 0 &&
    !skipsMonitor(plan, ctx)
  )
}

/**
 * `call()` hides a command from MONITOR when its table entry is flagged
 * `skip_monitor` or `admin` ("administrative commands are considered too
 * dangerous to be shown"). The entry is the subcommand's from 7.0
 * (`CLIENT KILL` is hidden, `CLIENT GETNAME` shown) and the whole container's
 * on 6.2, where every CLIENT and CONFIG subcommand is hidden. Verified
 * against `call()` of 6.2.14 - 8.0.0 and Valkey 8.0 / 9.0 and a live
 * redis-server 6.2.14. A definition can hide itself on top of the table;
 * CLUSTER and CONFIG do not, so their non-admin subcommands (CLUSTER INFO,
 * CONFIG HELP) are shown from 7.0, as on real servers.
 */
function skipsMonitor(plan: CommandPlan, ctx: RedisExecutionContext): boolean {
  const skip = plan.definition.monitor?.skip
  if (typeof skip === 'function' ? skip(ctx.server.profile) : !!skip) {
    return true
  }

  const { name } = lookupTableArity(
    plan.definition,
    plan.rawArgs,
    ctx.server.profile,
  )
  if (commandTableEntry(name, ctx.server.profile)?.flags.includes('admin')) {
    return true
  }
  // A real subcommand this server captured no table entry for (CLUSTER
  // MEET, ACL SETUSER) still carries the real table's admin flag.
  const separator = name.indexOf('|')
  return (
    separator > 0 &&
    containerSubcommandIsAdmin(
      name.slice(0, separator),
      name.slice(separator + 1),
    )
  )
}

function isQueuedTransactionCommand(
  plan: CommandPlan,
  ctx: RedisExecutionContext,
  result: RedisResult,
): boolean {
  return (
    ctx.session.mode === 'transaction' &&
    !plan.definition.flags.includes('transaction') &&
    result.value.kind === 'simple-string' &&
    result.value.value === 'QUEUED'
  )
}

function redactMonitorArgs(plan: CommandPlan): Buffer[] {
  const args =
    plan.definition.monitor?.redactArgs?.(plan.rawArgs) ?? plan.rawArgs
  return args.map(arg => Buffer.from(arg))
}

const CLUSTER_PRE_EXECUTION_ERROR_CODES = new Set([
  'ASK',
  'CLUSTERDOWN',
  'CROSSSLOT',
  'MOVED',
  'TRYAGAIN',
])

/**
 * A policy that short-circuits with an error while a command is being queued in
 * MULTI dirties the transaction, so the later EXEC aborts.
 */
function applyPolicyShortCircuit(
  plan: CommandPlan,
  ctx: RedisExecutionContext,
  result: RedisResult,
): RedisResult {
  if (isTransactionQueueError(plan, ctx, result)) {
    ctx.session.markTransactionDirty()
  }

  return result
}

/**
 * Derive the context a command executes with: `ctx.db` becomes a view of the
 * selected database whose mutations carry the command's name (see
 * `RedisDatabase.withOrigin`), which keyspace notifications name write events
 * after. The name is bound to this command's handle, so it survives the
 * command parking and resuming in any order, and cannot leak onto another
 * command's writes (#444). A nested command (Lua `redis.call`, EXEC's queue)
 * derives its own view from this one, shadowing the name.
 *
 * Like {@link createMonitorDeferredContext}, this overrides `db` on a
 * prototype link rather than copying the context, and keeps it a *live*
 * getter: a SELECT mid-command (or mid-EXEC) must be seen by every later
 * access (#94). The view is cached per underlying database so `ctx.db` keeps a
 * stable identity within one command.
 */
/**
 * CLIENT TRACKING: once a readonly command has run, the client that sent it
 * (a script's caller, the client running EXEC) remembers its keys, missing
 * ones included (redis@7.2.4:src/server.c:3651-3666). EVAL_RO, EVALSHA_RO and
 * FCALL_RO are skipped, only the commands they run count, and so are Pub/Sub
 * commands, whose shard channels are keys. Before Redis 7.2 a readonly
 * command run from a script remembers the keys the script declared instead
 * (`trackingRememberKeys(caller)` reads the caller's argv,
 * redis@7.0.15:src/server.c:3515-3529, redis@6.2.14:src/server.c:3918-3928).
 */
function rememberTrackedKeys(
  plan: CommandPlan,
  ctx: RedisExecutionContext,
): void {
  const { definition } = plan
  if (
    !definition.flags.includes('readonly') ||
    definition.flags.includes('pubsub') ||
    READONLY_SCRIPT_COMMANDS.has(definition.name)
  ) {
    return
  }
  const keys =
    ctx.inScript &&
    ctx.scriptKeys &&
    !ctx.server.profile.has('client.tracking-script-command-keys')
      ? ctx.scriptKeys
      : plan.keys
  if (keys.length > 0) {
    ctx.server.clientTracking.rememberKeys(ctx.session, keys)
  }
}

const READONLY_SCRIPT_COMMANDS = new Set(['eval_ro', 'evalsha_ro', 'fcall_ro'])

function withMutationOrigin(
  plan: CommandPlan,
  ctx: RedisExecutionContext,
): RedisExecutionContext {
  const command = plan.definition.name
  let base: RedisDatabase | undefined
  let view: RedisDatabase | undefined
  return Object.create(ctx, {
    db: {
      get(): RedisDatabase {
        const db = ctx.db
        if (db !== base || !view) {
          base = db
          view = db.withOrigin(command, ctx.session, ctx.inScript === true)
        }
        return view
      },
      enumerable: true,
      configurable: true,
    },
  }) as RedisExecutionContext
}

/**
 * Map an execution-time {@link RedisCommandError} to a RESP error reply,
 * dirtying an open transaction when appropriate. Anything else is a real bug
 * and propagates.
 */
function executionErrorResult(
  plan: CommandPlan,
  ctx: RedisExecutionContext,
  err: unknown,
): RedisResult {
  if (!(err instanceof RedisCommandError)) {
    throw err
  }

  if (shouldDirtyTransaction(plan, ctx)) {
    ctx.session.markTransactionDirty()
  }

  return RedisResult.fromError(err)
}

/**
 * Guard for the synchronous (Lua) path: a command's result must be a ready
 * {@link RedisResult}. Async commands are not callable from scripts, so a
 * promise is surfaced as a script-facing {@link RedisCommandError}. The
 * promise itself is dropped, so its rejection is consumed here: an unhandled
 * one would take the process down (#563).
 */
function assertSyncCommandResult(
  plan: CommandPlan,
  result: ReturnType<CommandDefinition['execute']>,
): RedisResult {
  if (isThenable(result)) {
    result.then(undefined, () => {})
    throw new RedisCommandError(
      `${plan.definition.name.toUpperCase()} cannot run asynchronously from scripts`,
    )
  }

  return result
}

function assertSyncCommandDefinition(plan: CommandPlan): void {
  if (!isAsyncFunction(plan.definition.execute)) {
    return
  }

  throw new RedisCommandError(
    `${plan.definition.name.toUpperCase()} cannot run asynchronously from scripts`,
  )
}

function isThenable(value: unknown): value is PromiseLike<unknown> {
  if (value === null || value === undefined) {
    return false
  }

  if (typeof value !== 'object' && typeof value !== 'function') {
    return false
  }

  return typeof (value as { then?: unknown }).then === 'function'
}

function isAsyncFunction(value: unknown): boolean {
  return (
    typeof value === 'function' && value.constructor?.name === 'AsyncFunction'
  )
}

/**
 * True when a policy short-circuit returned an error while queuing a command in
 * MULTI — i.e. the command was rejected at queue time. Redis aborts the whole
 * transaction on EXEC in that case, so the session must be marked dirty.
 */
function isTransactionQueueError(
  plan: CommandPlan,
  ctx: RedisExecutionContext,
  result: RedisResult,
): boolean {
  return shouldDirtyTransaction(plan, ctx) && result.value.kind === 'error'
}

/**
 * Whether an error on this plan should dirty the current transaction.
 *
 * Only meaningful while the session is in `transaction` mode. Commands flagged
 * `transaction` are the control commands themselves (MULTI/EXEC/DISCARD/WATCH);
 * their errors must not abort the transaction, so they are excluded.
 */
function shouldDirtyTransaction(
  plan: CommandPlan,
  ctx: RedisExecutionContext,
): boolean {
  return (
    ctx.session.mode === 'transaction' &&
    !plan.definition.flags.includes('transaction')
  )
}
