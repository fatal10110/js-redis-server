import { asciiLowerCase, asciiUpperCase } from './ascii-case'
import { CommandDefinition, CommandPlan } from './command-definition'
import { CommandRegistry } from './command-registry'
import { parseCommandArgs } from './command-schema'
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
import { containerSubcommandExists } from './compatibility/subcommand-gates'
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
   *   (see {@link lookupSubcommand}).
   */
  plan(rawCommand: Buffer | string, rawArgs: readonly Buffer[]): CommandPlan {
    const definition = this.registry.get(rawCommand.toString())

    if (!definition) {
      throw new UnknownRedisCommandError(rawCommand, rawArgs)
    }

    this.lookupSubcommand(definition, rawArgs)
    return this.createPlan(definition, rawCommand, rawArgs)
  }

  /**
   * Redis 7.0 put container subcommands (`config|get`, `xgroup|create`, ...)
   * in the command table, so from 7.0 command lookup resolves the subcommand
   * too and an unknown one fails right there — ahead of arity, the schema,
   * routing keys and every policy. Doing it here, the one place every command
   * is planned, is what makes MULTI refuse to queue it (#435), keeps XGROUP /
   * XINFO from looking their key up first (#436) and gives a script's
   * `redis.call` the unknown-command error (#439).
   *
   * 6.2 has no such lookup; there each container rejects the subcommand when
   * it runs. The table is the *real* one (`subcommand-gates.ts`), so a real
   * subcommand this server does not implement passes and is rejected by its
   * container at execute time, as before.
   */
  private lookupSubcommand(
    definition: CommandDefinition<unknown>,
    rawArgs: readonly Buffer[],
  ): void {
    if (
      rawArgs.length === 0 ||
      !this.profile.has('error.unknown-subcommand-dispatch-timing')
    ) {
      return
    }

    const subcommand = rawArgs[0]
    if (
      containerSubcommandExists(definition.name, subcommand, this.profile) !==
      false
    ) {
      return
    }

    throw unknownSubcommandError(
      asciiUpperCase(definition.name),
      subcommand,
      this.profile,
    )
  }

  /**
   * Plan and execute a raw command in one step — the normal entry point for a
   * network client.
   *
   * Errors thrown during *planning* (unknown command, arity/parse failures) are
   * caught here and converted into a RESP error reply. Such a failure also marks
   * any open MULTI transaction dirty so a later EXEC is aborted, matching Redis:
   * a command that cannot even be parsed must not silently vanish from the queue.
   * Execution-time errors are handled inside {@link executePlan}.
   */
  async executeRaw(
    rawCommand: Buffer | string,
    rawArgs: readonly Buffer[],
    ctx: RedisExecutionContext,
  ): Promise<RedisResult> {
    try {
      return await this.executePlan(this.plan(rawCommand, rawArgs), ctx)
    } catch (err) {
      if (err instanceof RedisCommandError) {
        return this.rawCommandErrorResult(err, rawCommand, ctx)
      }

      throw err
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
    const result = await this.executePlanInternal(plan, monitorCtx)
    publishMonitorEvent(plan, monitorCtx, result)
    flushDeferredMonitorEvents(monitorCtx)
    return result
  }

  private async executePlanInternal(
    plan: CommandPlan,
    ctx: RedisExecutionContext,
  ): Promise<RedisResult> {
    try {
      for (const policy of this.policies) {
        const policyResult = await policy.beforeExecute?.(plan, ctx)
        if (policyResult) {
          return applyPolicyShortCircuit(plan, ctx, policyResult)
        }
      }

      return await plan.definition.execute(
        plan.args,
        withMutationOrigin(plan, ctx),
      )
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
    const result = this.executePlanSyncInternal(plan, monitorCtx)
    publishMonitorEvent(plan, monitorCtx, result)
    flushDeferredMonitorEvents(monitorCtx)
    return result
  }

  private executePlanSyncInternal(
    plan: CommandPlan,
    ctx: RedisExecutionContext,
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
          return applyPolicyShortCircuit(plan, ctx, policyResult)
        }
      }

      assertSyncCommandDefinition(plan)

      return assertSyncCommandResult(
        plan,
        plan.definition.execute(plan.args, withMutationOrigin(plan, ctx)),
      )
    } catch (err) {
      return executionErrorResult(plan, ctx, err)
    }
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
  ): CommandPlan<TArgs> {
    const args = parseCommandArgs(
      definition.schema,
      rawArgs,
      definition.name,
      this.profile,
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

function publishMonitorEvent(
  plan: CommandPlan,
  ctx: RedisExecutionContext,
  result: RedisResult,
): void {
  if (!shouldPublishMonitorEvent(plan, ctx, result)) {
    return
  }

  const event: RedisMonitorCommandEvent = {
    timestampMicros: monitorTimestampMicros(),
    database: ctx.session.selectedDatabase,
    clientId: ctx.session.id,
    clientAddress: ctx.monitor?.clientAddress ?? ctx.session.clientAddress,
    command: Buffer.from(plan.rawCommand),
    args: redactMonitorArgs(plan),
  }

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
  if (ctx.monitor?.disabled) {
    return false
  }

  if (ctx.server.monitorFeed.subscriberCount === 0) {
    return false
  }

  if (plan.definition.monitor?.skip) {
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
          view = db.withOrigin(command)
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
 * promise is surfaced as a script-facing {@link RedisCommandError}.
 */
function assertSyncCommandResult(
  plan: CommandPlan,
  result: ReturnType<CommandDefinition['execute']>,
): RedisResult {
  if (isThenable(result)) {
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
