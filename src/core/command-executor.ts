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
import { isResponseStream, ResponseStream } from './response-stream'
import type { RedisMonitorCommandEvent } from '../state/monitor-feed'

/**
 * The result of running a command: either a finished {@link RedisResult} or a
 * long-lived {@link ResponseStream} (e.g. SUBSCRIBE / MONITOR) whose frames the
 * transport drains over time.
 */
export type ExecutorResult = RedisResult | ResponseStream

export type RawExecutionResult = {
  result: ExecutorResult
  plan?: CommandPlan
}

export type CommandExecutorOptions = {
  registry: CommandRegistry
  policies?: readonly ExecutionPolicy[]
}

/**
 * Central command pipeline shared by every client session.
 *
 * Responsibilities:
 *  1. Resolve a raw command name to a {@link CommandDefinition} (case-insensitive).
 *  2. Parse raw argument buffers into typed args and extract routing keys,
 *     producing a {@link CommandPlan}.
 *  3. Run the configured {@link ExecutionPolicy} chain around the command's own
 *     `execute`, giving policies (transaction, cluster, ...) a chance to
 *     short-circuit, rewrite, or wrap the result.
 *
 * Two execution paths exist on purpose:
 *  - {@link executePlan} / {@link executeRaw} — async, used for real network
 *    clients; may return a {@link ResponseStream} and may await async commands.
 *  - {@link executePlanSync} — synchronous mirror used by the Lua runtime, where
 *    `redis.call` must complete in a single tick. Streams and promises are
 *    rejected rather than awaited.
 *
 * The executor is stateless per-call: all mutable state lives on the
 * {@link RedisExecutionContext} (and the session it carries).
 */
export class CommandExecutor {
  private readonly registry: CommandRegistry
  private readonly policies: readonly ExecutionPolicy[]

  constructor(options: CommandExecutorOptions) {
    this.registry = options.registry
    this.policies = options.policies ?? []
  }

  getCommandDefinition(name: string): CommandDefinition<unknown> | undefined {
    return this.registry.get(name)
  }

  getCommandDefinitions(): readonly CommandDefinition<unknown>[] {
    return this.registry.getAll()
  }

  /**
   * Resolve a raw command + args into a {@link CommandPlan} without executing it.
   * The command name is matched case-insensitively against the registry.
   *
   * @throws {UnknownRedisCommandError} if no command is registered under the name.
   */
  plan(rawCommand: Buffer | string, rawArgs: readonly Buffer[]): CommandPlan {
    const commandName = CommandExecutor.normalizeCommandName(rawCommand)
    const definition = this.registry.get(commandName)

    if (!definition) {
      throw new UnknownRedisCommandError(rawCommand, rawArgs)
    }

    return this.createPlan(definition, rawCommand, rawArgs)
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
  ): Promise<ExecutorResult> {
    return (await this.executeRawWithPlan(rawCommand, rawArgs, ctx)).result
  }

  async executeRawWithPlan(
    rawCommand: Buffer | string,
    rawArgs: readonly Buffer[],
    ctx: RedisExecutionContext,
  ): Promise<RawExecutionResult> {
    try {
      const plan = this.plan(rawCommand, rawArgs)
      return { plan, result: await this.executePlan(plan, ctx) }
    } catch (err) {
      if (err instanceof RedisCommandError) {
        return {
          result: this.rawCommandErrorResult(err, rawCommand, ctx),
        }
      }

      throw err
    }
  }

  private static normalizeCommandName(rawCommand: Buffer | string): string {
    return typeof rawCommand === 'string'
      ? rawCommand.toLowerCase()
      : rawCommand.toString().toLowerCase()
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
      CommandExecutor.normalizeCommandName(rawCommand) === 'exec'
    ) {
      ctx.session.discardTransaction()
      const abortError = new ExecCommandAbortError(err.message)
      return RedisResult.error(abortError.message, abortError.code)
    }

    ctx.session.markTransactionDirty()
    return RedisResult.error(err.message, err.code)
  }

  /**
   * Run a pre-built plan through the full async pipeline.
   *
   * Order of operations:
   *  1. `beforeExecute` for each policy. The first policy that returns a result
   *     short-circuits execution (e.g. the transaction policy queues the command
   *     and returns "+QUEUED"; the cluster policy returns a MOVED/CROSSSLOT
   *     error). A short-circuit error during MULTI also dirties the transaction.
   *  2. The command's own `execute`.
   *  3. If a {@link ResponseStream} is produced, run it through every policy's
   *     `onStream` hook; otherwise await the value and run it through every
   *     `afterExecute` hook (each may replace the result).
   *
   * Execution-time {@link RedisCommandError}s become RESP error replies (and
   * dirty an open transaction when appropriate). Non-Redis errors propagate.
   */
  async executePlan(
    plan: CommandPlan,
    ctx: RedisExecutionContext,
  ): Promise<ExecutorResult> {
    const monitorCtx = createMonitorDeferredContext(ctx)
    const result = await this.executePlanInternal(plan, monitorCtx)
    publishMonitorEvent(plan, monitorCtx, result)
    flushDeferredMonitorEvents(monitorCtx)
    return result
  }

  private async executePlanInternal(
    plan: CommandPlan,
    ctx: RedisExecutionContext,
  ): Promise<ExecutorResult> {
    try {
      for (const policy of this.policies) {
        const policyResult = await policy.beforeExecute?.(plan, ctx)
        if (policyResult) {
          if (isTransactionQueueError(plan, ctx, policyResult)) {
            ctx.session.markTransactionDirty()
          }

          return policyResult
        }
      }

      // Tag the database with the active command so keyspace notifications can
      // name write events after the originating command. Saved/restored so
      // nested (Lua redis.call) and parked/interleaved commands stay correct.
      const prevNotifyCommand = ctx.db.activeNotifyCommand
      ctx.db.activeNotifyCommand = plan.definition.name
      try {
        const rawResult = plan.definition.execute(plan.args, ctx)

        if (isResponseStream(rawResult)) {
          let finalStream = ensureNonThenableStream(rawResult)
          for (const policy of this.policies) {
            finalStream =
              (await policy.onStream?.(plan, ctx, finalStream)) ?? finalStream
            finalStream = ensureNonThenableStream(finalStream)
          }

          return finalStream
        }

        const result = await rawResult

        let finalResult = result
        for (const policy of this.policies) {
          finalResult =
            (await policy.afterExecute?.(plan, ctx, finalResult)) ?? finalResult
        }

        return finalResult
      } finally {
        ctx.db.activeNotifyCommand = prevNotifyCommand
      }
    } catch (err) {
      if (err instanceof RedisCommandError) {
        if (shouldDirtyTransaction(plan, ctx)) {
          ctx.session.markTransactionDirty()
        }

        return RedisResult.error(err.message, err.code)
      }

      throw err
    }
  }

  /**
   * Synchronous counterpart to {@link executePlan}, used by the Lua runtime for
   * `redis.call` / `redis.pcall`. Lua expects each nested command to resolve
   * immediately, so anything that would require awaiting — a command that
   * returns a promise, a {@link ResponseStream}, or an async policy hook — is
   * rejected with a {@link RedisCommandError} instead of being awaited. Async
   * command definitions are rejected before invocation so they cannot leave
   * orphaned work running after the script error (see
   * {@link assertSyncCommandDefinition}, {@link assertSyncCommandResult}, and
   * {@link assertSyncPolicyResult}).
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
        const policyResult = assertSyncPolicyResult(
          policy.name,
          'beforeExecute',
          policy.beforeExecute?.(plan, ctx),
        )
        if (policyResult) {
          if (isTransactionQueueError(plan, ctx, policyResult)) {
            ctx.session.markTransactionDirty()
          }

          return policyResult
        }
      }

      assertSyncCommandDefinition(plan)

      const prevNotifyCommand = ctx.db.activeNotifyCommand
      ctx.db.activeNotifyCommand = plan.definition.name
      try {
        const rawResult = plan.definition.execute(plan.args, ctx)
        const result = assertSyncCommandResult(plan, rawResult)

        let finalResult = result
        for (const policy of this.policies) {
          finalResult =
            assertSyncPolicyResult(
              policy.name,
              'afterExecute',
              policy.afterExecute?.(plan, ctx, finalResult),
            ) ?? finalResult
        }

        return finalResult
      } finally {
        ctx.db.activeNotifyCommand = prevNotifyCommand
      }
    } catch (err) {
      if (err instanceof RedisCommandError) {
        if (shouldDirtyTransaction(plan, ctx)) {
          ctx.session.markTransactionDirty()
        }

        return RedisResult.error(err.message, err.code)
      }

      throw err
    }
  }

  /**
   * Build a {@link CommandPlan} from a resolved definition: parse the raw buffers
   * against the command's schema (may throw arity/type errors) and extract the
   * routing keys used for cluster slot validation. Flags are copied onto the plan
   * so policies can inspect them without re-resolving the definition.
   */
  private createPlan<TArgs>(
    definition: CommandDefinition<TArgs>,
    rawCommand: Buffer | string,
    rawArgs: readonly Buffer[],
  ): CommandPlan<TArgs> {
    const args = parseCommandArgs(definition.schema, rawArgs, definition.name)
    const keys = definition.keys(args)

    return {
      definition,
      args,
      keys,
      flags: definition.flags,
      rawCommand: Buffer.from(rawCommand),
      rawArgs: rawArgs.map(arg => Buffer.from(arg)),
    }
  }
}

function publishMonitorEvent(
  plan: CommandPlan,
  ctx: RedisExecutionContext,
  result: ExecutorResult,
): void {
  if (!shouldPublishMonitorEvent(plan, ctx, result)) {
    return
  }

  const event: RedisMonitorCommandEvent = {
    timestampMs: Date.now(),
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

  return {
    get db() {
      return ctx.db
    },
    server: ctx.server,
    session: ctx.session,
    executor: ctx.executor,
    ...(ctx.nodeRole ? { nodeRole: ctx.nodeRole } : {}),
    monitor: {
      ...ctx.monitor,
      deferredEvents: [],
    },
    signal: ctx.signal,
    park: ctx.park,
  }
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
  result: ExecutorResult,
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

  if (!(result instanceof RedisResult)) {
    return true
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
    !plan.flags.includes('transaction') &&
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
 * A policy's `onStream` hook may return an object that is both a
 * {@link ResponseStream} and thenable (e.g. an async wrapper). Callers `await`
 * the executor result, and awaiting a thenable stream would unwrap it into its
 * resolved value, breaking streaming. This re-wraps such a stream in a plain,
 * non-thenable object so it survives the surrounding `await` untouched.
 */
function ensureNonThenableStream(stream: ResponseStream): ResponseStream {
  if (!('then' in stream)) {
    return stream
  }

  return {
    kind: 'response-stream',
    get closed() {
      return stream.closed
    },
    frames: signal => stream.frames(signal),
    close: reason => stream.close(reason),
  }
}

/**
 * Guard for the synchronous (Lua) path: a command's result must be a ready
 * {@link RedisResult}. Streaming commands and async commands are not callable
 * from scripts, so a stream is closed and both cases are surfaced as a
 * script-facing {@link RedisCommandError}.
 */
function assertSyncCommandResult(
  plan: CommandPlan,
  result: ReturnType<CommandDefinition['execute']>,
): RedisResult {
  if (isResponseStream(result)) {
    result.close('Lua redis.call cannot run streaming commands')
    throw new RedisCommandError(
      `${plan.definition.name.toUpperCase()} is not allowed from scripts`,
    )
  }

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

/**
 * Same idea as {@link assertSyncCommandResult} but for policy hooks: an async
 * hook result cannot be awaited on the Lua path, so it is rejected with a
 * descriptive {@link RedisCommandError} naming the offending policy and hook.
 */
function assertSyncPolicyResult<TValue>(
  policyName: string,
  hookName: string,
  value: TValue | Promise<TValue>,
): TValue {
  if (isThenable(value)) {
    throw new RedisCommandError(
      `Execution policy '${policyName}' ${hookName} hook cannot run asynchronously from scripts`,
    )
  }

  return value
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
    ctx.session.mode === 'transaction' && !plan.flags.includes('transaction')
  )
}
