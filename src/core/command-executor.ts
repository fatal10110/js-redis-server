import { CommandDefinition, CommandPlan } from './command-definition'
import { CommandRegistry } from './command-registry'
import { parseCommandArgs } from './command-schema'
import type { ExecutionPolicy } from './execution-policies'
import { RedisCommandError, UnknownRedisCommandError } from './redis-error'
import type { RedisExecutionContext } from './redis-context'
import { RedisResult } from './redis-result'
import { isResponseStream, ResponseStream } from './response-stream'

export type ExecutorResult = RedisResult | ResponseStream

export type CommandExecutorOptions = {
  registry: CommandRegistry
  policies?: readonly ExecutionPolicy[]
}

export class CommandExecutor {
  private readonly registry: CommandRegistry
  private readonly policies: readonly ExecutionPolicy[]

  constructor(options: CommandExecutorOptions) {
    this.registry = options.registry
    this.policies = options.policies ?? []
  }

  plan(rawCommand: Buffer | string, rawArgs: readonly Buffer[]): CommandPlan {
    const commandName =
      typeof rawCommand === 'string'
        ? rawCommand.toLowerCase()
        : rawCommand.toString().toLowerCase()
    const definition = this.registry.get(commandName)

    if (!definition) {
      throw new UnknownRedisCommandError(commandName, rawArgs)
    }

    return this.createPlan(definition, rawArgs)
  }

  async executeRaw(
    rawCommand: Buffer | string,
    rawArgs: readonly Buffer[],
    ctx: RedisExecutionContext,
  ): Promise<ExecutorResult> {
    try {
      return await this.executePlan(this.plan(rawCommand, rawArgs), ctx)
    } catch (err) {
      if (err instanceof RedisCommandError) {
        ctx.session.markTransactionDirty()
        return RedisResult.error(err.message, err.code)
      }

      throw err
    }
  }

  async executePlan(
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

  executePlanSync(plan: CommandPlan, ctx: RedisExecutionContext): RedisResult {
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

  private createPlan<TArgs>(
    definition: CommandDefinition<TArgs>,
    rawArgs: readonly Buffer[],
  ): CommandPlan<TArgs> {
    const args = parseCommandArgs(definition.schema, rawArgs, definition.name)
    const keys = definition.keys(args)

    return {
      definition,
      args,
      keys,
      flags: definition.flags,
    }
  }
}

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

function isTransactionQueueError(
  plan: CommandPlan,
  ctx: RedisExecutionContext,
  result: RedisResult,
): boolean {
  return shouldDirtyTransaction(plan, ctx) && result.value.kind === 'error'
}

function shouldDirtyTransaction(
  plan: CommandPlan,
  ctx: RedisExecutionContext,
): boolean {
  return (
    ctx.session.mode === 'transaction' && !plan.flags.includes('transaction')
  )
}
