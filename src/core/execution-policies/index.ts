import type { CommandPlan } from '../command-definition'
import type { RedisExecutionContext } from '../redis-context'
import type { RedisResult } from '../redis-result'

export type PolicyResult = RedisResult | void

export interface ExecutionPolicy {
  readonly name: string

  /**
   * Set when every short-circuit of this policy is one of the rejections real
   * Redis makes in `processCommand`, before `call()`: such a command never
   * runs, so MONITOR never sees it (NOAUTH, the subscribed-context refusal, a
   * MONITOR connection's keyspace refusal).
   */
  readonly rejectsBeforeCall?: boolean

  /**
   * Runs before the command's own `execute`. Returning a {@link RedisResult}
   * short-circuits execution (queue / redirect / reject); returning nothing
   * lets the next policy — and ultimately the command — run.
   *
   * May be async, but only on the network path: the synchronous Lua path
   * (`redis.call`) rejects a promise here with a `RedisCommandError`.
   */
  beforeExecute?(
    plan: CommandPlan,
    ctx: RedisExecutionContext,
  ): PolicyResult | Promise<PolicyResult>
}

export { createTransactionPolicy } from './transaction-policy'
export { createAuthPolicy } from './auth-policy'
export { createSubscribedModePolicy } from './subscribed-policy'
export { createMonitorClientPolicy } from './monitor-client-policy'
export type { ClusterPolicyOptions } from './cluster-policy'
export { createClusterPolicy } from './cluster-policy'
