import type { CommandPlan } from '../command-definition'
import type { RedisExecutionContext } from '../redis-context'
import type { RedisResult } from '../redis-result'
import type { ResponseStream } from '../response-stream'

export type PolicyResult = RedisResult | void
export type MaybePromise<TValue> = TValue | Promise<TValue>

export interface ExecutionPolicy {
  readonly name: string

  beforeExecute?(
    plan: CommandPlan,
    ctx: RedisExecutionContext,
  ): MaybePromise<PolicyResult>

  afterExecute?(
    plan: CommandPlan,
    ctx: RedisExecutionContext,
    result: RedisResult,
  ): MaybePromise<RedisResult>

  onStream?(
    plan: CommandPlan,
    ctx: RedisExecutionContext,
    stream: ResponseStream,
  ): MaybePromise<ResponseStream | void>
}

export { createTransactionPolicy } from './transaction-policy'
export type { ClusterPolicyOptions } from './cluster-policy'
export { createClusterPolicy } from './cluster-policy'
