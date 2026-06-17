import type {
  RedisClusterNodeRole,
  RedisDatabase,
  RedisMonitorCommandEvent,
  RedisServerState,
} from '../state'
import type { CommandExecutor } from './command-executor'
import type { CommandPlan } from './command-definition'
import type { RedisResult } from './redis-result'
import type { RespVersion } from './resp-encoder'

export type ParkRequest<TValue> = {
  waitFor: Promise<TValue | null>
  timeoutMs?: number
  signal: AbortSignal
}

export type ParkHandler = <TValue>(
  request: ParkRequest<TValue>,
) => Promise<TValue | null>

export type ClientSessionMode = 'normal' | 'transaction' | 'subscribed'

export type RedisMonitorContext = {
  readonly disabled?: boolean
  readonly defer?: boolean
  readonly clientAddress?: string
  readonly deferredEvents?: RedisMonitorCommandEvent[]
}

export interface RedisClientSession {
  readonly id: string
  readonly clientAddress?: string
  readonly selectedDatabase: number
  readonly mode: ClientSessionMode
  readonly protocolVersion: RespVersion
  readonly usesSubscribedReplyMode: boolean
  readonly clusterReadOnly: boolean
  readonly isAuthenticated: boolean
  setAuthenticated(value: boolean): void
  setProtocolVersion(version: RespVersion): void
  setClusterReadOnly(value: boolean): void
  selectDatabase(database: number): void
  beginTransaction(): void
  queueTransaction(plan: CommandPlan): void
  drainTransaction(): CommandPlan[]
  discardTransaction(): void
  markTransactionDirty(): void
  isTransactionDirty(): boolean
  executeTransaction(plans: readonly CommandPlan[]): Promise<RedisResult>
  watch(keys: readonly Buffer[]): void
  unwatch(): void
  isWatchDirty(): boolean
  readonly pubsubChannelCount: number
  readonly pubsubPatternCount: number
  readonly pubsubSubscriptionCount: number
  subscribePubSubChannels(channels: readonly Buffer[]): RedisResult[]
  unsubscribePubSubChannels(channels: readonly Buffer[]): RedisResult[]
  subscribePubSubPatterns(patterns: readonly Buffer[]): RedisResult[]
  unsubscribePubSubPatterns(patterns: readonly Buffer[]): RedisResult[]
  resetPubSub(): void
  registerResponseStreamCleanup(cleanup: () => void): () => void
  resetResponseStreams(): void
}

export interface RedisExecutionContext {
  readonly db: RedisDatabase
  readonly server: RedisServerState
  readonly session: RedisClientSession
  readonly executor: CommandExecutor
  readonly nodeRole?: RedisClusterNodeRole
  readonly monitor?: RedisMonitorContext
  readonly signal: AbortSignal
  park: ParkHandler
}

export function createDefaultParkHandler(): ParkHandler {
  return request =>
    new Promise((resolve, reject) => {
      if (request.signal.aborted) {
        reject(createAbortError())
        return
      }

      let settled = false
      let timer: NodeJS.Timeout | undefined

      const cleanup = () => {
        if (timer) {
          clearTimeout(timer)
        }
        request.signal.removeEventListener('abort', onAbort)
      }

      const settle = (callback: () => void) => {
        if (settled) return
        settled = true
        cleanup()
        callback()
      }

      const onAbort = () => {
        settle(() => reject(createAbortError()))
      }

      request.signal.addEventListener('abort', onAbort, { once: true })

      if (request.timeoutMs !== undefined) {
        timer = setTimeout(() => {
          settle(() => resolve(null))
        }, request.timeoutMs)
      }

      request.waitFor.then(
        value => settle(() => resolve(value)),
        err => settle(() => reject(err)),
      )
    })
}

export function createNoopParkHandler(): ParkHandler {
  return createDefaultParkHandler()
}

/**
 * Park handler for commands replayed inside MULTI/EXEC.
 *
 * Blocking commands (BLPOP, BLMOVE, BLMPOP, BZMPOP, XREAD BLOCK, ...) must not
 * actually block here: real Redis runs them non-blocking inside a transaction
 * and returns the immediate "nothing happened" result (e.g. BLPOP -> nil). This
 * resolves `null` straight away — the same timeout sentinel a real park returns
 * on expiry — so the command takes its non-blocking branch and its `finally`
 * cleanup (wakeup-subscription teardown) runs.
 *
 * Unlike a bare `async () => null`, it still *honors* the park request:
 *  - it attaches a no-op handler to `request.waitFor`, so a command whose wait
 *    rejects never surfaces an unhandled promise rejection (mirrors
 *    {@link createDefaultParkHandler}, which consumes `waitFor`);
 *  - it rejects with an AbortError if the session signal is already aborted, so
 *    a transaction aborted mid-EXEC propagates immediately instead of swallowing
 *    the abort.
 *
 * Commands MUST release any wakeup subscription in a `finally` around
 * `ctx.park(...)`, never by chaining off `waitFor` — a non-blocking park may
 * resolve without `waitFor` ever settling.
 */
export function createNonBlockingParkHandler(): ParkHandler {
  return request => {
    // Consume waitFor; we never await its value but a rejection must not leak.
    request.waitFor.catch(() => {})

    if (request.signal.aborted) {
      return Promise.reject(createAbortError())
    }

    return Promise.resolve(null)
  }
}

function createAbortError(): Error {
  const err = new Error('The operation was aborted')
  err.name = 'AbortError'
  return err
}
