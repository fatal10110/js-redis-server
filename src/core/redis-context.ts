import type {
  RedisClusterNodeRole,
  RedisDatabase,
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

export interface RedisClientSession {
  readonly id: string
  readonly selectedDatabase: number
  readonly mode: ClientSessionMode
  readonly protocolVersion: RespVersion
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
}

export interface RedisExecutionContext {
  readonly db: RedisDatabase
  readonly server: RedisServerState
  readonly session: RedisClientSession
  readonly executor: CommandExecutor
  readonly nodeRole?: RedisClusterNodeRole
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

function createAbortError(): Error {
  const err = new Error('The operation was aborted')
  err.name = 'AbortError'
  return err
}
