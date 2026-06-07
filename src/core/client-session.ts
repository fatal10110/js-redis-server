import type { CommandPlan } from './command-definition'
import { CommandExecutor, type ExecutorResult } from './command-executor'
import {
  createDefaultParkHandler,
  type ClientSessionMode,
  type ParkHandler,
  type ParkRequest,
  type RedisClientSession,
  type RedisExecutionContext,
} from './redis-context'
import { RedisCommandError } from './redis-error'
import { RedisResult } from './redis-result'
import { RedisValue } from './redis-value'
import type { RespVersion } from './resp-encoder'
import { type RedisTurnHandle, type RedisTurnQueue } from './turn-queue'
import type { RedisDatabase, RedisServerState, Unsubscribe } from '../state'

export type ClientSessionOptions = {
  id?: string
  server: RedisServerState
  executor: CommandExecutor
  database?: number
  signal?: AbortSignal
  park?: ParkHandler
  turnQueue?: RedisTurnQueue
}

type TurnAccess = {
  get(): RedisTurnHandle | undefined
  set(turn: RedisTurnHandle | undefined): void
}

type WatchRegistration = {
  database: number
  key: Buffer
  unsubscribe: Unsubscribe
}

export class ClientSession implements RedisClientSession {
  private static nextId = 0

  readonly id: string
  readonly server: RedisServerState
  readonly signal: AbortSignal

  private readonly executor: CommandExecutor
  private readonly signalSource?: AbortController
  private readonly parkHandler: ParkHandler
  private readonly turnQueueOverride?: RedisTurnQueue
  private selectedDatabaseId: number
  private sessionMode: ClientSessionMode = 'normal'
  private respVersion: RespVersion = 2
  private transactionPlans: CommandPlan[] = []
  private transactionDirty = false
  private readonly watches = new Map<string, WatchRegistration>()
  private readonly dirtyWatches = new Set<string>()

  constructor(options: ClientSessionOptions) {
    this.id = options.id ?? `client-${++ClientSession.nextId}`
    this.server = options.server
    this.executor = options.executor
    this.selectedDatabaseId = options.database ?? 0
    this.parkHandler = options.park ?? createDefaultParkHandler()
    this.turnQueueOverride = options.turnQueue

    if (options.signal) {
      this.signal = options.signal
    } else {
      this.signalSource = new AbortController()
      this.signal = this.signalSource.signal
    }

    this.server.getDatabase(this.selectedDatabaseId)
  }

  get selectedDatabase(): number {
    return this.selectedDatabaseId
  }

  get mode(): ClientSessionMode {
    return this.sessionMode
  }

  get protocolVersion(): RespVersion {
    return this.respVersion
  }

  get db(): RedisDatabase {
    return this.server.getDatabase(this.selectedDatabaseId)
  }

  setProtocolVersion(version: RespVersion): void {
    this.respVersion = version
  }

  selectDatabase(database: number): void {
    if (
      !Number.isInteger(database) ||
      database < 0 ||
      database >= this.server.databases.length
    ) {
      throw new RedisCommandError('DB index is out of range')
    }

    this.selectedDatabaseId = database
  }

  beginTransaction(): void {
    if (this.sessionMode === 'transaction') {
      throw new RedisCommandError('MULTI calls can not be nested')
    }

    this.sessionMode = 'transaction'
    this.transactionPlans = []
    this.transactionDirty = false
  }

  queueTransaction(plan: CommandPlan): void {
    if (this.sessionMode !== 'transaction') {
      throw new RedisCommandError('MULTI has not been called')
    }

    this.transactionPlans.push(plan)
  }

  drainTransaction(): CommandPlan[] {
    if (this.sessionMode !== 'transaction') {
      return []
    }

    const plans = [...this.transactionPlans]
    this.transactionPlans = []
    this.sessionMode = 'normal'
    this.transactionDirty = false
    this.unwatch()
    return plans
  }

  discardTransaction(): void {
    this.transactionPlans = []
    this.sessionMode = 'normal'
    this.transactionDirty = false
    this.unwatch()
  }

  markTransactionDirty(): void {
    if (this.sessionMode === 'transaction') {
      this.transactionDirty = true
    }
  }

  isTransactionDirty(): boolean {
    return this.transactionDirty
  }

  async executeTransaction(
    plans: readonly CommandPlan[],
  ): Promise<RedisResult> {
    const values: RedisValue[] = []

    for (const plan of plans) {
      if (this.signal.aborted) {
        throw createAbortError()
      }

      const result = await this.executor.executePlan(
        plan,
        this.createExecutionContext(),
      )

      if (result instanceof RedisResult) {
        values.push(result.value)
        continue
      }

      result.close('streaming command is not allowed in transaction')
      values.push(
        RedisValue.error('Streaming command is not allowed in transaction'),
      )
    }

    return RedisResult.create(RedisValue.array(values))
  }

  watch(keys: readonly Buffer[]): void {
    const database = this.selectedDatabaseId
    const db = this.db

    for (const key of keys) {
      const id = watchId(database, key)
      if (this.watches.has(id)) {
        continue
      }

      const unsubscribe = db.subscribeKey(key, () => {
        this.dirtyWatches.add(id)
      })

      this.watches.set(id, {
        database,
        key: Buffer.from(key),
        unsubscribe,
      })
    }
  }

  unwatch(): void {
    for (const watch of this.watches.values()) {
      watch.unsubscribe()
    }

    this.watches.clear()
    this.dirtyWatches.clear()
  }

  isWatchDirty(): boolean {
    return this.dirtyWatches.size > 0
  }

  async execute(
    rawCommand: Buffer | string,
    rawArgs: readonly Buffer[],
  ): Promise<ExecutorResult> {
    if (this.signal.aborted) {
      throw createAbortError()
    }

    const turnQueue = this.turnQueueOverride ?? this.db.turnQueue
    let turn: RedisTurnHandle | undefined = await turnQueue.waitTurn()
    try {
      const ctx = this.createExecutionContext({
        get: () => turn,
        set: nextTurn => {
          turn = nextTurn
        },
      })

      return await this.executor.executeRaw(rawCommand, rawArgs, ctx)
    } finally {
      turn?.release()
    }
  }

  createExecutionContext(turnAccess?: TurnAccess): RedisExecutionContext {
    return {
      db: this.db,
      server: this.server,
      session: this,
      executor: this.executor,
      signal: this.signal,
      park: turnAccess
        ? this.createTurnAwareParkHandler(turnAccess)
        : this.parkHandler,
    }
  }

  close(): void {
    this.signalSource?.abort()
    this.unwatch()
    this.transactionPlans = []
    this.transactionDirty = false
    this.sessionMode = 'normal'
  }

  private createTurnAwareParkHandler(turnAccess: TurnAccess): ParkHandler {
    return async <TValue>(request: ParkRequest<TValue>) => {
      const turn = turnAccess.get()
      if (!turn) {
        return this.parkHandler(request)
      }

      let parkedValue: TValue | null = null
      const parked = this.parkHandler(request).then(value => {
        parkedValue = value
      })

      turnAccess.set(undefined)
      const nextTurn = await turn.suspend(parked)
      turnAccess.set(nextTurn)
      return parkedValue
    }
  }
}

function watchId(database: number, key: Buffer): string {
  return `${database}:${key.toString('hex')}`
}

function createAbortError(): Error {
  const err = new Error('The operation was aborted')
  err.name = 'AbortError'
  return err
}
