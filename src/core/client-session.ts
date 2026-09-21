import type { CommandPlan } from './command-definition'
import { CommandExecutor, type ExecutorResult } from './command-executor'
import {
  createDefaultParkHandler,
  createNonBlockingParkHandler,
  type ClientSessionMode,
  type ParkHandler,
  type ParkRequest,
  type PubSubKind,
  type RedisClientSession,
  type RedisExecutionContext,
  type RedisMonitorContext,
} from './redis-context'
import { RedisCommandError } from './redis-error'
import { RedisResult } from './redis-result'
import { RedisValue } from './redis-value'
import { encodeRedisValue, type RespVersion } from './resp-encoder'
import { type RedisTurnHandle, type RedisTurnQueue } from './turn-queue'
import type { RedisClusterNodeRole } from '../state/cluster-topology'
import type { RedisDatabase } from '../state/database'
import type { RedisServerState } from '../state/server-state'
import type { Unsubscribe } from '../state/mutation-events'
import type { RedisPubSubBroker } from '../state/pubsub-broker'

export type ClientSessionOptions = {
  id?: string
  clientAddress?: string
  server: RedisServerState
  executor: CommandExecutor
  database?: number
  nodeRole?: RedisClusterNodeRole
  signal?: AbortSignal
  park?: ParkHandler
  turnQueue?: RedisTurnQueue
  closeConnection?: (reason?: string) => void
}

/**
 * Mutable view of the turn currently held by an in-flight command. A blocking
 * command suspends its turn while parked and is later resumed on a *new* handle,
 * so the holder must be able to both read the current turn and swap it.
 */
type TurnAccess = {
  get(): RedisTurnHandle | undefined
  set(turn: RedisTurnHandle | undefined): void
}

type WatchRegistration = {
  database: number
  key: Buffer
  unsubscribe: Unsubscribe
}

type PubSubRegistration = {
  value: Buffer
  unsubscribe: Unsubscribe
}

/**
 * Everything that distinguishes one pub/sub kind from the other two. The three
 * `SUBSCRIBE`/`UNSUBSCRIBE` families are otherwise identical bookkeeping, so
 * they are driven from this table instead of being written out per kind.
 */
type PubSubKindSpec = {
  /**
   * Registers the session's listener with the broker and builds the items of
   * every push frame it delivers — `pmessage` carries the matched pattern in
   * front of the channel, the other two do not.
   */
  readonly listen: (
    broker: RedisPubSubBroker,
    target: Buffer,
    emit: (items: RedisValue[]) => void,
  ) => Unsubscribe
  /** Push frame name used to deliver a message to a subscriber. */
  readonly message: 'message' | 'smessage' | 'pmessage'
  /** Confirmation frame name echoed back by SUBSCRIBE/SSUBSCRIBE/PSUBSCRIBE. */
  readonly subscribed: 'subscribe' | 'ssubscribe' | 'psubscribe'
  /** Confirmation frame name echoed back by the matching UNSUBSCRIBE. */
  readonly unsubscribed: 'unsubscribe' | 'sunsubscribe' | 'punsubscribe'
  /**
   * Which counter the confirmation frames report. Redis reports channels and
   * patterns together, but keeps the shard count separate — do not unify.
   */
  readonly counter: 'regular' | 'shard'
}

const PUBSUB_KINDS: Record<PubSubKind, PubSubKindSpec> = {
  channel: {
    listen: (broker, target, emit) =>
      broker.subscribe(target, m => emit([bulk(m.channel), bulk(m.message)])),
    message: 'message',
    subscribed: 'subscribe',
    unsubscribed: 'unsubscribe',
    counter: 'regular',
  },
  shard: {
    listen: (broker, target, emit) =>
      broker.ssubscribe(target, m => emit([bulk(m.channel), bulk(m.message)])),
    message: 'smessage',
    subscribed: 'ssubscribe',
    unsubscribed: 'sunsubscribe',
    counter: 'shard',
  },
  pattern: {
    listen: (broker, target, emit) =>
      broker.psubscribe(target, m =>
        emit([bulk(m.pattern), bulk(m.channel), bulk(m.message)]),
      ),
    message: 'pmessage',
    subscribed: 'psubscribe',
    unsubscribed: 'punsubscribe',
    counter: 'regular',
  },
}

/**
 * `Object.entries` widens the key to `string`; the table is a `Record` over
 * `PubSubKind`, so narrowing it back is sound and keeps the kind list derived
 * from the table instead of restated beside it.
 */
const PUBSUB_KIND_ENTRIES = Object.entries(PUBSUB_KINDS) as [
  PubSubKind,
  PubSubKindSpec,
][]

/**
 * Per-connection server state and the concrete {@link RedisClientSession}.
 *
 * One instance exists per connected client and owns everything that is scoped
 * to that connection rather than to the shared server:
 *  - the selected database index and the resolved {@link RedisDatabase};
 *  - the session {@link ClientSessionMode} (normal / transaction / subscribed);
 *  - the negotiated RESP protocol version (HELLO);
 *  - the cluster READONLY flag (replica reads via READONLY/READWRITE);
 *  - the MULTI command queue and its dirty bit;
 *  - WATCH key registrations for optimistic locking.
 *
 * Commands are serialized through a per-database turn queue so that, even though
 * execution is async, only one command mutates a given database at a time. This
 * is also what makes blocking commands (BLPOP, ...) cooperate instead of
 * deadlock — see {@link createTurnAwareParkHandler}.
 */
export class ClientSession implements RedisClientSession {
  private static nextId = 0

  readonly id: string
  readonly clientAddress?: string
  readonly connectedAtMs: number
  readonly server: RedisServerState
  /** Aborted when the connection closes; threaded into every command's ctx. */
  readonly signal: AbortSignal

  private readonly executor: CommandExecutor
  private readonly signalSource?: AbortController
  private readonly nodeRole?: RedisClusterNodeRole
  private readonly parkHandler: ParkHandler
  private readonly turnQueueOverride?: RedisTurnQueue
  private readonly closeConnection?: (reason?: string) => void
  /**
   * The turn handle of the command currently executing on this session,
   * exposed so {@link executeTransaction} can hand the turn off to another
   * database's queue when a queued SELECT switches databases mid-EXEC.
   */
  private activeTurnAccess?: TurnAccess
  private selectedDatabaseId: number
  private sessionMode: ClientSessionMode = 'normal'
  private respVersion: RespVersion = 2
  private authenticated = false
  /** Set by READONLY, cleared by READWRITE/RESET; lets a replica serve reads. */
  private clusterReadOnlyMode = false
  /** Commands buffered between MULTI and EXEC, in submission order. */
  private transactionPlans: CommandPlan[] = []
  /** True once a queued command errored — forces EXEC to abort with EXECABORT. */
  private transactionDirty = false
  /** Active WATCH registrations, keyed by `db:keyHex`. */
  private readonly watches = new Map<string, WatchRegistration>()
  /** Subset of watched keys mutated since WATCH — non-empty fails the next EXEC. */
  private readonly dirtyWatches = new Set<string>()
  /** Active pub/sub registrations per kind, keyed by `channelOrPatternHex`. */
  private readonly pubsubSubscriptions: Record<
    PubSubKind,
    Map<string, PubSubRegistration>
  > = {
    channel: new Map(),
    shard: new Map(),
    pattern: new Map(),
  }
  private readonly pushQueue: RedisResult[] = []
  private readonly pushWaiters = new Set<() => void>()
  private deferredPushes: RedisResult[] | null = null
  private pushQueueClosed = false
  private readonly responseStreamCleanups = new Set<() => void>()
  private unregisterClientSession?: Unsubscribe

  constructor(options: ClientSessionOptions) {
    this.id = options.id ?? `client-${++ClientSession.nextId}`
    this.clientAddress = options.clientAddress
    this.connectedAtMs = Date.now()
    this.server = options.server
    this.executor = options.executor
    this.selectedDatabaseId = options.database ?? 0
    this.nodeRole = options.nodeRole
    this.parkHandler = options.park ?? createDefaultParkHandler()
    this.turnQueueOverride = options.turnQueue
    this.closeConnection = options.closeConnection

    if (options.signal) {
      this.signal = options.signal
    } else {
      this.signalSource = new AbortController()
      this.signal = this.signalSource.signal
    }

    this.server.getDatabase(this.selectedDatabaseId)
    this.unregisterClientSession = this.server.registerClientSession(this)
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

  get usesSubscribedReplyMode(): boolean {
    return this.sessionMode === 'subscribed' && this.respVersion === 2
  }

  get clusterReadOnly(): boolean {
    return this.clusterReadOnlyMode
  }

  get isAuthenticated(): boolean {
    return this.authenticated
  }

  get pubsubChannelCount(): number {
    return this.pubsubSubscriptions.channel.size
  }

  get pubsubShardChannelCount(): number {
    return this.pubsubSubscriptions.shard.size
  }

  get pubsubPatternCount(): number {
    return this.pubsubSubscriptions.pattern.size
  }

  get pubsubRegularSubscriptionCount(): number {
    return this.pubsubCount('regular')
  }

  get pubsubSubscriptionCount(): number {
    let total = 0
    for (const registrations of Object.values(this.pubsubSubscriptions)) {
      total += registrations.size
    }

    return total
  }

  setAuthenticated(value: boolean): void {
    this.authenticated = value
  }

  /** The live database object for the currently selected index. */
  get db(): RedisDatabase {
    return this.server.getDatabase(this.selectedDatabaseId)
  }

  setProtocolVersion(version: RespVersion): void {
    this.respVersion = version
  }

  /** Toggle replica read mode for this connection (READONLY / READWRITE). */
  setClusterReadOnly(value: boolean): void {
    this.clusterReadOnlyMode = value
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

  /** MULTI: enter transaction mode. Redis forbids nesting. */
  beginTransaction(): void {
    if (this.sessionMode === 'transaction') {
      throw new RedisCommandError('MULTI calls can not be nested')
    }

    this.sessionMode = 'transaction'
    this.transactionPlans = []
    this.transactionDirty = false
  }

  /** Buffer one command while in MULTI; replies "+QUEUED" to the client. */
  queueTransaction(plan: CommandPlan): void {
    if (this.sessionMode !== 'transaction') {
      throw new RedisCommandError('MULTI has not been called')
    }

    this.transactionPlans.push(plan)
  }

  /**
   * EXEC step 1: hand back the queued plans and atomically reset the session to
   * normal mode (clearing the queue, dirty bit, and WATCHes). The caller is
   * responsible for actually running the returned plans via
   * {@link executeTransaction}. Returns an empty list if not in MULTI.
   */
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

  /** DISCARD: drop the queued commands and leave transaction mode. */
  discardTransaction(): void {
    this.transactionPlans = []
    this.sessionMode = 'normal'
    this.transactionDirty = false
    this.unwatch()
  }

  /** Flag the transaction as poisoned (a queued command failed). No-op outside MULTI. */
  markTransactionDirty(): void {
    if (this.sessionMode === 'transaction') {
      this.transactionDirty = true
    }
  }

  isTransactionDirty(): boolean {
    return this.transactionDirty
  }

  /**
   * EXEC step 2: run the drained plans in order and collect their replies into a
   * single array reply. Each command runs in its own fresh execution context.
   * Streaming commands (SUBSCRIBE/MONITOR) are not permitted inside a
   * transaction: the stream is closed immediately and replaced with an error
   * entry so the array stays positionally aligned with the queued commands.
   */
  async executeTransaction(
    plans: readonly CommandPlan[],
  ): Promise<RedisResult> {
    const values: RedisValue[] = []
    const encodedValues: Buffer[] = []
    let sawProtocolSwitch = false

    // Blocking commands must not park while the EXEC turn is held — that would
    // deadlock because no other session could produce the wakeup write. Override
    // park so any blocking command queued in MULTI behaves non-blocking (returns
    // null immediately), matching real Redis BLPOP-inside-MULTI semantics. The
    // handler still consumes the park request (waitFor + abort signal) instead of
    // discarding it — see createNonBlockingParkHandler.
    const noBlockCtx = this.createExecutionContext(
      undefined,
      createNonBlockingParkHandler(),
      undefined,
      true,
    )
    let currentDbId = this.selectedDatabaseId

    for (const plan of plans) {
      if (this.signal.aborted) {
        throw createAbortError()
      }

      // A queued SELECT on a previous iteration may have switched databases.
      // Move the held turn onto the now-selected database's queue so its
      // keyspace stays serialized against other sessions (#94 follow-up). Only
      // one turn is ever held at a time (release-then-acquire), so this cannot
      // deadlock even if two transactions select databases in opposite orders.
      if (this.selectedDatabaseId !== currentDbId) {
        await this.handoffTurnToSelectedDb()
        currentDbId = this.selectedDatabaseId
      }

      // executePlan converts RedisCommandErrors into error results, but a
      // command whose execute() throws an unexpected runtime error (TypeError,
      // etc.) would otherwise propagate out and abandon the partial results
      // array. Real Redis always replies with an N-element EXEC array, so trap
      // the failure into this command's slot and keep running the rest (#83).
      let result: Awaited<ReturnType<typeof this.executor.executePlan>>
      const versionBefore = this.protocolVersion
      try {
        result = await this.executor.executePlan(plan, noBlockCtx)
      } catch (err) {
        if (this.signal.aborted) {
          throw err
        }
        this.appendTransactionValue(
          RedisValue.error((err as Error).message),
          values,
          encodedValues,
        )
        sawProtocolSwitch ||= this.protocolVersion !== versionBefore
        continue
      }

      if (result instanceof RedisResult) {
        this.appendTransactionValue(
          result.value,
          values,
          encodedValues,
          result.encoded,
        )
        sawProtocolSwitch ||= this.protocolVersion !== versionBefore
        continue
      }

      result.close('streaming command is not allowed in transaction')
      this.appendTransactionValue(
        RedisValue.error('Streaming command is not allowed in transaction'),
        values,
        encodedValues,
      )
      sawProtocolSwitch ||= this.protocolVersion !== versionBefore
    }

    const value = RedisValue.array(values)
    if (!sawProtocolSwitch) {
      return RedisResult.create(value)
    }

    return RedisResult.preEncoded(value, encodeTransactionArray(encodedValues))
  }

  private appendTransactionValue(
    value: RedisValue,
    values: RedisValue[],
    encodedValues: Buffer[],
    encoded?: Buffer,
  ): void {
    values.push(value)
    encodedValues.push(
      encoded
        ? Buffer.from(encoded)
        : encodeRedisValue(value, { version: this.protocolVersion }),
    )
  }

  /**
   * Release the currently held serialization turn and acquire a fresh one on
   * the selected database's queue. Called when a queued SELECT switches
   * databases mid-EXEC so subsequent commands run under the correct
   * per-database turn (see {@link executeTransaction}).
   *
   * No-op when a fixed turn-queue override is in force (a single queue already
   * serializes every database) or when no managed turn is active.
   */
  private async handoffTurnToSelectedDb(): Promise<void> {
    if (this.turnQueueOverride) {
      return
    }
    const turnAccess = this.activeTurnAccess
    if (!turnAccess) {
      return
    }

    turnAccess.get()?.release()
    const nextTurn = await this.db.turnQueue.waitTurn()
    turnAccess.set(nextTurn)
  }

  /**
   * WATCH the given keys for optimistic locking. Each key is subscribed in the
   * keyspace; any mutation flips its entry into {@link dirtyWatches}, which a
   * subsequent EXEC checks via {@link isWatchDirty}. Already-watched keys are
   * skipped so re-WATCHing is idempotent.
   */
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

  /** UNWATCH / cleanup: drop every keyspace subscription and clear dirty state. */
  unwatch(): void {
    for (const watch of this.watches.values()) {
      watch.unsubscribe()
    }

    this.watches.clear()
    this.dirtyWatches.clear()
  }

  /** True if any watched key was mutated since WATCH — EXEC must return nil. */
  isWatchDirty(): boolean {
    return this.dirtyWatches.size > 0
  }

  /**
   * SUBSCRIBE / SSUBSCRIBE / PSUBSCRIBE bookkeeping for one kind.
   *
   * Registering an already-subscribed target is a no-op on the broker but still
   * produces a confirmation frame, exactly like real Redis.
   */
  subscribe(kind: PubSubKind, targets: readonly Buffer[]): RedisResult[] {
    const spec = PUBSUB_KINDS[kind]
    const registrations = this.pubsubSubscriptions[kind]
    const frames: RedisResult[] = []

    for (const target of targets) {
      const key = pubsubId(target)
      if (!registrations.has(key)) {
        const value = Buffer.from(target)
        const unsubscribe = spec.listen(
          this.server.pubsubBroker,
          value,
          items => {
            this.enqueuePush(pubsubFrame(spec.message, items))
          },
        )

        registrations.set(key, { value, unsubscribe })
      }

      this.refreshPubSubMode()
      frames.push(
        pubsubFrame(spec.subscribed, [
          bulk(target),
          RedisValue.integer(this.pubsubCount(spec.counter)),
        ]),
      )
    }

    return frames
  }

  /**
   * UNSUBSCRIBE / SUNSUBSCRIBE / PUNSUBSCRIBE bookkeeping for one kind.
   *
   * With no targets Redis drops every current subscription of that kind and
   * replies with a single nil-named frame when there was nothing to drop.
   *
   * Real Redis emits those frames in the iteration order of the client's
   * subscription dict — effectively hash order, and *not* a documented
   * contract: subscribing `k1..k6` to Redis 7.2.1 and sending a bare
   * UNSUBSCRIBE came back as `k4 k2 k1 k5 k6 k3`. We emit in reverse insertion
   * order instead, purely because a mock should be deterministic. Do not pin
   * this order in a test as though it were Redis behavior.
   */
  unsubscribe(kind: PubSubKind, targets: readonly Buffer[]): RedisResult[] {
    const spec = PUBSUB_KINDS[kind]
    const registrations = this.pubsubSubscriptions[kind]
    const resolved =
      targets.length > 0
        ? targets
        : Array.from(registrations.values(), entry => entry.value).reverse()

    if (resolved.length === 0) {
      this.refreshPubSubMode()
      return [
        pubsubFrame(spec.unsubscribed, [
          RedisValue.bulkString(null),
          RedisValue.integer(this.pubsubCount(spec.counter)),
        ]),
      ]
    }

    const frames: RedisResult[] = []
    for (const target of resolved) {
      const key = pubsubId(target)
      const existing = registrations.get(key)
      if (existing) {
        existing.unsubscribe()
        registrations.delete(key)
      }

      this.refreshPubSubMode()
      frames.push(
        pubsubFrame(spec.unsubscribed, [
          bulk(target),
          RedisValue.integer(this.pubsubCount(spec.counter)),
        ]),
      )
    }

    return frames
  }

  resetPubSub(): void {
    for (const registrations of Object.values(this.pubsubSubscriptions)) {
      for (const registration of registrations.values()) {
        registration.unsubscribe()
      }

      registrations.clear()
    }

    this.refreshPubSubMode()
  }

  deferPushesUntilAfterReply(): () => void {
    const deferred: RedisResult[] = []
    this.deferredPushes = deferred
    return () => {
      this.deferredPushes = null
      for (const result of deferred) {
        this.enqueuePush(result)
      }
    }
  }

  registerResponseStreamCleanup(cleanup: () => void): Unsubscribe {
    this.responseStreamCleanups.add(cleanup)
    return () => {
      this.responseStreamCleanups.delete(cleanup)
    }
  }

  resetResponseStreams(): void {
    const cleanups = Array.from(this.responseStreamCleanups)
    this.responseStreamCleanups.clear()

    for (const cleanup of cleanups) {
      cleanup()
    }
  }

  enqueuePush(result: RedisResult): void {
    if (this.pushQueueClosed || this.signal.aborted) {
      return
    }

    if (this.deferredPushes) {
      this.deferredPushes.push(result)
      return
    }

    this.pushQueue.push(result)
    this.wakePushWaiters()
  }

  async *readPushes(signal: AbortSignal): AsyncIterable<RedisResult> {
    while (!signal.aborted && !this.signal.aborted) {
      const frame = this.pushQueue.shift()
      if (frame) {
        yield frame
        continue
      }

      if (this.pushQueueClosed) {
        return
      }

      await this.waitForPush(signal)
    }
  }

  /**
   * Public entry point for executing one client command.
   *
   * Acquires a turn on the database's turn queue before running, guaranteeing
   * serialized access to the keyspace, and always releases it afterward. The
   * acquired turn is exposed to the command via a turn-aware park handler so a
   * blocking command can yield the turn while parked (see
   * {@link createTurnAwareParkHandler}); `turn` is reassigned through the
   * {@link TurnAccess} closure because suspending returns a *new* handle.
   */
  async execute(
    rawCommand: Buffer | string,
    rawArgs: readonly Buffer[],
  ): Promise<ExecutorResult> {
    if (this.signal.aborted) {
      throw createAbortError()
    }

    const turnQueue = this.turnQueueOverride ?? this.db.turnQueue
    let turn: RedisTurnHandle | undefined = await turnQueue.waitTurn()
    const turnAccess: TurnAccess = {
      get: () => turn,
      set: nextTurn => {
        turn = nextTurn
      },
    }
    this.activeTurnAccess = turnAccess
    try {
      const ctx = this.createExecutionContext(turnAccess)
      return await this.executor.executeRaw(rawCommand, rawArgs, ctx)
    } finally {
      this.activeTurnAccess = undefined
      turn?.release()
    }
  }

  /**
   * Build the {@link RedisExecutionContext} passed to a command's `execute`.
   * When a turn is supplied (the normal client path) the context gets a
   * turn-aware park handler so blocking commands release their turn while
   * waiting; without one (e.g. nested transaction execution) the plain park
   * handler is used.
   */
  createExecutionContext(
    turnAccess?: TurnAccess,
    parkOverride?: ParkHandler,
    monitor?: RedisMonitorContext,
    transactionReplay = false,
  ): RedisExecutionContext {
    // `db` is a live getter, not a snapshot: a queued `SELECT N` runs mid-EXEC
    // and updates `selectedDatabaseId`, so every command must resolve the
    // currently selected database at access time, not at context-build time
    // (issue #94). Arrow keeps `this` bound to the session without aliasing.
    const resolveDb = () => this.db
    return {
      get db() {
        return resolveDb()
      },
      server: this.server,
      session: this,
      executor: this.executor,
      ...(transactionReplay ? { transactionReplay } : {}),
      ...(this.nodeRole ? { nodeRole: this.nodeRole } : {}),
      ...(monitor ? { monitor } : {}),
      signal: this.signal,
      park:
        parkOverride ??
        (turnAccess
          ? this.createTurnAwareParkHandler(turnAccess)
          : this.parkHandler),
    }
  }

  /** Tear down the session: abort in-flight work and reset all per-connection state. */
  close(): void {
    this.unregisterClientSession?.()
    this.unregisterClientSession = undefined
    this.resetResponseStreams()
    this.signalSource?.abort()
    this.unwatch()
    this.resetPubSub()
    this.closePushQueue()
    this.transactionPlans = []
    this.transactionDirty = false
    this.sessionMode = 'normal'
    this.clusterReadOnlyMode = false
  }

  disconnect(reason = 'client disconnected'): void {
    if (this.closeConnection) {
      this.closeConnection(reason)
      return
    }

    this.close()
  }

  /**
   * Wrap the base park handler so that parking also yields the command's turn.
   *
   * Blocking commands (BLPOP, BRPOP, ...) must not hold the database turn while
   * they wait, or no other client could ever produce the value that unblocks
   * them — a deadlock. The flow:
   *  1. Start the underlying park, capturing its eventual value.
   *  2. Clear the local turn and call `turn.suspend(parked)`, which releases the
   *     turn back to the queue and resolves with a fresh turn once the park
   *     settles and this session is scheduled again.
   *  3. Store the new turn (so `finally`/subsequent parks see it) and return the
   *     parked value.
   *
   * If there is no current turn, fall back to plain parking.
   */
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

  /**
   * Totals every kind that reports through the given counter — channels and
   * patterns share the 'regular' one, shard channels have their own. Derived
   * from the table rather than hand-enumerated so that adding a kind cannot
   * silently under-count the frames it appears in.
   */
  private pubsubCount(counter: PubSubKindSpec['counter']): number {
    let total = 0
    for (const [kind, spec] of PUBSUB_KIND_ENTRIES) {
      if (spec.counter === counter) {
        total += this.pubsubSubscriptions[kind].size
      }
    }

    return total
  }

  private refreshPubSubMode(): void {
    if (this.pubsubSubscriptionCount > 0) {
      this.sessionMode = 'subscribed'
      return
    }

    if (this.sessionMode === 'subscribed') {
      this.sessionMode = 'normal'
    }
  }

  private waitForPush(signal: AbortSignal): Promise<void> {
    if (signal.aborted || this.signal.aborted || this.pushQueueClosed) {
      return Promise.resolve()
    }

    return new Promise(resolve => {
      const cleanup = () => {
        this.pushWaiters.delete(waiter)
        signal.removeEventListener('abort', waiter)
        this.signal.removeEventListener('abort', waiter)
      }
      const waiter = () => {
        cleanup()
        resolve()
      }

      this.pushWaiters.add(waiter)
      signal.addEventListener('abort', waiter, { once: true })
      this.signal.addEventListener('abort', waiter, { once: true })
    })
  }

  private closePushQueue(): void {
    this.pushQueueClosed = true
    this.pushQueue.length = 0
    this.wakePushWaiters()
  }

  private wakePushWaiters(): void {
    for (const waiter of Array.from(this.pushWaiters)) {
      waiter()
    }
  }
}

function watchId(database: number, key: Buffer): string {
  return `${database}:${key.toString('hex')}`
}

function pubsubId(value: Buffer): string {
  return value.toString('hex')
}

/**
 * Defensive copy — neither broker payloads nor caller-owned command args may
 * alias into an emitted frame.
 */
function bulk(value: Buffer): RedisValue {
  return RedisValue.bulkString(Buffer.from(value))
}

function pubsubFrame(name: string, items: RedisValue[]): RedisResult {
  return RedisResult.create(RedisValue.push(name, items))
}

function encodeTransactionArray(encodedValues: readonly Buffer[]): Buffer {
  return Buffer.concat([
    Buffer.from(`*${encodedValues.length}\r\n`),
    ...encodedValues,
  ])
}

function createAbortError(): Error {
  const err = new Error('The operation was aborted')
  err.name = 'AbortError'
  return err
}
