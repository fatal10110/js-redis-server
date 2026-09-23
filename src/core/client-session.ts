import type { CommandPlan } from './command-definition'
import { CommandExecutor } from './command-executor'
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
import { RedisCommandError, errors } from './redis-error'
import { RedisResult } from './redis-result'
import { RedisValue } from './redis-value'
import { encodeRedisValue, type RespVersion } from './resp-encoder'
import { type RedisTurnHandle } from './turn-queue'
import type { RedisClusterNodeRole } from '../state/cluster-topology'
import type { RedisDatabase } from '../state/database'
import type { RedisServerState } from '../state/server-state'
import type { Unsubscribe } from '../state/mutation-events'
import type { RedisMonitorCommandEvent } from '../state/monitor-feed'
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
 * The command-name prefix each kind's frames carry. All nine frame names are
 * derived from these three, so a row cannot borrow another kind's names.
 */
type PubSubFramePrefix = {
  channel: ''
  shard: 's'
  pattern: 'p'
}

/**
 * Everything that distinguishes one pub/sub kind from the other two. The three
 * `SUBSCRIBE`/`UNSUBSCRIBE` families are otherwise identical bookkeeping, so
 * they are driven from this table instead of being written out per kind.
 *
 * The kind parameter defaults to the full union so the type stays usable bare
 * (see `pubsubCount` and `PUBSUB_KIND_ENTRIES`); the table below binds each row
 * to its own kind, which is what makes a crossed frame name a compile error.
 */
type PubSubKindSpec<TKind extends PubSubKind = PubSubKind> = {
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
  readonly message: `${PubSubFramePrefix[TKind]}message`
  /** Confirmation frame name echoed back by SUBSCRIBE/SSUBSCRIBE/PSUBSCRIBE. */
  readonly subscribed: `${PubSubFramePrefix[TKind]}subscribe`
  /** Confirmation frame name echoed back by the matching UNSUBSCRIBE. */
  readonly unsubscribed: `${PubSubFramePrefix[TKind]}unsubscribe`
  /**
   * Which counter the confirmation frames report. Redis reports channels and
   * patterns together, but keeps the shard count separate — do not unify.
   */
  readonly counter: 'regular' | 'shard'
}

const PUBSUB_KINDS: { readonly [TKind in PubSubKind]: PubSubKindSpec<TKind> } =
  {
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
        broker.ssubscribe(target, m =>
          emit([bulk(m.channel), bulk(m.message)]),
        ),
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
 * `Object.entries` widens the key to `string`; the table is keyed by
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
 * Commands are serialized through the server's single turn queue so that, even
 * though execution is async, only one command runs at a time on any database.
 * This is also what makes blocking commands (BLPOP, ...) cooperate instead of
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
  private readonly closeConnection?: (reason?: string) => void
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
  /** Teardown for push producers (MONITOR); run on RESET and close. */
  private readonly resetHooks = new Set<() => void>()
  /** Set while this connection is in MONITOR mode; leaves it. */
  private stopMonitor?: () => void
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
      throw errors.dbIndexOutOfRange()
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
   *
   * The reply is pre-encoded item by item whenever an item's wire bytes cannot
   * be rebuilt from the array value alone: a queued `HELLO` switched the
   * protocol partway, or a command pre-encoded its own reply (a multi-channel
   * `SUBSCRIBE`, whose extra confirmations Redis appends inside the array).
   */
  async executeTransaction(
    plans: readonly CommandPlan[],
  ): Promise<RedisResult> {
    const values: RedisValue[] = []
    const encodedValues: Buffer[] = []
    let preEncode = false

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
    // The EXEC turn is server-wide, so a queued SELECT switching databases
    // mid-EXEC needs no turn handoff: later commands stay serialized (#94).
    for (const plan of plans) {
      this.signal.throwIfAborted()

      // executePlan converts RedisCommandErrors into error results, but a
      // command whose execute() throws an unexpected runtime error (TypeError,
      // etc.) would otherwise propagate out and abandon the partial results
      // array. Real Redis always replies with an N-element EXEC array, so trap
      // the failure into this command's slot and keep running the rest (#83).
      let result: RedisResult
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
        preEncode ||= this.protocolVersion !== versionBefore
        continue
      }

      this.appendTransactionValue(
        result.value,
        values,
        encodedValues,
        result.encoded,
      )
      preEncode ||=
        this.protocolVersion !== versionBefore || result.encoded !== undefined
    }

    const value = RedisValue.array(values)
    if (!preEncode) {
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
        : encodeRedisValue(value, {
            version: this.protocolVersion,
            profile: this.server.profile,
          }),
    )
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
  pubsubSubscribe(kind: PubSubKind, targets: readonly Buffer[]): RedisResult[] {
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
   * subscription dict. That order is stable within one `redis-server` process
   * and changes across restarts, because the dict hash seed is randomized at
   * startup — so it is not a documented contract and not reproducible. We emit
   * in reverse insertion order instead, purely because a mock should be
   * deterministic. Do not pin this order in a test as though it were Redis
   * behavior.
   */
  pubsubUnsubscribe(
    kind: PubSubKind,
    targets: readonly Buffer[],
  ): RedisResult[] {
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

  get monitoring(): boolean {
    return this.stopMonitor !== undefined
  }

  /**
   * MONITOR: deliver every other client's command to this connection as a push
   * frame, rendered by `frame`, until RESET or close. No-op while already
   * monitoring, so a repeated MONITOR cannot double lines.
   */
  startMonitor(frame: (event: RedisMonitorCommandEvent) => RedisResult): void {
    if (this.stopMonitor) {
      return
    }

    const unsubscribe = this.server.monitorFeed.subscribe(event => {
      if (event.clientId !== this.id) {
        this.enqueuePush(frame(event))
      }
    })
    this.stopMonitor = this.onReset(() => {
      unsubscribe()
      this.stopMonitor = undefined
    })
  }

  /**
   * Register teardown for something that keeps producing pushes for this
   * connection. It runs once, on RESET or close, unless the returned function
   * runs it first.
   */
  onReset(cleanup: () => void): () => void {
    const hook = () => {
      if (this.resetHooks.delete(hook)) {
        cleanup()
      }
    }
    this.resetHooks.add(hook)
    return hook
  }

  /** RESET / close: tear down every producer registered with {@link onReset}. */
  resetPushProducers(): void {
    for (const hook of Array.from(this.resetHooks)) {
      hook()
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
   * Acquires a turn on the server's turn queue before running, guaranteeing
   * serialized access to every database, and always releases it afterward. The
   * acquired turn is exposed to the command via a turn-aware park handler so a
   * blocking command can yield the turn while parked (see
   * {@link createTurnAwareParkHandler}); `turn` is reassigned through the
   * {@link TurnAccess} closure because suspending returns a *new* handle.
   */
  async execute(
    rawCommand: Buffer | string,
    rawArgs: readonly Buffer[],
  ): Promise<RedisResult> {
    this.signal.throwIfAborted()

    let turn: RedisTurnHandle | undefined =
      await this.server.turnQueue.waitTurn()
    const turnAccess: TurnAccess = {
      get: () => turn,
      set: nextTurn => {
        turn = nextTurn
      },
    }
    try {
      const ctx = this.createExecutionContext(turnAccess)
      return await this.executor.executeRaw(rawCommand, rawArgs, ctx)
    } finally {
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
    this.resetPushProducers()
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
   * Blocking commands (BLPOP, BRPOP, ...) must not hold the server turn while
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
