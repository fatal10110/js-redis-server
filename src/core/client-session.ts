import type { CommandPlan } from './command-definition'
import { CommandExecutor } from './command-executor'
import { applyClientReplyMode } from './client-reply'
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
import { RedisResult, type RedisResultOptions } from './redis-result'
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
  /** The server's end of the connection, `host:port` (CLIENT LIST `laddr=`). */
  localAddress?: string
  /** The socket's file descriptor (CLIENT LIST `fd=`), when it has one. */
  fd?: number
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
  readonly localAddress?: string
  readonly fd?: number
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
  private clusterAskingMode = false
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
  /** Pushes for this connection made by the command EXEC is running. */
  private transactionPushes: RedisResult[] | null = null
  private pushQueueClosed = false
  /** Teardown for push producers (MONITOR); run on RESET and close. */
  private readonly resetHooks = new Set<() => void>()
  /** Set while this connection is in MONITOR mode; leaves it. */
  private stopMonitor?: () => void
  /**
   * The monitor lines of this connection's own command, collected while it
   * runs and delivered after its reply (see {@link execute}). Set only while
   * a call that started in MONITOR mode holds the turn.
   */
  private ownMonitorLines: RedisResult[] | null = null
  private unregisterClientSession?: Unsubscribe
  private lastCommandName: string | null = null
  private lastInteraction: number
  /** Aborted by {@link endInput}; aborts every park from then on. */
  private readonly inputEnd = new AbortController()
  /** Commands of this session parked right now (see {@link onPark}). */
  private parkedCommands = 0
  private readonly parkListeners = new Set<() => void>()
  /**
   * While EXEC runs its queue: how many commands it holds. Redis keeps the
   * client's `CLIENT_MULTI` flag and `mstate.count` until EXEC is done, so a
   * queued CLIENT INFO / LIST sees `flags=x multi=<count>`; -1 otherwise.
   */
  private execQueuedCount = -1
  /**
   * Whether MULTI or WATCH ever ran: Valkey 8.1+ allocates the client's multi
   * state then and keeps it, so CLIENT INFO shows `multi=0` from then on
   * (`client.info-multi-lazy-state`).
   */
  private multiStateAllocated = false

  constructor(options: ClientSessionOptions) {
    this.id = options.id ?? `client-${++ClientSession.nextId}`
    this.clientAddress = options.clientAddress
    this.localAddress = options.localAddress
    this.fd = options.fd
    this.connectedAtMs = Date.now()
    this.lastInteraction = this.connectedAtMs
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

  get clusterAsking(): boolean {
    return this.clusterAskingMode
  }

  get lastCommand(): string | null {
    return this.lastCommandName
  }

  get lastInteractionMs(): number {
    return this.lastInteraction
  }

  get queuedCommandCount(): number {
    return this.sessionMode === 'transaction'
      ? this.transactionPlans.length
      : this.execQueuedCount
  }

  get clientInfoMultiCount(): number {
    const count = this.queuedCommandCount
    return count === -1 &&
      this.multiStateAllocated &&
      this.server.profile.has('client.info-multi-lazy-state')
      ? 0
      : count
  }

  get watchedKeyCount(): number {
    return this.watches.size
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

  /** ASKING: the next command may use a slot this node is importing. */
  setClusterAsking(value: boolean): void {
    this.clusterAskingMode = value
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
    this.multiStateAllocated = true
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
    this.leaveTransactionMode()
    this.transactionDirty = false
    this.unwatch()
    return plans
  }

  /** DISCARD: drop the queued commands and leave transaction mode. */
  discardTransaction(): void {
    this.transactionPlans = []
    this.leaveTransactionMode()
    this.transactionDirty = false
    this.unwatch()
  }

  /**
   * Back to normal mode, or to subscribed mode: a RESP3 connection may
   * MULTI while subscribed and stays subscribed after EXEC / DISCARD.
   */
  private leaveTransactionMode(): void {
    if (this.sessionMode === 'transaction') {
      this.sessionMode = 'normal'
    }
    this.refreshPubSubMode()
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
   * protocol partway, a command pre-encoded its own reply (a multi-channel
   * `SUBSCRIBE`, whose extra confirmations Redis appends inside the array),
   * or a push for this connection is written inline (below).
   *
   * Pushes a queued command produces for this connection (its PUBLISH to a
   * channel it subscribes to) follow the EXEC reply on Redis 7.2+ / valkey
   * 8.0+ (`server.pending_push_messages`, flushed after the outermost call);
   * older versions write them into the output buffer as they happen, inside
   * the array ahead of the item that produced them. Each item's `afterReply`
   * and `trailingFrames` move to the EXEC reply (#509).
   */
  async executeTransaction(
    plans: readonly CommandPlan[],
  ): Promise<RedisResult> {
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
    this.execQueuedCount = plans.length
    try {
      return await this.runTransactionPlans(plans, noBlockCtx)
    } finally {
      this.execQueuedCount = -1
    }
  }

  private async runTransactionPlans(
    plans: readonly CommandPlan[],
    noBlockCtx: RedisExecutionContext,
  ): Promise<RedisResult> {
    const values: RedisValue[] = []
    const encodedValues: Buffer[] = []
    const trailingFrames: RedisValue[] = []
    const heldPushes: RedisResult[] = []
    const afterReplies: (() => void)[] = []
    const pushesAfterReply = this.server.profile.has(
      'pubsub.resp3-publish-reply-first',
    )
    let preEncode = false

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
      const pushes: RedisResult[] = []
      this.transactionPushes = pushes
      try {
        result = await this.executor.executePlan(plan, noBlockCtx)
      } catch (err) {
        if (this.signal.aborted) {
          throw err
        }
        result = RedisResult.error((err as Error).message)
      } finally {
        this.transactionPushes = null
      }

      this.appendTransactionValue(
        result.value,
        values,
        encodedValues,
        result.encoded,
      )
      preEncode ||=
        this.protocolVersion !== versionBefore || result.encoded !== undefined

      if (pushes.length > 0 && pushesAfterReply) {
        heldPushes.push(...pushes)
      } else if (pushes.length > 0) {
        const last = encodedValues.length - 1
        encodedValues[last] = Buffer.concat([
          ...pushes.map(push => this.encodePush(push)),
          encodedValues[last],
        ])
        trailingFrames.push(...pushes.map(push => push.value))
        preEncode = true
      }

      trailingFrames.push(...(result.options?.trailingFrames ?? []))
      if (result.options?.afterReply) {
        afterReplies.push(result.options.afterReply)
      }
    }

    const value = RedisValue.array(values)
    const options = this.transactionReplyOptions(
      trailingFrames,
      heldPushes,
      afterReplies,
    )
    if (!preEncode) {
      return RedisResult.create(value, options)
    }

    return RedisResult.preEncoded(
      value,
      encodeTransactionArray(encodedValues),
      options,
    )
  }

  private transactionReplyOptions(
    trailingFrames: readonly RedisValue[],
    heldPushes: readonly RedisResult[],
    afterReplies: readonly (() => void)[],
  ): RedisResultOptions | undefined {
    if (
      trailingFrames.length === 0 &&
      heldPushes.length === 0 &&
      afterReplies.length === 0
    ) {
      return undefined
    }

    return {
      ...(trailingFrames.length > 0 ? { trailingFrames } : {}),
      afterReply: () => {
        for (const push of heldPushes) {
          this.enqueuePush(push)
        }
        for (const afterReply of afterReplies) {
          afterReply()
        }
      },
    }
  }

  private encodePush(push: RedisResult): Buffer {
    return push.encoded
      ? Buffer.from(push.encoded)
      : encodeRedisValue(push.value, {
          version: this.protocolVersion,
          profile: this.server.profile,
        })
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
    this.multiStateAllocated = true
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
    // The command's own hold (see execute) already delivers after the reply.
    if (this.deferredPushes) {
      return () => {}
    }
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
   * MONITOR: deliver every client's command to this connection as a push
   * frame, rendered by `frame`, until RESET or close. No-op while already
   * monitoring, so a repeated MONITOR cannot double lines.
   *
   * Real Redis feeds the monitoring connection its own commands too (#456):
   * `call()` adds the line after the command's reply, so a line from this
   * connection's own command waits in {@link ownMonitorLines} until the
   * reply is out, and every other line waits behind it
   * ({@link holdPushesUntilReply}).
   */
  startMonitor(frame: (event: RedisMonitorCommandEvent) => RedisResult): void {
    if (this.stopMonitor) {
      return
    }

    const unsubscribe = this.server.monitorFeed.subscribe(event => {
      const line = frame(event)
      if (event.clientId === this.id && this.ownMonitorLines) {
        this.ownMonitorLines.push(line)
        return
      }

      this.enqueuePush(line)
    })
    this.stopMonitor = this.onReset(() => {
      unsubscribe()
      this.stopMonitor = undefined
    })
  }

  /**
   * Register teardown for something that keeps producing pushes for this
   * connection. It runs once, on RESET or close, unless the returned function
   * runs it first. A cleanup that throws there cannot stop the rest of the
   * teardown: RESET and close run every other hook and finish, and the error
   * is dropped (#509).
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
      try {
        hook()
      } catch {
        // Teardown must complete; see onReset.
      }
    }
  }

  enqueuePush(result: RedisResult): void {
    if (this.pushQueueClosed || this.signal.aborted) {
      return
    }

    if (this.transactionPushes) {
      this.transactionPushes.push(result)
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
   * Resolves true once a push is queued, false once the queue is closed or a
   * signal aborts. A transport pairs it with {@link takePushes} to move every
   * queued push onto its output at once.
   */
  async waitForPushes(signal: AbortSignal): Promise<boolean> {
    while (!signal.aborted && !this.signal.aborted) {
      if (this.pushQueue.length > 0) {
        return true
      }
      if (this.pushQueueClosed) {
        return false
      }
      await this.waitForPush(signal)
    }
    return false
  }

  /** Every push queued so far, oldest first, removed from the queue. */
  takePushes(): RedisResult[] {
    return this.pushQueue.splice(0)
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
    // Call-scoped: re-installed whenever this call gets the turn back, so
    // overlapping calls (one parked, another running) never swap lines.
    const ownLines: RedisResult[] | null = this.monitoring ? [] : null
    this.ownMonitorLines = ownLines
    // Redis 7.2+ and valkey 8.0+ hold every push for the calling connection
    // made while its command runs (a keyspace notification, its own PUBLISH,
    // a script's inner calls) until the reply is out
    // (`server.pending_push_messages`, joined after the outermost call).
    // Older servers write each push as it is made.
    const holdsOwnPushes = this.server.profile.has(
      'pubsub.resp3-publish-reply-first',
    )
    let ownPushes: RedisResult[] | null = null
    const holdOwnPushes = () => {
      if (holdsOwnPushes && !this.deferredPushes) {
        ownPushes = []
        this.deferredPushes = ownPushes
      }
    }
    // A blocked command's call returns before it has a reply: what it held
    // so far goes out then, and pushes others make meanwhile are not held.
    const flushOwnPushes = () => {
      const held = ownPushes
      ownPushes = null
      if (!held) {
        return
      }
      if (this.deferredPushes === held) {
        this.deferredPushes = null
      }
      for (const push of held) {
        this.enqueuePush(push)
      }
    }
    holdOwnPushes()
    const turnAccess: TurnAccess = {
      get: () => turn,
      set: nextTurn => {
        turn = nextTurn
        this.ownMonitorLines = nextTurn ? ownLines : null
        if (nextTurn) {
          holdOwnPushes()
        } else {
          flushOwnPushes()
        }
      },
    }
    let result: RedisResult | undefined
    let releaseHeld: ((deliver: boolean) => void) | undefined
    try {
      // Redis records the command as the client's last one (`cmd=` in CLIENT
      // LIST) at lookup, before any check can refuse it, and stamps the
      // interaction when the command is read and again when it is answered.
      const commandName = this.executor.lookupCommandName(rawCommand, rawArgs)
      this.lastCommandName = commandName
      this.lastInteraction = Date.now()
      const ctx = this.createExecutionContext(turnAccess)
      // CLIENT REPLY OFF / SKIP drop the reply of every command processed,
      // refused ones included, as real Redis checks before writing any reply.
      result = applyClientReplyMode(
        this,
        await this.executor.executeRaw(rawCommand, rawArgs, ctx),
      )
      // ASKING lasts one command (`resetClient`): any other command clears
      // it once processed, refused or not, unless it leaves the client in
      // MULTI, where the flag holds until EXEC or DISCARD is done.
      if (commandName !== 'asking' && this.sessionMode !== 'transaction') {
        this.clusterAskingMode = false
      }
    } finally {
      this.lastInteraction = Date.now()
      this.ownMonitorLines = null
      releaseHeld = this.holdPushesUntilReply(ownPushes, ownLines)
      turn?.release()
      // A throw has no reply to wait for.
      if (!result) {
        releaseHeld?.(true)
      }
    }

    return this.deliverHeldPushesAfter(result, releaseHeld)
  }

  /**
   * Keep holding every push from here until the reply is written: real Redis
   * appends to one output buffer, so the reply comes first, then the pushes
   * the command made (`ownPushes`, the hold {@link execute} took), then its
   * own monitor lines, then whatever other clients run once the turn is
   * released. Returns the release (deliver or drop), or nothing when there
   * is no hold to take: no own hold and not monitoring, or a hold is already
   * up (MONITOR's own, or an overlapping call's), which its `afterReply`
   * releases and the own lines join.
   */
  private holdPushesUntilReply(
    ownPushes: RedisResult[] | null,
    ownLines: readonly RedisResult[] | null,
  ): ((deliver: boolean) => void) | undefined {
    // Nothing held and nothing to receive: let go (the monitor hold below
    // still applies).
    if (
      ownPushes &&
      this.deferredPushes === ownPushes &&
      ownPushes.length === 0 &&
      this.pubsubSubscriptionCount === 0
    ) {
      this.deferredPushes = null
    }

    if (ownPushes && this.deferredPushes === ownPushes) {
      const held = ownPushes
      const madeByCommand = held.length
      held.push(...(ownLines ?? []))
      return deliver => {
        if (this.deferredPushes === held) {
          this.deferredPushes = null
        }
        // A closing command's pending pushes still join its reply
        // (`listJoin` skips the CLIENT_CLOSE_AFTER_REPLY check); later
        // output is dropped.
        for (const push of deliver ? held : held.slice(0, madeByCommand)) {
          this.enqueuePush(push)
        }
      }
    }

    if (!this.monitoring) {
      return undefined
    }

    if (this.deferredPushes) {
      this.deferredPushes.push(...(ownLines ?? []))
      return undefined
    }

    const held: RedisResult[] = [...(ownLines ?? [])]
    this.deferredPushes = held
    return deliver => {
      if (this.deferredPushes === held) {
        this.deferredPushes = null
      }
      if (!deliver) {
        return
      }
      for (const push of held) {
        this.enqueuePush(push)
      }
    }
  }

  /**
   * Release {@link holdPushesUntilReply}'s hold once the reply is written. A
   * command that closes the connection (QUIT) drops it: Redis discards output
   * added after `CLIENT_CLOSE_AFTER_REPLY` is set.
   */
  private deliverHeldPushesAfter(
    result: RedisResult,
    releaseHeld: ((deliver: boolean) => void) | undefined,
  ): RedisResult {
    if (!releaseHeld) {
      return result
    }

    const deliver = !result.options?.close && !result.options?.disconnect
    const afterReply = result.options?.afterReply
    return new RedisResult(
      result.value,
      {
        ...result.options,
        // Released first: the command's `afterReply` (EXEC's held pushes)
        // goes out after what the command itself made.
        afterReply: () => {
          releaseHeld(deliver)
          afterReply?.()
        },
      },
      result.encoded,
    )
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

  /** Whether one of this session's commands is parked (BLPOP, XREAD BLOCK, ...). */
  get parked(): boolean {
    return this.parkedCommands > 0
  }

  /**
   * Call `listener` each time a command of this session parks. The transport
   * adapter uses it to keep reading the socket while the client is blocked,
   * as Redis keeps reading a blocked client's input (#554).
   */
  onPark(listener: () => void): Unsubscribe {
    this.parkListeners.add(listener)
    return () => this.parkListeners.delete(listener)
  }

  /**
   * The client's input has ended (EOF, or the socket went away). Redis frees
   * a blocked client as soon as its read returns 0, so a command parked now,
   * or one that parks later from a frame read before the EOF, is aborted. A
   * command that does not park still runs and is answered, as Redis answers
   * the commands it read before the EOF.
   */
  endInput(): void {
    this.inputEnd.abort()
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
    this.clusterAskingMode = false
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
      const parkRequest: ParkRequest<TValue> = {
        ...request,
        signal: AbortSignal.any([request.signal, this.inputEnd.signal]),
      }
      this.parkedCommands += 1
      try {
        for (const listener of [...this.parkListeners]) listener()

        const turn = turnAccess.get()
        if (!turn) {
          return await this.parkHandler(parkRequest)
        }

        let parkedValue: TValue | null = null
        const parked = this.parkHandler(parkRequest).then(value => {
          parkedValue = value
        })

        turnAccess.set(undefined)
        const nextTurn = await turn.suspend(parked, resume =>
          request.onWake?.(resume),
        )
        turnAccess.set(nextTurn)
        return parkedValue
      } finally {
        this.parkedCommands -= 1
      }
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
