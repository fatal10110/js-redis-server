import { EventEmitter } from 'node:events'
import { createRedisCommandExecutor } from '../commands'
import { buildClusterNodes, type ClusterNodePipeline } from '../cluster'
import { ClientSession } from '../core/client-session'
import type { CommandExecutor } from '../core/command-executor'
import { RedisCommandError, RedisCrossSlotError } from '../core/redis-error'
import { RedisResult } from '../core/redis-result'
import type { RedisValue } from '../core/redis-value'
import { isResponseStream, type ResponseStream } from '../core/response-stream'
import { RedisServerState, RedisClusterTopology } from '../state'

/**
 * In-memory drop-in for the `node-redis` (`redis` package) client.
 *
 * node-redis exposes no socket/connector hook (its `#socketFactory` is private),
 * so unlike the ioredis path we cannot drive the *real* client over a virtual
 * socket. Instead this is a **hand-written facade** (the plan's "Tier 2") that
 * replicates node-redis' public surface and routes every command through the
 * shared in-memory command pipeline via a per-connection {@link ClientSession} —
 * the exact same pipeline used by the networked server, MULTI/EXEC and Lua.
 *
 * Scope is deliberately honest: a curated set of camelCase methods carries
 * node-redis-correct return types, and EVERYTHING ELSE falls through to the
 * generic {@link NodeRedisMockClient.sendCommand} escape hatch, which decodes
 * replies to native JS via the shared {@link decodeReply}. This avoids the
 * `ioredis-mock` whack-a-mole trap of hand-shaping every command's reply.
 */

const DEFAULT_DATABASE_COUNT = 16

// node-redis throws its own error types (`instanceof WatchError` / `ErrorReply`
// is the documented user idiom), so when the `redis` package is present — it
// always is, since it's the thing being mocked — surface the real classes.
// Resolved once, before any client is returned, and cached for the synchronous
// decode path. Falls back to shape-compatible local classes if `redis` is
// somehow absent.
type RedisErrorConstructors = {
  WatchError: new (message?: string) => Error
  ErrorReply: new (message: string) => Error
  MultiErrorReply: new (replies: unknown[], errorIndexes: number[]) => Error
}

class FacadeWatchError extends Error {
  constructor() {
    super('One (or more) of the watched keys has been changed')
    this.name = 'WatchError'
  }
}
class FacadeErrorReply extends Error {}
class FacadeMultiErrorReply extends FacadeErrorReply {
  constructor(
    readonly replies: unknown[],
    readonly errorIndexes: number[],
  ) {
    super('One or more commands in the MULTI/EXEC failed')
    this.name = 'MultiErrorReply'
  }
  *errors(): IterableIterator<unknown> {
    for (const index of this.errorIndexes) {
      yield this.replies[index]
    }
  }
}

const FALLBACK_REDIS_ERRORS: RedisErrorConstructors = {
  WatchError: FacadeWatchError,
  ErrorReply: FacadeErrorReply,
  MultiErrorReply: FacadeMultiErrorReply,
}

let resolvedRedisErrors: RedisErrorConstructors | undefined

async function ensureRedisErrors(): Promise<RedisErrorConstructors> {
  if (resolvedRedisErrors) {
    return resolvedRedisErrors
  }
  try {
    const redis = (await import('redis')) as unknown as
      | Partial<RedisErrorConstructors>
      | undefined
    resolvedRedisErrors =
      redis?.WatchError && redis.ErrorReply && redis.MultiErrorReply
        ? {
            WatchError: redis.WatchError,
            ErrorReply: redis.ErrorReply,
            MultiErrorReply: redis.MultiErrorReply,
          }
        : FALLBACK_REDIS_ERRORS
  } catch {
    resolvedRedisErrors = FALLBACK_REDIS_ERRORS
  }
  return resolvedRedisErrors
}

function errorReplyText(value: { code?: string; message: string }): string {
  // Reconstruct the on-the-wire `CODE message` (e.g. `WRONGTYPE Operation …`)
  // so the surfaced text matches what node-redis parses off the wire.
  return value.code ? `${value.code} ${value.message}` : value.message
}

/** A command argument node-redis accepts on the wire. */
export type NodeRedisCommandArgument = string | Buffer

/** Native JS value a reply decodes to (mirrors node-redis RESP2 defaults). */
export type NodeRedisReply =
  | string
  | number
  | bigint
  | boolean
  | Buffer
  | null
  | NodeRedisReply[]
  | { [key: string]: NodeRedisReply }

export type NodeRedisMockClusterOptions = {
  masters: number
  replicas?: number
}

export type CreateNodeRedisMockOptions =
  | { cluster?: undefined; databaseCount?: number }
  | { cluster: NodeRedisMockClusterOptions }

/** A single `{ score, value }` member for ZADD-style methods. */
export type NodeRedisZMember = { score: number; value: string }

/** Listener invoked for each delivered pub/sub message: `(message, channel)`. */
export type NodeRedisPubSubListener = (message: string, channel: string) => void

/**
 * Creates an in-memory node-redis-shaped client (standalone) or cluster client.
 * Pass `{ cluster: { masters } }` for a {@link NodeRedisMockCluster}; otherwise
 * a {@link NodeRedisMockClient} backed by a single in-memory pipeline.
 */
export async function createNodeRedisMock(
  options: CreateNodeRedisMockOptions = {},
): Promise<NodeRedisMockClient | NodeRedisMockCluster> {
  // Resolve node-redis' error classes before any command can decode/throw, so
  // the synchronous decode path can surface real WatchError/ErrorReply types.
  await ensureRedisErrors()
  if ('cluster' in options && options.cluster) {
    return NodeRedisMockCluster.create(options.cluster)
  }
  const databaseCount =
    ('databaseCount' in options ? options.databaseCount : undefined) ??
    DEFAULT_DATABASE_COUNT
  const state = new RedisServerState({ databaseCount })
  const executor = createRedisCommandExecutor()
  return new NodeRedisMockClient({ state, executor, ownsState: true })
}

type FacadeBackend = {
  state: RedisServerState
  executor: CommandExecutor
}

/**
 * Shared command-method surface implemented by both the standalone client and
 * the cluster client. Standalone runs every command on its own session; the
 * cluster routes by slot to the owning node's session — but the curated method
 * bodies are identical, so they live in this base and dispatch through the
 * abstract {@link CommandRunner.run}.
 */
abstract class CommandRunner {
  /**
   * Execute one already-tokenised command and return its decoded reply.
   * Implementations pick the session (standalone: the only one; cluster: the
   * slot owner for the command's keys).
   */
  protected abstract run(args: NodeRedisCommandArgument[]): Promise<RedisValue>

  /** Generic escape hatch for any command, decoded to a native JS reply. */
  async sendCommand(args: NodeRedisCommandArgument[]): Promise<NodeRedisReply> {
    return decodeReply(await this.run(args))
  }

  // --- strings -------------------------------------------------------------

  async get(key: string): Promise<string | null> {
    return asStringOrNull(await this.run(['GET', key]))
  }

  async set(
    key: string,
    value: string | number,
    ...rest: NodeRedisCommandArgument[]
  ): Promise<string | null> {
    return asStringOrNull(await this.run(['SET', key, String(value), ...rest]))
  }

  async del(...keys: string[]): Promise<number> {
    return asNumber(await this.run(['DEL', ...keys]))
  }

  async exists(...keys: string[]): Promise<number> {
    return asNumber(await this.run(['EXISTS', ...keys]))
  }

  async incr(key: string): Promise<number> {
    return asNumber(await this.run(['INCR', key]))
  }

  async expire(key: string, seconds: number): Promise<number> {
    // node-redis' EXPIRE has no transformReply — it passes the raw 0/1 integer
    // through as a number (not a boolean).
    return asNumber(await this.run(['EXPIRE', key, String(seconds)]))
  }

  async ttl(key: string): Promise<number> {
    return asNumber(await this.run(['TTL', key]))
  }

  // --- hashes --------------------------------------------------------------

  async hSet(key: string, field: string, value: string): Promise<number> {
    return asNumber(await this.run(['HSET', key, field, value]))
  }

  async hGet(key: string, field: string): Promise<string | null> {
    return asStringOrNull(await this.run(['HGET', key, field]))
  }

  async hGetAll(key: string): Promise<{ [field: string]: string }> {
    const reply = decodeReply(await this.run(['HGETALL', key]))
    // decode() turns a RESP2 map reply into a plain object already.
    return (reply as { [field: string]: string }) ?? {}
  }

  // --- lists ---------------------------------------------------------------

  async lPush(key: string, ...values: string[]): Promise<number> {
    return asNumber(await this.run(['LPUSH', key, ...values]))
  }

  async rPush(key: string, ...values: string[]): Promise<number> {
    return asNumber(await this.run(['RPUSH', key, ...values]))
  }

  async lRange(key: string, start: number, stop: number): Promise<string[]> {
    return asStringArray(
      await this.run(['LRANGE', key, String(start), String(stop)]),
    )
  }

  // --- sets ----------------------------------------------------------------

  async sAdd(key: string, ...members: string[]): Promise<number> {
    return asNumber(await this.run(['SADD', key, ...members]))
  }

  async sMembers(key: string): Promise<string[]> {
    return asStringArray(await this.run(['SMEMBERS', key]))
  }

  // --- sorted sets ---------------------------------------------------------

  async zAdd(
    key: string,
    members: NodeRedisZMember | NodeRedisZMember[],
  ): Promise<number> {
    const list = Array.isArray(members) ? members : [members]
    const args = ['ZADD', key]
    for (const { score, value } of list) {
      args.push(String(score), value)
    }
    return asNumber(await this.run(args))
  }

  async zRange(key: string, start: number, stop: number): Promise<string[]> {
    return asStringArray(
      await this.run(['ZRANGE', key, String(start), String(stop)]),
    )
  }
}

export type NodeRedisMockClientInit = FacadeBackend & {
  database?: number
  /** True only for the client that created the state — it owns its teardown. */
  ownsState?: boolean
}

/**
 * Standalone in-memory node-redis facade. Drives one {@link ClientSession}
 * against a single keyspace; pub/sub uses a *dedicated* session that drains
 * pushes off {@link ClientSession.readPushes}, mirroring node-redis' rule that a
 * subscribed connection is reserved for pub/sub.
 */
export class NodeRedisMockClient extends CommandRunner {
  private readonly emitter = new EventEmitter()
  private readonly backend: FacadeBackend
  private readonly database?: number
  private readonly ownsState: boolean
  private readonly session: ClientSession
  /**
   * Serializes commands on this client the way a real single node-redis
   * connection does, so a concurrent (un-awaited) call cannot interleave between
   * the MULTI and EXEC of a transaction sharing {@link session}.
   */
  private commandLock: Promise<unknown> = Promise.resolve()
  /** Dedicated session + push-reader loop, created lazily on first subscribe. */
  private pubsub?: {
    session: ClientSession
    abort: AbortController
    listeners: Map<string, Set<NodeRedisPubSubListener>>
    patternListeners: Map<string, Set<NodeRedisPubSubListener>>
    /** The push-draining loop; awaited on teardown so nothing dangles. */
    drained: Promise<void>
  }
  private closed = false

  constructor(init: NodeRedisMockClientInit) {
    super()
    this.backend = { state: init.state, executor: init.executor }
    this.database = init.database
    this.ownsState = init.ownsState ?? false
    this.session = new ClientSession({
      server: init.state,
      executor: init.executor,
      database: init.database,
    })
    // node-redis emits 'connect' then 'ready' once the handshake completes.
    queueMicrotask(() => {
      this.emitter.emit('connect')
      this.emitter.emit('ready')
    })
  }

  on(event: string, listener: (...args: unknown[]) => void): this {
    this.emitter.on(event, listener)
    return this
  }

  once(event: string, listener: (...args: unknown[]) => void): this {
    this.emitter.once(event, listener)
    return this
  }

  off(event: string, listener: (...args: unknown[]) => void): this {
    this.emitter.off(event, listener)
    return this
  }

  /** node-redis clients require an explicit connect(); here it is a no-op. */
  async connect(): Promise<this> {
    return this
  }

  /** A fresh, independent client over the **same** shared keyspace. */
  async duplicate(): Promise<NodeRedisMockClient> {
    return new NodeRedisMockClient({
      ...this.backend,
      database: this.database,
    })
  }

  protected run(args: NodeRedisCommandArgument[]): Promise<RedisValue> {
    return this.runExclusive(() =>
      runOnSession(this.session, args, this.closed),
    )
  }

  /** Run `fn` after any in-flight command/transaction on this client settles. */
  private runExclusive<T>(fn: () => Promise<T>): Promise<T> {
    const result = this.commandLock.then(fn, fn)
    this.commandLock = result.then(
      () => undefined,
      () => undefined,
    )
    return result
  }

  // --- pub/sub -------------------------------------------------------------

  async publish(channel: string, message: string): Promise<number> {
    return asNumber(await this.run(['PUBLISH', channel, message]))
  }

  async subscribe(
    channel: string,
    listener: NodeRedisPubSubListener,
  ): Promise<void> {
    const pubsub = this.ensurePubSub()
    addListener(pubsub.listeners, channel, listener)
    await runOnSession(pubsub.session, ['SUBSCRIBE', channel], this.closed)
  }

  async pSubscribe(
    pattern: string,
    listener: NodeRedisPubSubListener,
  ): Promise<void> {
    const pubsub = this.ensurePubSub()
    addListener(pubsub.patternListeners, pattern, listener)
    await runOnSession(pubsub.session, ['PSUBSCRIBE', pattern], this.closed)
  }

  async unsubscribe(channel?: string): Promise<void> {
    if (!this.pubsub) {
      return
    }
    const args = channel ? ['UNSUBSCRIBE', channel] : ['UNSUBSCRIBE']
    await runOnSession(this.pubsub.session, args, this.closed)
    if (channel) {
      this.pubsub.listeners.delete(channel)
    } else {
      this.pubsub.listeners.clear()
    }
  }

  async pUnsubscribe(pattern?: string): Promise<void> {
    if (!this.pubsub) {
      return
    }
    const args = pattern ? ['PUNSUBSCRIBE', pattern] : ['PUNSUBSCRIBE']
    await runOnSession(this.pubsub.session, args, this.closed)
    if (pattern) {
      this.pubsub.patternListeners.delete(pattern)
    } else {
      this.pubsub.patternListeners.clear()
    }
  }

  // --- transactions --------------------------------------------------------

  async watch(...keys: string[]): Promise<string> {
    return asString(await this.run(['WATCH', ...keys]))
  }

  async unwatch(): Promise<string> {
    return asString(await this.run(['UNWATCH']))
  }

  /** Begin a MULTI transaction. Commands are queued, then replayed on exec(). */
  multi(): NodeRedisMockMulti {
    return new NodeRedisMockMulti(queued =>
      this.runExclusive(() => this.runTransactionSpan(queued)),
    )
  }

  /** Replay MULTI → queued commands → EXEC on the shared session as one span. */
  private async runTransactionSpan(
    queued: NodeRedisCommandArgument[][],
  ): Promise<RedisValue> {
    await runOnSession(this.session, ['MULTI'], this.closed)
    for (const args of queued) {
      await runOnSession(this.session, args, this.closed)
    }
    return runOnSession(this.session, ['EXEC'], this.closed)
  }

  // --- lifecycle -----------------------------------------------------------

  /** Gracefully close: tear down pub/sub + the command session. */
  async quit(): Promise<string> {
    await this.teardown()
    this.emitter.emit('end')
    return 'OK'
  }

  /** Hard close (node-redis `disconnect()`). Same teardown as quit(). */
  async disconnect(): Promise<void> {
    await this.teardown()
    this.emitter.emit('end')
  }

  /**
   * node-redis exposes a synchronous destroy(); abort everything immediately.
   * The push-drain loop settles on the next tick via the abort signal.
   */
  destroy(): void {
    // teardown() is async (it awaits the push-drain loop); surface a rejection
    // as an 'error' event rather than dropping it as an unhandled rejection.
    void this.teardown().catch(err => this.emitter.emit('error', err))
    this.emitter.emit('end')
  }

  private async teardown(): Promise<void> {
    if (this.closed) {
      return
    }
    this.closed = true
    this.session.close()
    if (this.pubsub) {
      const pubsub = this.pubsub
      this.pubsub = undefined
      pubsub.abort.abort()
      pubsub.session.close()
      // Wait for the drain loop to observe the abort and finish, so no async
      // iteration dangles past teardown.
      await pubsub.drained
      pubsub.listeners.clear()
      pubsub.patternListeners.clear()
    }
    if (this.ownsState) {
      // Only the creating client owns the state graph; closing it clears the
      // self-rescheduling active-expiry timer. Duplicates share this state and
      // must not close it out from under their siblings.
      this.backend.state.close()
    }
  }

  private ensurePubSub(): NonNullable<NodeRedisMockClient['pubsub']> {
    if (this.pubsub) {
      return this.pubsub
    }

    const abort = new AbortController()
    const session = new ClientSession({
      server: this.backend.state,
      executor: this.backend.executor,
      database: this.database,
    })
    // Drain pushes for the lifetime of the subscription; route each delivered
    // message/pmessage frame to the registered listeners. The promise is held
    // so teardown can await its completion.
    const pubsub: NonNullable<NodeRedisMockClient['pubsub']> = {
      session,
      abort,
      listeners: new Map<string, Set<NodeRedisPubSubListener>>(),
      patternListeners: new Map<string, Set<NodeRedisPubSubListener>>(),
      drained: Promise.resolve(),
    }
    pubsub.drained = this.drainPushes(pubsub, abort.signal)
    this.pubsub = pubsub
    return pubsub
  }

  private async drainPushes(
    pubsub: NonNullable<NodeRedisMockClient['pubsub']>,
    signal: AbortSignal,
  ): Promise<void> {
    try {
      for await (const frame of pubsub.session.readPushes(signal)) {
        this.dispatchPush(pubsub, frame)
      }
    } catch (err) {
      if (!signal.aborted) {
        this.emitter.emit('error', err)
      }
    }
  }

  private dispatchPush(
    pubsub: NonNullable<NodeRedisMockClient['pubsub']>,
    frame: RedisResult,
  ): void {
    const value = frame.value
    if (value.kind !== 'push') {
      return
    }
    // A push frame's type is its `name` ('message' / 'pmessage' / 'subscribe' /
    // …); `items` is just the payload. We only deliver actual messages —
    // subscribe/unsubscribe confirmations are consumed elsewhere.
    const items = value.items.map(item => String(decodeReply(item)))

    if (value.name === 'message') {
      const [channel, message] = items
      notify(pubsub.listeners.get(channel), message, channel)
      return
    }

    if (value.name === 'pmessage') {
      const [pattern, channel, message] = items
      notify(pubsub.patternListeners.get(pattern), message, channel)
    }
  }
}

/**
 * MULTI builder mirroring node-redis' chainable transaction API. Queues curated
 * commands and replays them with a real MULTI/EXEC on the owning session, so the
 * server's transaction + WATCH semantics drive the result (EXEC returns an array
 * of replies, or `null` when a watched key changed).
 */
export class NodeRedisMockMulti {
  private readonly queued: NodeRedisCommandArgument[][] = []
  private settled = false

  constructor(
    private readonly runTransaction: (
      queued: NodeRedisCommandArgument[][],
    ) => Promise<RedisValue>,
  ) {}

  set(key: string, value: string | number): this {
    return this.queue(['SET', key, String(value)])
  }

  get(key: string): this {
    return this.queue(['GET', key])
  }

  del(...keys: string[]): this {
    return this.queue(['DEL', ...keys])
  }

  incr(key: string): this {
    return this.queue(['INCR', key])
  }

  hSet(key: string, field: string, value: string): this {
    return this.queue(['HSET', key, field, value])
  }

  hGet(key: string, field: string): this {
    return this.queue(['HGET', key, field])
  }

  /** Generic escape hatch: queue any raw command. */
  addCommand(args: NodeRedisCommandArgument[]): this {
    return this.queue(args)
  }

  private queue(args: NodeRedisCommandArgument[]): this {
    this.queued.push(args)
    return this
  }

  /**
   * Replay the queued commands inside a real MULTI/EXEC and return the array of
   * decoded replies. Matching node-redis, a watch-aborted transaction throws a
   * `WatchError` (never returns null), and per-command errors are aggregated
   * into a single `MultiErrorReply` carrying every reply + the error indexes.
   */
  async exec(): Promise<NodeRedisReply[]> {
    this.assertOpen()
    this.settled = true

    const result = await this.runTransaction(this.queued)
    const errors = await ensureRedisErrors()

    if (result.kind === 'null' || result.kind === 'null-array') {
      // RESP2 `*-1` (a watched key changed). node-redis throws, never null.
      throw new errors.WatchError()
    }
    if (result.kind !== 'array' && result.kind !== 'set') {
      // Defensive: any non-array EXEC reply (shouldn't happen) → decode as-is.
      return [decodeReply(result)]
    }

    const replies: unknown[] = []
    const errorIndexes: number[] = []
    result.items.forEach((item, index) => {
      if (item.kind === 'error') {
        replies.push(new errors.ErrorReply(errorReplyText(item)))
        errorIndexes.push(index)
        return
      }
      replies.push(decodeReply(item))
    })

    if (errorIndexes.length > 0) {
      throw new errors.MultiErrorReply(replies, errorIndexes)
    }
    return replies as NodeRedisReply[]
  }

  /** Cancel the transaction without running the queued commands. */
  async discard(): Promise<void> {
    this.assertOpen()
    this.settled = true
    // No MULTI was opened on the session yet (we only open it in exec()), so
    // discarding is purely client-side: drop the queue.
    this.queued.length = 0
  }

  private assertOpen(): void {
    if (this.settled) {
      throw new Error('this multi has already been executed or discarded')
    }
  }
}

/**
 * In-memory node-redis cluster facade. Reuses {@link buildClusterNodes} for a
 * TCP-free cluster, then routes each command to the slot owner's session
 * (computed via {@link RedisClusterTopology.calculateSlotForKeys}). The curated
 * method surface is inherited unchanged from {@link CommandRunner}.
 */
export class NodeRedisMockCluster extends CommandRunner {
  private readonly emitter = new EventEmitter()
  private readonly topology: RedisClusterTopology
  private readonly masters: ClusterNodePipeline[]
  private readonly sessions = new Map<string, ClientSession>()
  private readonly replicationLinks: { close(): void }[]
  private closed = false

  private constructor(
    topology: RedisClusterTopology,
    masters: ClusterNodePipeline[],
    replicationLinks: readonly { close(): void }[],
  ) {
    super()
    this.topology = topology
    this.masters = masters
    this.replicationLinks = [...replicationLinks]
    queueMicrotask(() => {
      this.emitter.emit('connect')
      this.emitter.emit('ready')
    })
  }

  static create(options: NodeRedisMockClusterOptions): NodeRedisMockCluster {
    const { topology, nodes, replicationLinks } = buildClusterNodes({
      masters: options.masters,
      replicasPerMaster: options.replicas ?? 0,
      basePort: 0,
    })
    const masters = nodes.filter(node => node.role === 'master')
    return new NodeRedisMockCluster(topology, masters, replicationLinks)
  }

  on(event: string, listener: (...args: unknown[]) => void): this {
    this.emitter.on(event, listener)
    return this
  }

  once(event: string, listener: (...args: unknown[]) => void): this {
    this.emitter.once(event, listener)
    return this
  }

  off(event: string, listener: (...args: unknown[]) => void): this {
    this.emitter.off(event, listener)
    return this
  }

  async connect(): Promise<this> {
    return this
  }

  protected async run(args: NodeRedisCommandArgument[]): Promise<RedisValue> {
    const session = this.sessionForCommand(args)
    return runOnSession(session, args, this.closed)
  }

  // quit()/disconnect() are async to match node-redis' signatures, but cluster
  // teardown is fully synchronous (no push-drain loop to await) — unlike the
  // standalone client whose teardown awaits its pub/sub drain.
  async quit(): Promise<string> {
    this.teardown()
    this.emitter.emit('end')
    return 'OK'
  }

  async disconnect(): Promise<void> {
    this.teardown()
    this.emitter.emit('end')
  }

  destroy(): void {
    this.teardown()
    this.emitter.emit('end')
  }

  private teardown(): void {
    if (this.closed) {
      return
    }
    this.closed = true
    for (const session of this.sessions.values()) {
      session.close()
    }
    this.sessions.clear()
    for (const link of this.replicationLinks) {
      link.close()
    }
    // Close each master's state to clear its active-expiry timer. (Replicas are
    // built with expiry disabled, so only masters arm one.)
    for (const node of this.masters) {
      node.state.close()
    }
  }

  /**
   * Resolve (and cache) a session on the master that owns the slot for the
   * command's keys. Keyless commands run on the first master.
   */
  private sessionForCommand(args: NodeRedisCommandArgument[]): ClientSession {
    const keys = extractRoutingKeys(args)
    const slot =
      keys.length > 0 ? this.topology.calculateSlotForKeys(keys) : null

    if (slot === -1) {
      // generateMulti() returns -1 when the keys span multiple slots. Match the
      // real cluster (ClusterPolicy) and refuse, rather than silently running
      // the whole command against the first key's node with a wrong result.
      // Surface it the way node-redis would parse `-CROSSSLOT …` off the wire:
      // an ErrorReply whose message carries the code prefix.
      const crossSlot = new RedisCrossSlotError()
      const ErrorReply = resolvedRedisErrors?.ErrorReply
      throw ErrorReply ? new ErrorReply(errorReplyText(crossSlot)) : crossSlot
    }

    const owner =
      slot === null
        ? this.masters[0]
        : (this.topology.getSlotOwner(slot) ?? this.masters[0])

    return this.sessionFor(owner.id)
  }

  private sessionFor(nodeId: string): ClientSession {
    const existing = this.sessions.get(nodeId)
    if (existing) {
      return existing
    }
    const node = this.masters.find(master => master.id === nodeId)
    if (!node) {
      throw new Error(`No master pipeline for cluster node ${nodeId}`)
    }
    const session = new ClientSession({
      server: node.state,
      executor: node.executor,
      nodeRole: node.role,
    })
    this.sessions.set(nodeId, session)
    return session
  }
}

// --- shared helpers --------------------------------------------------------

/**
 * Execute a tokenised command on a session and return its raw {@link RedisValue}
 * (callers decode it to the node-redis-correct shape). Throws when the client is
 * closed, and rejects streaming commands the facade can't deliver as a reply
 * (pub/sub uses the dedicated push-draining session instead).
 */
async function runOnSession(
  session: ClientSession,
  args: NodeRedisCommandArgument[],
  closed: boolean,
): Promise<RedisValue> {
  if (closed) {
    throw new Error('node-redis mock client is closed')
  }
  if (args.length === 0) {
    throw new Error('command requires at least a name')
  }

  const [name, ...rest] = args
  const result = await session.execute(toBuffer(name), rest.map(toBuffer))

  if (isResponseStream(result)) {
    // A multi-channel SUBSCRIBE/PSUBSCRIBE returns the per-channel confirmation
    // frames as a stream. The actual *messages* never flow here — they go to
    // the session's push queue (drained via readPushes). So consume the
    // confirmations to settle the subscription and return the last one as the
    // ack value.
    return drainSubscribeAck(result)
  }

  return result.value
}

/**
 * Consume a SUBSCRIBE/PSUBSCRIBE confirmation stream to completion and return
 * the final confirmation frame's value as the ack. Messages are delivered out
 * of band via {@link ClientSession.readPushes}, so this stream only ever yields
 * the subscribe confirmations.
 */
async function drainSubscribeAck(stream: ResponseStream): Promise<RedisValue> {
  let last: RedisValue = { kind: 'simple-string', value: 'OK' }
  const abort = new AbortController()
  for await (const frame of stream.frames(abort.signal)) {
    last = frame.value
  }
  return last
}

// Commands whose keys are every positional argument after the name. The router
// extracts all of them so a cross-slot invocation (e.g. `del('a','b')` across
// slots) is detected and refused rather than silently run on the first key's
// node. Other commands route by their first key argument.
const MULTI_KEY_COMMANDS = new Set([
  'DEL',
  'EXISTS',
  'UNLINK',
  'TOUCH',
  'MGET',
  'WATCH',
  'SINTER',
  'SUNION',
  'SDIFF',
  'PFCOUNT',
])

function extractRoutingKeys(args: NodeRedisCommandArgument[]): Buffer[] {
  if (args.length < 2) {
    return []
  }
  if (MULTI_KEY_COMMANDS.has(String(args[0]).toUpperCase())) {
    return args.slice(1).map(toBuffer)
  }
  // Single-key heuristic: every other routed command keys off its first arg.
  // Uncommon multi-key commands sent via the generic sendCommand fall through
  // here and route by first key (documented honest scope).
  return [toBuffer(args[1])]
}

function addListener(
  map: Map<string, Set<NodeRedisPubSubListener>>,
  key: string,
  listener: NodeRedisPubSubListener,
): void {
  const set = map.get(key) ?? new Set<NodeRedisPubSubListener>()
  set.add(listener)
  map.set(key, set)
}

function notify(
  listeners: Set<NodeRedisPubSubListener> | undefined,
  message: string,
  channel: string,
): void {
  if (!listeners) {
    return
  }
  for (const listener of listeners) {
    listener(message, channel)
  }
}

function toBuffer(arg: NodeRedisCommandArgument): Buffer {
  return Buffer.isBuffer(arg) ? arg : Buffer.from(arg)
}

// --- reply decoding --------------------------------------------------------
//
// node-redis leaves most RESP2 replies untransformed, and the in-memory decode()
// (RedisValue → native JS) already matches those defaults: bulk-string → utf8
// string, integer → number, map → object, array/set → array. So the curated
// methods are thin coercions over this shared decoder rather than per-command
// reply tables — uncommon commands get the same native shapes via sendCommand.

function decodeReply(value: RedisValue): NodeRedisReply {
  switch (value.kind) {
    case 'simple-string':
      return value.value
    case 'bulk-string':
      return value.value === null ? null : value.value.toString('utf8')
    case 'verbatim':
      return value.value.toString('utf8')
    case 'integer':
      // node-redis decodes a RESP2 `:` integer with plain JS number arithmetic,
      // so it is always a `number` (precision loss past 2^53 included) — never a
      // bigint. Only the RESP3 `(` BIG_NUMBER type yields a bigint, handled below.
      return typeof value.value === 'bigint' ? Number(value.value) : value.value
    case 'double':
      return value.value
    case 'boolean':
      return value.value
    case 'big-number':
      return value.value
    case 'array':
    case 'set':
    case 'push':
      return value.items.map(item => decodeReply(item))
    case 'map':
    case 'map-pairs': {
      const out: { [key: string]: NodeRedisReply } = {}
      for (const [key, val] of value.entries) {
        out[decodeKey(key)] = decodeReply(val)
      }
      return out
    }
    case 'flat-pairs':
      return value.entries.flatMap(([key, val]) => [
        decodeReply(key),
        decodeReply(val),
      ])
    case 'null':
    case 'null-array':
      return null
    case 'error': {
      // Surface node-redis' own ErrorReply (so `instanceof ErrorReply` matches
      // the documented idiom) with the reconstructed on-the-wire `CODE message`.
      // Falls back to RedisCommandError only if the redis package is absent.
      const text = errorReplyText(value)
      const ErrorReply = resolvedRedisErrors?.ErrorReply
      throw ErrorReply
        ? new ErrorReply(text)
        : new RedisCommandError(text, value.code)
    }
  }
}

function decodeKey(value: RedisValue): string {
  switch (value.kind) {
    case 'simple-string':
      return value.value
    case 'bulk-string':
      return value.value === null ? '' : value.value.toString('utf8')
    case 'verbatim':
      return value.value.toString('utf8')
    case 'integer':
    case 'double':
    case 'big-number':
      return String(value.value)
    case 'boolean':
      return String(value.value)
    default:
      return ''
  }
}

function asNumber(value: RedisValue): number {
  const reply = decodeReply(value)
  if (typeof reply === 'number') {
    return reply
  }
  if (typeof reply === 'bigint') {
    return Number(reply)
  }
  throw new RedisCommandError(`expected an integer reply, got ${typeof reply}`)
}

function asString(value: RedisValue): string {
  const reply = decodeReply(value)
  return typeof reply === 'string' ? reply : String(reply)
}

function asStringOrNull(value: RedisValue): string | null {
  const reply = decodeReply(value)
  if (reply === null) {
    return null
  }
  if (Buffer.isBuffer(reply)) {
    return reply.toString('utf8')
  }
  return typeof reply === 'string' ? reply : String(reply)
}

function asStringArray(value: RedisValue): string[] {
  const reply = decodeReply(value)
  if (!Array.isArray(reply)) {
    return []
  }
  return reply.map(item =>
    Buffer.isBuffer(item) ? item.toString('utf8') : String(item),
  )
}
