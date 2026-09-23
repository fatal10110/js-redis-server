import { EventEmitter } from 'node:events'
import { createRequire } from 'node:module'
import { createRedisCommandExecutor } from '../commands'
import { buildClusterNodes, type ClusterNodePipeline } from '../cluster'
import { ClientSession } from '../core/client-session'
import type { CommandExecutor } from '../core/command-executor'
import type { CompatibilityProfile } from '../core/compatibility'
import {
  decodeRedisMapEntries,
  decodeRedisValue,
  redisErrorText,
  type ClientDecodeOptions,
  type NativeRedisReply,
} from '../core/decode-redis-value'
import { RedisCommandError } from '../core/redis-error'
import { RedisResult } from '../core/redis-result'
import type { RedisValue } from '../core/redis-value'
import type { RespVersion } from '../core/resp-encoder'
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
// always is, since it's the thing being mocked — surface the real classes, and
// follow the installed version where their behavior differs. Resolved once, on
// first need, and cached. Local stand-ins cover what `redis` does not export.
type ErrorReplyConstructor = new (message: string) => Error
type RedisErrorConstructors = {
  WatchError: new (message?: string) => Error
  ErrorReply: ErrorReplyConstructor
  /**
   * The class a server `-ERR` reply decodes to. node-redis v5+ decodes it into
   * `SimpleError` (a subclass of `ErrorReply`); v4 has no `SimpleError` and
   * decodes it into `ErrorReply` itself, so that is the fallback.
   */
  SimpleError: ErrorReplyConstructor
  /**
   * What `exec()` throws when any queued command failed. `undefined` on a
   * `redis` that predates it (< 4.6.12): that `exec()` never throws for a
   * failed command — it resolves, with the error inline in the reply array.
   */
  MultiErrorReply:
    (new (replies: unknown[], errorIndexes: number[]) => Error) | undefined
  ClientClosedError: new () => Error
  DisconnectsClientError: new () => Error
}

// Local stand-ins, used when `redis` cannot be required at all (they then model
// v6, the version this facade tracks) or lacks one of the classes. They mirror
// node-redis' own classes: same message, same `name` ('Error' — none of the
// real classes assigns one), and the same `constructor.name`, which is what
// `util.inspect` and assertion libraries print. None is `instanceof` a class a
// user can import, so code that must work without `redis` matches `message`.
class WatchError extends Error {
  constructor(message = 'One (or more) of the watched keys has been changed') {
    super(message)
  }
}
class ErrorReply extends Error {}
class SimpleError extends ErrorReply {}
class MultiErrorReply extends ErrorReply {
  constructor(
    readonly replies: unknown[],
    readonly errorIndexes: number[],
  ) {
    super(
      `${errorIndexes.length} commands failed, see .replies and .errorIndexes for more information`,
    )
  }
  *errors(): IterableIterator<unknown> {
    for (const index of this.errorIndexes) {
      yield this.replies[index]
    }
  }
}
class ClientClosedError extends Error {
  constructor() {
    super('The client is closed')
  }
}
class DisconnectsClientError extends Error {
  constructor() {
    super('Disconnects client')
  }
}

const STAND_IN_REDIS_ERRORS: RedisErrorConstructors = {
  WatchError,
  ErrorReply,
  SimpleError,
  MultiErrorReply,
  ClientClosedError,
  DisconnectsClientError,
}

let resolvedRedisErrors: RedisErrorConstructors | undefined

/**
 * node-redis' own error classes, resolved on first need and cached.
 *
 * - **Lazy.** Importing this module must do no work (`"sideEffects": false`),
 *   and ioredis-only consumers import it too, so nothing here may load the
 *   optional `redis` peer at import time — only the first facade client
 *   constructed ({@link CommandRunner}'s constructor) or the first error
 *   decoded does.
 * - **Synchronous.** `destroy()` throws synchronously, so the close path cannot
 *   await an import. `redis` and `@redis/client` are CommonJS-only packages (no
 *   `exports` map), so `require` returns the very module instance an ESM
 *   `import … from 'redis'` sees — the classes are identical and `instanceof`
 *   holds for ESM and CJS consumers alike.
 * - **Resolved from this module's own location**, as a static import would be.
 * - **Per class.** Older `redis` releases lack some classes — `ErrorReply`
 *   arrived in redis 4.1.0, `MultiErrorReply` in 4.6.12 (@redis/client 1.5.13),
 *   `SimpleError` in v5 — and a missing one must not cost the ones that do
 *   exist: `instanceof ClientClosedError` has to keep holding on every version
 *   that exports it. A missing `SimpleError` becomes `ErrorReply` and a missing
 *   `MultiErrorReply` stays `undefined`, as that version behaves; any other
 *   missing class falls back to its stand-in alone.
 */
function redisErrors(): RedisErrorConstructors {
  resolvedRedisErrors ??= loadRedisErrors()
  return resolvedRedisErrors
}

function requireRedis(): Record<string, unknown> | undefined {
  try {
    return createRequire(__filename)('redis') as Record<string, unknown>
  } catch {
    return undefined
  }
}

function loadRedisErrors(): RedisErrorConstructors {
  const redis = requireRedis()
  if (!redis) {
    return STAND_IN_REDIS_ERRORS
  }
  const exported = <K extends keyof RedisErrorConstructors>(
    name: K,
  ): RedisErrorConstructors[K] | undefined => {
    const value = redis[name]
    return typeof value === 'function'
      ? (value as RedisErrorConstructors[K])
      : undefined
  }

  const errorReply = exported('ErrorReply') ?? ErrorReply
  return {
    WatchError: exported('WatchError') ?? WatchError,
    ErrorReply: errorReply,
    SimpleError: exported('SimpleError') ?? errorReply,
    MultiErrorReply: exported('MultiErrorReply'),
    ClientClosedError: exported('ClientClosedError') ?? ClientClosedError,
    DisconnectsClientError:
      exported('DisconnectsClientError') ?? DisconnectsClientError,
  }
}

/** A close-path error, synchronously — `destroy()` cannot await. */
function closeError(which: 'ClientClosedError' | 'DisconnectsClientError') {
  return new (redisErrors()[which])()
}

/** Thrown by any call on a closed client, a redundant close included. */
function clientClosedError(): Error {
  return closeError('ClientClosedError')
}

/**
 * Real node-redis' `destroy()` flushes the in-flight command queue with this
 * (`client/index.js`: `#queue.flushAll(new DisconnectsClientError())`), and
 * `disconnect()` is an alias for `destroy()`.
 */
function disconnectsClientError(): Error {
  return closeError('DisconnectsClientError')
}

/** A command argument node-redis accepts on the wire. */
export type NodeRedisCommandArgument = string | Buffer

/** Native JS value a reply decodes to. */
export type NodeRedisReply = NativeRedisReply

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
  // node-redis' error classes are resolved by each client's constructor (see
  // redisErrors()), so every construction path — this factory or a direct
  // `new`/`create()` — gets the real WatchError/ErrorReply/ClientClosedError.
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
 *
 * Extends `EventEmitter` because a real node-redis client is one: `on`/`once`/
 * `off` (and the rest of the emitter surface) come for free on both clients.
 */
abstract class CommandRunner extends EventEmitter {
  /**
   * Whether this client has been torn down. Shared by both clients, but they
   * act on it differently: the standalone one refuses every later call, while
   * a real `RedisCluster` closes idempotently (see
   * {@link NodeRedisMockCluster.quit}).
   */
  protected closed = false

  constructor() {
    super()
    // First need of node-redis' error classes: resolve them before this client
    // can run, decode or close anything. Not at import — see redisErrors().
    redisErrors()
  }

  /**
   * Execute one already-tokenised command and return its decoded reply.
   * Implementations pick the session (standalone: the only one; cluster: the
   * slot owner for the command's keys).
   */
  protected abstract run(args: NodeRedisCommandArgument[]): Promise<RedisValue>

  /**
   * RESP version this client's replies decode under — what every
   * protocol-dependent shape keys off (map, map-pairs, flat-pairs, double,
   * big-number, boolean). `HELLO` switches it mid-connection, so it is read per
   * reply rather than fixed at construction.
   */
  protected abstract get respVersion(): RespVersion

  /** The served profile, which spells a RESP2 `double` (#451). */
  protected abstract get profile(): CompatibilityProfile

  /** Generic escape hatch for any command, decoded to a native JS reply. */
  async sendCommand(args: NodeRedisCommandArgument[]): Promise<NodeRedisReply> {
    const value = await this.run(args)
    return decodeReply(value, this.respVersion, this.profile)
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

  async mSet(
    entries: [key: string, value: string | number][],
  ): Promise<string> {
    const args: NodeRedisCommandArgument[] = ['MSET']
    for (const [key, value] of entries) {
      args.push(key, String(value))
    }
    return asString(await this.run(args))
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
    const value = await this.run(['HGETALL', key])
    // Deliberately *not* `decodeReply`: node-redis' own `hGetAll`
    // transformReply assembles the object itself, from the RESP2 flat array as
    // readily as from the RESP3 map, so the curated method is an object on both
    // protocols where the raw `sendCommand` path follows the protocol (#414).
    return decodeMapReply(value, this.respVersion) as {
      [field: string]: string
    }
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

  async zUnionStore(destination: string, keys: string[]): Promise<number> {
    return asNumber(
      await this.run([
        'ZUNIONSTORE',
        destination,
        String(keys.length),
        ...keys,
      ]),
    )
  }

  // --- scripting -----------------------------------------------------------

  /**
   * EVAL with node-redis' options shape. The declared `keys` — not the script
   * text — are the command's routing keys, which is exactly what the executor's
   * plan reports.
   */
  async eval(
    script: string,
    options: { keys?: string[]; arguments?: string[] } = {},
  ): Promise<NodeRedisReply> {
    const keys = options.keys ?? []
    const args = options.arguments ?? []
    const value = await this.run([
      'EVAL',
      script,
      String(keys.length),
      ...keys,
      ...args,
    ])
    return decodeReply(value, this.respVersion, this.profile)
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
  /**
   * Set synchronously by `destroy()` (and so `disconnect()`), which — unlike
   * `quit()` — flush instead of draining. Every unit of work not yet answered,
   * queued or already executing, rejects with this (see {@link flushable}).
   */
  private flushPendingWith?: () => Error
  /**
   * Present exactly while a graceful `quit()` is draining. This is the
   * "closing" state, as distinct from closed: new work is refused, but a
   * forced close may still escalate — it calls this to reject the quit().
   */
  private rejectPendingQuit?: (err: Error) => void
  /** Guards {@link teardown}; see its note on why {@link closed} cannot. */
  private tornDown = false

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
      this.emit('connect')
      this.emit('ready')
    })
  }

  /**
   * node-redis clients require an explicit connect(); on an open client it is a
   * no-op here.
   *
   * KNOWN GAP: real node-redis re-opens a *closed* client — it reconnects,
   * serves commands again and emits a second `'end'` on the next close. This
   * facade cannot yet, because the client that owns its `RedisServerState`
   * closes it during teardown and `RedisServerState.close()` is terminal.
   * Rather than hand back a silently dead client, reconnection fails loudly
   * with the same `ClientClosedError` every other call on a closed client
   * throws. Tracked in #440; see docs/TESTING.md.
   */
  async connect(): Promise<this> {
    this.assertOpen()
    return this
  }

  /** A fresh, independent client over the **same** shared keyspace. */
  async duplicate(): Promise<NodeRedisMockClient> {
    return new NodeRedisMockClient({
      ...this.backend,
      database: this.database,
    })
  }

  protected get respVersion(): RespVersion {
    return this.session.protocolVersion
  }

  protected get profile(): CompatibilityProfile {
    return this.backend.state.profile
  }

  protected run(args: NodeRedisCommandArgument[]): Promise<RedisValue> {
    // Open/closed is decided at *issue* time, the way a real client decides it
    // when it writes the command to the socket. A command issued before a close
    // is already in the queue and must not be retroactively killed by a close
    // that happens while it waits its turn — `quit()` drains those, and
    // `disconnect()`/`destroy()` flush them via `flushPendingWith`.
    this.assertOpen()
    return this.runExclusive(() =>
      this.flushable(() => runOnSession(this.session, args, false)),
    )
  }

  /**
   * Run one queued unit of work subject to a hard close. Real node-redis'
   * `#queue.flushAll(new DisconnectsClientError())` rejects every command it
   * has not yet answered — still queued, *or already on the wire* — so a unit
   * that has not started, or is still running when the flush lands, rejects
   * with the flush error whatever it would otherwise have produced. That also
   * covers a parked `BLPOP 0`: teardown's `session.close()` unblocks it, and
   * the internal abort must not leak out as a bare `AbortError`.
   */
  private async flushable<T>(work: () => Promise<T>): Promise<T> {
    this.throwIfFlushed()
    let result: T
    try {
      result = await work()
    } catch (err) {
      this.throwIfFlushed()
      throw err
    }
    this.throwIfFlushed()
    return result
  }

  private throwIfFlushed(): void {
    const flush = this.flushPendingWith
    if (flush) {
      throw flush()
    }
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
    return new NodeRedisMockMulti(queued => {
      // Same issue-time gate as run(): a transaction handed to exec() before a
      // close drains with the rest of the queue.
      this.assertOpen()
      return this.runExclusive(() =>
        this.flushable(() => this.runTransactionSpan(queued)),
      )
    })
  }

  /** Replay MULTI → queued commands → EXEC on the shared session as one span. */
  private async runTransactionSpan(
    queued: NodeRedisCommandArgument[][],
  ): Promise<TransactionSpan> {
    await runOnSession(this.session, ['MULTI'], false)
    for (const args of queued) {
      await runOnSession(this.session, args, false)
    }
    const before = this.session.protocolVersion
    const value = await runOnSession(this.session, ['EXEC'], false)
    return {
      value,
      respVersion: this.session.protocolVersion,
      profile: this.backend.state.profile,
      replyVersions: replayProtocolSwitches(queued, before, value),
    }
  }

  // --- lifecycle -----------------------------------------------------------

  /**
   * Real node-redis rejects new work on a client that is closed *or closing*
   * (a `quit()` still draining) with `ClientClosedError` — so defensive
   * teardown that closes twice sees the same error here, and 'end' stays a
   * once-per-close signal.
   */
  private assertOpen(): void {
    if (this.closed) {
      throw clientClosedError()
    }
  }

  /**
   * Graceful close. Real node-redis' `quit()` appends `QUIT` to the command
   * queue, so every command already issued runs to completion first — 2000
   * pending commands all resolve. Queueing the teardown through the same
   * {@link runExclusive} lock reproduces that: the lock is FIFO, so by the time
   * teardown runs, nothing issued before the `quit()` is left.
   *
   * While it drains the client is *closing*: new work is refused, but a forced
   * {@link destroy}/{@link disconnect} can still escalate and reject this.
   */
  async quit(): Promise<string> {
    this.assertOpen()
    // Claim the closed flag synchronously so commands issued *after* this point
    // are refused (assertOpen in run()) while the ones before it still drain.
    this.closed = true
    const forced = new Promise<never>((_, reject) => {
      this.rejectPendingQuit = reject
    })
    try {
      // A drain can block indefinitely (a BLPOP 0 ahead of it), so it races the
      // escalation rather than waiting on it.
      await Promise.race([this.runExclusive(async () => undefined), forced])
    } finally {
      this.rejectPendingQuit = undefined
    }
    await this.teardown()
    this.emit('end')
    return 'OK'
  }

  /**
   * Hard close. Unlike {@link quit} this does *not* drain. Real node-redis'
   * `disconnect()` is literally `Promise.resolve(this.destroy())`, so all of
   * it — including the throw on an already-closed client — happens
   * synchronously inside destroy(), before any promise exists:
   * `client.disconnect().catch(...)` crashes against the real client, and so
   * it does here.
   */
  // Deliberately NOT `async`: that would turn destroy()'s synchronous throw into
  // a rejection, which is exactly the divergence this mirrors away.
  disconnect(): Promise<void> {
    this.destroy()
    return Promise.resolve()
  }

  /**
   * node-redis exposes a synchronous destroy(). It flushes — every unit of
   * work not yet answered, queued or executing, rejects with
   * {@link disconnectsClientError} — then tears down and emits 'end' in-line.
   *
   * Escalating a pending {@link quit} (the "graceful, then force" teardown
   * pattern) follows real node-redis v6 exactly as observed against a live
   * server: the flush also rejects the quit() itself, and then destroy() STILL
   * throws `ClientClosedError`, and no 'end' is emitted. That is because real
   * destroy() flushes its queue first and only then reaches its socket, which
   * the pending quit() had already marked closed. (Real node-redis also leaks
   * the socket on this path — the server keeps the connection and the process
   * never exits. That is deliberately NOT reproduced: the facade still tears
   * everything down, so nothing hangs.)
   */
  destroy(): void {
    const rejectQuit = this.rejectPendingQuit
    if (this.closed && !rejectQuit) {
      throw clientClosedError()
    }
    // Flush first. Both flags go up synchronously, so nothing queued slips
    // past, and anything executing is rejected when it settles.
    this.closed = true
    this.flushPendingWith = disconnectsClientError
    this.rejectPendingQuit = undefined
    rejectQuit?.(disconnectsClientError())
    // teardown() is async (it awaits the push-drain loop). Re-emit a rejection
    // as an 'error' event, which is what node-redis does with a teardown
    // failure — and, like any EventEmitter, that itself throws when nothing is
    // listening. teardown() releases everything it owns before this point, so
    // the throw leaks nothing. Its synchronous prefix closes the session, which
    // is what unblocks an executing BLPOP so the flush can reject it.
    void this.teardown().catch(err => this.emit('error', err))
    if (rejectQuit) {
      throw clientClosedError()
    }
    this.emit('end')
  }

  /**
   * Releases the session, pub/sub and (when owned) the state. Guarded by its
   * own flag rather than {@link closed}, which the close methods claim earlier
   * — `quit()` sets it before draining, so it is already true by the time
   * teardown runs.
   */
  private async teardown(): Promise<void> {
    if (this.tornDown) {
      return
    }
    this.tornDown = true
    this.closed = true
    try {
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
    } finally {
      // In a `finally` because a push-drain loop that rejects must not strand
      // the state: closing it is what clears the self-rescheduling
      // active-expiry timer, and a live timer keeps the process alive.
      // Only the creating client owns the state graph — duplicates share it and
      // must not close it out from under their siblings.
      if (this.ownsState) {
        this.backend.state.close()
      }
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
        this.emit('error', err)
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
    const items = value.items.map(item =>
      String(
        decodeReply(
          item,
          pubsub.session.protocolVersion,
          pubsub.session.server.profile,
        ),
      ),
    )

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

/** One replayed MULTI → … → EXEC span, with what each reply must decode under. */
type TransactionSpan = {
  /** EXEC's reply. */
  value: RedisValue
  /** The protocol in force once the transaction finished. */
  respVersion: RespVersion
  /** The served profile, which spells a RESP2 `double`. */
  profile: CompatibilityProfile
  /** Protocol each queued command's reply was produced under, by index. */
  replyVersions: readonly RespVersion[]
}

/**
 * The RESP version each queued reply was produced under. A `HELLO` inside a
 * MULTI switches the protocol partway through EXEC, and real Redis encodes the
 * items before it under the old version and the rest — including `HELLO`'s own
 * reply — under the new one. Verified against Redis 8.0.6 with node-redis@6, in
 * both directions:
 *
 *   MULTI; ZRANGE z 0 -1 WITHSCORES; HELLO 3; ZRANGE …; EXEC   (on RESP2)
 *     → ["a","1","b","2"], <RESP3 map>, [["a",1],["b",2]]
 *   the same with HELLO 2 on a RESP3 connection
 *     → [["a",1],["b",2]], <RESP2 array>, ["a","1","b","2"]
 *
 * `ClientSession` gets this right for the wire path by pre-encoding each item
 * as it goes (its `sawProtocolSwitch`), but hands the facade only the decoded
 * array, so the switch is replayed here from the queue the facade owns.
 *
 * Only `HELLO <n>` is modelled — `RESET` is not queued by real Redis, it runs
 * immediately and aborts the transaction (verified: `MULTI; RESET; EXEC` →
 * `ERR EXEC without MULTI`). A queued `HELLO` that *fails* leaves the protocol
 * where it was, so the replay skips any whose slot in EXEC's array came back
 * an error; that is what makes it exact rather than a guess, and it is why
 * there is no "prediction missed, use the final version everywhere" fallback:
 * such a fallback is wrong for every item before the divergence (it decodes a
 * reply that ran at RESP2 as RESP3), and with failed HELLOs accounted for
 * there is nothing left for it to catch but a protocol-moving command this
 * facade does not know about — where a replay that is right up to the
 * divergence still beats a uniform value that is wrong before it.
 */
function replayProtocolSwitches(
  queued: readonly NodeRedisCommandArgument[][],
  before: RespVersion,
  result: RedisValue,
): RespVersion[] {
  const items =
    result.kind === 'array' || result.kind === 'set' ? result.items : []
  const versions: RespVersion[] = []
  let current = before
  queued.forEach((args, index) => {
    if (items[index]?.kind !== 'error') {
      current = protocolSwitchedBy(args) ?? current
    }
    versions.push(current)
  })
  return versions
}

/** The version a queued `HELLO <2|3>` switches to, or undefined for anything else. */
function protocolSwitchedBy(
  args: readonly NodeRedisCommandArgument[],
): RespVersion | undefined {
  if (args.length < 2 || toText(args[0]).toLowerCase() !== 'hello') {
    return undefined
  }
  const version = Number(toText(args[1]))
  return version === 2 || version === 3 ? version : undefined
}

function toText(arg: NodeRedisCommandArgument): string {
  return Buffer.isBuffer(arg) ? arg.toString('utf8') : arg
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
    ) => Promise<TransactionSpan>,
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
   * into a single `MultiErrorReply` carrying every reply + the error indexes
   * (on a `redis` that has that class — older ones resolve, errors inline).
   */
  async exec(): Promise<NodeRedisReply[]> {
    this.assertOpen()
    this.settled = true

    const {
      value: result,
      respVersion,
      profile,
      replyVersions,
    } = await this.runTransaction(this.queued)
    const errors = redisErrors()

    if (result.kind === 'null' || result.kind === 'null-array') {
      // RESP2 `*-1` (a watched key changed). node-redis throws, never null.
      throw new errors.WatchError()
    }
    if (result.kind !== 'array' && result.kind !== 'set') {
      // Defensive: any non-array EXEC reply (shouldn't happen) → decode as-is.
      return [decodeReply(result, respVersion, profile)]
    }

    const replies: unknown[] = []
    const errorIndexes: number[] = []
    result.items.forEach((item, index) => {
      if (item.kind === 'error') {
        replies.push(new errors.SimpleError(redisErrorText(item)))
        errorIndexes.push(index)
        return
      }
      replies.push(
        decodeReply(item, replyVersions[index] ?? respVersion, profile),
      )
    })

    // A `redis` older than 4.6.12 has no MultiErrorReply, and its `exec()`
    // resolves with each failed command's error inline instead of throwing.
    if (errorIndexes.length > 0 && errors.MultiErrorReply) {
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
  private readonly topology: RedisClusterTopology
  private readonly masters: ClusterNodePipeline[]
  private readonly sessions = new Map<string, ClientSession>()
  private readonly replicationLinks: { close(): void }[]
  /**
   * RESP version for the client as a whole, not for one node. Real node-redis
   * hands its `RESP` setting to every node client when it builds the slot map
   * (`@redis/client`'s `cluster-slots`), so no key can come back in the other
   * protocol's shape. `HELLO` here is keyless, so it only ever reaches
   * `masters[0]`; the version it negotiates is held here and replayed onto each
   * other node session by {@link syncProtocol} before that session serves a
   * command. Reading *this* rather than the serving session's version is also
   * what keeps two concurrent commands on different nodes from decoding under
   * each other's protocol.
   *
   * What it does not remove — and cannot — is the ambiguity of a `HELLO` that
   * is itself in flight. Commands here run concurrently, so every command
   * already running when the switch lands reads this field only when its reply
   * is decoded: the replies that can come back in the other protocol's shape
   * are bounded by the set of commands in flight at that moment, not by one.
   * Real node-redis has the same ambiguity for the same reason — its parser
   * belongs to the connection, not to the individual reply — so the point of
   * the conditional write in {@link run} is to stop that window outliving the
   * switch, not to close it.
   */
  private clientRespVersion: RespVersion = 2

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
      this.emit('connect')
      this.emit('ready')
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

  async connect(): Promise<this> {
    return this
  }

  protected get respVersion(): RespVersion {
    return this.clientRespVersion
  }

  protected get profile(): CompatibilityProfile {
    // Every node shares one hoisted profile (see buildClusterNodes).
    return this.masters[0].state.profile
  }

  protected async run(args: NodeRedisCommandArgument[]): Promise<RedisValue> {
    // DELIBERATE DEVIATION: a command on a closed cluster throws
    // ClientClosedError here, where real node-redis v6 throws an internal
    // `TypeError: Cannot read properties of undefined (reading 'replicas')` —
    // it dereferences the slot map its own close path just reset. That is an
    // upstream crash, not a contract, and reproducing it would only teach
    // callers to catch a TypeError. Everything else about the cluster close
    // path follows real node-redis exactly; see the note above quit().
    const session = this.sessionForCommand(args)
    await this.syncProtocol(session)
    // Captured *after* syncProtocol, which moves the session itself: reading
    // it earlier would make the first command on a newly-synced node look like
    // a protocol-moving one and re-adopt a version the client already holds.
    const before = session.protocolVersion
    const value = await runOnSession(session, args, this.closed)
    // Adopt the version only when *this* command moved *this* session — a
    // HELLO or a RESET. Writing it unconditionally would let an ordinary
    // command racing a HELLO put its own stale version back: commands here run
    // concurrently, so a command that started before the HELLO landed sees a
    // session still on the old protocol and would undo the switch for the
    // whole client. Comparing before/after instead of matching on the command
    // name keeps RESET (and anything future) working without a second list of
    // protocol-moving commands to maintain.
    if (session.protocolVersion !== before) {
      this.clientRespVersion = session.protocolVersion
    }
    return value
  }

  /**
   * Bring `session` up to {@link clientRespVersion} before it serves a command
   * — the facade's stand-in for the RESP handshake node-redis performs on every
   * node connection it opens. Done lazily, on the node's next command, which is
   * indistinguishable from doing it eagerly: a session that has not run a
   * command has not produced a reply to mis-shape.
   */
  private async syncProtocol(session: ClientSession): Promise<void> {
    if (session.protocolVersion === this.clientRespVersion) {
      return
    }
    await runOnSession(
      session,
      ['HELLO', String(this.clientRespVersion)],
      this.closed,
    )
  }

  // A real `RedisCluster` closes on completely different terms from a single
  // client, so do NOT copy the standalone contract here. Ground-truthed against
  // node-redis v6 driving a live 3-node cluster, and matching
  // `cluster/cluster-slots.js`, where every close routes through `#destroy()`:
  //
  //  - it emits 'disconnect', and NEVER 'end' — no 'end' emit exists anywhere
  //    under `cluster/`;
  //  - a redundant close does NOT throw. `#destroy()` resets its slot/node maps
  //    first, so a second call simply finds nothing to close, awaits
  //    `Promise.allSettled([])` and emits 'disconnect' AGAIN. The event is
  //    therefore once per close *call*, not once per open→closed transition;
  //  - quit()/disconnect()/destroy() all resolve `undefined` — the cluster's
  //    quit() returns `#destroy()`'s promise, not the standalone client's 'OK'.
  //
  // quit()/disconnect() stay async to match node-redis' signatures, but this
  // teardown is fully synchronous (no push-drain loop to await) — unlike the
  // standalone client, whose teardown awaits its pub/sub drain.

  async quit(): Promise<void> {
    await this.closeAsync()
  }

  async disconnect(): Promise<void> {
    await this.closeAsync()
  }

  /** Synchronous in real node-redis too, so it emits in-line. */
  destroy(): void {
    this.teardown()
    this.emit('disconnect')
  }

  /**
   * Real quit()/disconnect() run `#destroy()`, which tears down synchronously
   * but emits 'disconnect' only after `await Promise.allSettled(...)` — so the
   * caller never sees the event before the call returns, and always sees it
   * before the returned promise settles. The await reproduces that ordering.
   */
  private async closeAsync(): Promise<void> {
    this.teardown()
    await Promise.allSettled([])
    this.emit('disconnect')
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
    // Per-connection state goes back to its handshake default alongside the
    // sessions it belongs to.
    this.clientRespVersion = 2
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
   *
   * There is deliberately no client-side rejection here. A key set spanning
   * slots gives `-1`, which owns no node and falls through to the first master;
   * that node's `ClusterPolicy` recomputes the same keys and raises the real
   * error, which `decodeReply` surfaces as the same `ErrorReply`. Refusing here
   * instead would mean a second copy of rules the policy already owns, and
   * would pre-empt the ones it does not share — `SORT` routes on its source
   * key alone (plus `STORE`), while `ClusterPolicy` separately refuses BY/GET
   * patterns that may resolve to another slot with `BY option of SORT denied
   * in Cluster mode …` rather than `CROSSSLOT`.
   *
   * KNOWN LIMITATION — pub/sub is not supported through this cluster facade.
   * It exposes no subscribe API, and a raw `sendCommand(['SUBSCRIBE', …])` puts
   * the cached session for its node into subscriber mode for good: every later
   * command on that node is then refused with `ERR Can't execute …`. Because
   * `SUBSCRIBE` has no keys it lands on `masters[0]`, which also serves every
   * keyless command and the first slot range, so the blast radius is wide.
   * Fixing it properly means what the standalone client already does — a
   * dedicated pub/sub session plus a push-drain loop ({@link
   * NodeRedisMockClient.ensurePubSub}) — which is a feature, not a routing
   * change. Use {@link NodeRedisMockClient} for pub/sub.
   */
  private sessionForCommand(args: NodeRedisCommandArgument[]): ClientSession {
    const slot = this.topology.calculateSlotForKeys(this.routingKeys(args))
    const owner =
      slot === null
        ? this.masters[0]
        : (this.topology.getSlotOwner(slot) ?? this.masters[0])

    return this.sessionFor(owner.id)
  }

  /**
   * The command's routing keys, taken from the executor's own
   * {@link CommandExecutor.plan} — the exact extraction `ClusterPolicy` routes
   * on. Guessing them from the argument list gets multi-key commands (`MSET`,
   * `RENAME`), numkeys-prefixed ones (`EVAL`, `ZDIFF`, `LMPOP`), subcommand-
   * prefixed ones (`BITOP`) and STORE targets (`GEORADIUS … STORE`) wrong,
   * which picks the wrong node.
   *
   * Two invariants this leans on, neither enforced by a type:
   *
   *  - Planning is registry + schema only — no policy runs — so any master's
   *    executor answers identically. True because `buildClusterNodes` hands
   *    every node one hoisted profile and the same registry, differing only in
   *    their `CLUSTER` definitions. Per-node profiles would break it.
   *  - Every `RedisCommandError` out of `plan()` means "no keys we can route
   *    on" — an unregistered name, or an argument list the schema rejects.
   *    True while no registered command declares keys it cannot extract. Such
   *    a command goes to the first master and the normal pipeline turns it
   *    into the canonical error reply, exactly as the standalone client does,
   *    rather than growing a second error path here.
   *
   * Planning is not free — `session.execute` plans the command again, so a
   * cluster command pays two schema parses. A `routingKeysFor()` seam on the
   * executor would be cheaper and tidier, but with one call site it is not yet
   * worth the API.
   */
  private routingKeys(args: NodeRedisCommandArgument[]): readonly Buffer[] {
    if (args.length === 0) {
      return []
    }

    const [name, ...rest] = args
    try {
      return this.masters[0].executor.plan(
        toBuffer(name),
        rest.map((arg, index) => toBuffer(arg, index + 1)),
      ).keys
    } catch (err) {
      if (err instanceof RedisCommandError) {
        return []
      }
      throw err
    }
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
 * closed. A multi-target (UN)SUBSCRIBE's value is its first confirmation;
 * messages never flow here (pub/sub uses the dedicated push-draining session).
 *
 * Runs the reply's `afterReply` step, as the wire does. Nothing reads the push
 * queue of a non-pub/sub session, so `sendCommand(['MONITOR'])` resolves `OK`
 * but its feed lines are never surfaced — the facade has no `monitor()`.
 */
async function runOnSession(
  session: ClientSession,
  args: NodeRedisCommandArgument[],
  closed: boolean,
): Promise<RedisValue> {
  if (closed) {
    // Real node-redis rejects a command on a closed client with this exact
    // error, same as it does a redundant quit()/disconnect()/destroy().
    throw clientClosedError()
  }
  if (args.length === 0) {
    throw new Error('command requires at least a name')
  }

  const [name, ...rest] = args
  const result = await session.execute(
    toBuffer(name),
    rest.map((arg, index) => toBuffer(arg, index + 1)),
  )

  result.options?.afterReply?.()
  return result.value
}

/**
 * Deliberately narrower than the shared `toRedisArgument`, which also accepts
 * `number` for {@link InMemoryRedisClient}. `@redis/client`'s encoder takes
 * `string | Buffer` and nothing else, so a facade that coerced anything wider
 * would green-light test code that throws against the real client.
 *
 * `Buffer.from` alone is not narrow enough: it happily converts arrays and
 * TypedArrays that node-redis rejects. Hence the explicit check, whose message
 * mirrors the encoder's.
 */
function toBuffer(arg: NodeRedisCommandArgument, index = 0): Buffer {
  if (Buffer.isBuffer(arg)) {
    return arg
  }
  if (typeof arg !== 'string') {
    throw new TypeError(
      `"arguments[${index}]" must be of type "string | Buffer", got ${typeof arg} instead.`,
    )
  }
  return Buffer.from(arg)
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

// --- reply decoding --------------------------------------------------------
//
// node-redis leaves most replies untransformed, and the shared RedisValue →
// native JS decoder already matches what it hands back: bulk-string → utf8
// string, integer → number, array/set → array, plus the protocol-dependent
// kinds (map, map-pairs, flat-pairs, double, big-number, boolean) in whichever
// shape the negotiated RESP version puts on the wire. So the curated methods
// are thin coercions over that decoder rather than per-command reply tables —
// uncommon commands get the same native shapes via sendCommand. The exceptions
// are the few curated methods node-redis *does* transform, which decode through
// their own helper: `hGetAll` builds an object on both protocols (#414).

/**
 * How this facade reads a {@link RedisValue}, and therefore where it diverges
 * from `IN_MEMORY_DECODE_OPTIONS`. Exported so a test can assert the two apart —
 * the divergences are deliberate, and a silent re-convergence is the failure
 * mode worth catching.
 */
export const NODE_REDIS_DECODE_OPTIONS: ClientDecodeOptions = {
  // node-redis decodes a RESP2 `:` integer with plain JS number arithmetic, so
  // it is always a `number` (precision loss past 2^53 included) — never a
  // bigint. Only the RESP3 `(` BIG_NUMBER type yields a bigint.
  narrowBigInt: 'always',
  // node-redis routes a push frame by its type tag and hands listeners only the
  // payload, so the tag is not part of the decoded reply.
  pushShape: 'items',
  // Surface node-redis' own server-error class — `SimpleError`, a subclass of
  // `ErrorReply`, on v5+ — so both `instanceof ErrorReply` (the documented
  // idiom) and the concrete class match real node-redis.
  error: text => new (redisErrors().SimpleError)(text),
}

/**
 * Decode a reply the caller hands back whole. `respVersion` is the version the
 * connection served it under — it decides the protocol-dependent shapes — so it
 * is never defaulted.
 */
function decodeReply(
  value: RedisValue,
  respVersion: RespVersion,
  profile: CompatibilityProfile,
): NodeRedisReply {
  return decodeRedisValue(value, {
    ...NODE_REDIS_DECODE_OPTIONS,
    version: respVersion,
    profile,
  })
}

/**
 * Decode a reply a curated method immediately narrows to a scalar (or a flat
 * string array). None of the commands behind those methods replies with a kind
 * whose shape the protocol decides — no `map`, `double`, pair, `boolean` or
 * `big-number` reaches here — so the pinned version is unobservable, and the
 * `asNumber` / `asString` coercions on top of it stay honest.
 *
 * Adding a curated method for a command that *does* reply with one of those:
 * do not reach for this, and do not reach for `decodeReply` either. Ask what
 * node-redis' own `transformReply` for that command produces. Usually it
 * normalises, so the method is protocol-*independent* — real `zScore()` is a
 * number at RESP2 as well as RESP3, and `hGetAll()` an object at both — which
 * means decoding the reply's own kind directly, as {@link decodeMapReply}
 * does, not passing the live version to a decoder that would then follow it.
 */
function decodeScalarReply(value: RedisValue): NodeRedisReply {
  return decodeRedisValue(value, { ...NODE_REDIS_DECODE_OPTIONS, version: 2 })
}

/**
 * Decode a map reply for a curated method that presents it as an object on
 * *both* protocols, the way node-redis' `transformReply` does — see #414.
 * Entries still decode under the connection's own version, so only the
 * container shape is pinned.
 *
 * Nothing else is coerced into an object. An `error` — what a wrong-type key
 * replies with — goes through the ordinary decoder and throws, as real
 * node-redis' `hGetAll()` does at both protocols; swallowed into `{}` it would
 * be indistinguishable from a missing key. Any other kind (a `+QUEUED` after a
 * raw `MULTI`, say) fails loudly the way `asNumber` does, rather than resolving
 * to a string typed as an object.
 */
function decodeMapReply(
  value: RedisValue,
  respVersion: RespVersion,
): { [key: string]: NodeRedisReply } {
  const options = { ...NODE_REDIS_DECODE_OPTIONS, version: respVersion }
  if (value.kind === 'map' || value.kind === 'map-pairs') {
    return decodeRedisMapEntries(value.entries, options)
  }
  if (value.kind === 'error') {
    decodeRedisValue(value, options) // throws the reply's own error
  }
  throw new RedisCommandError(`expected a map reply, got ${value.kind}`)
}

function asNumber(value: RedisValue): number {
  const reply = decodeScalarReply(value)
  if (typeof reply === 'number') {
    return reply
  }
  if (typeof reply === 'bigint') {
    return Number(reply)
  }
  throw new RedisCommandError(`expected an integer reply, got ${typeof reply}`)
}

function asString(value: RedisValue): string {
  const reply = decodeScalarReply(value)
  return typeof reply === 'string' ? reply : String(reply)
}

function asStringOrNull(value: RedisValue): string | null {
  const reply = decodeScalarReply(value)
  if (reply === null) {
    return null
  }
  if (Buffer.isBuffer(reply)) {
    return reply.toString('utf8')
  }
  return typeof reply === 'string' ? reply : String(reply)
}

function asStringArray(value: RedisValue): string[] {
  const reply = decodeScalarReply(value)
  if (!Array.isArray(reply)) {
    return []
  }
  return reply.map(item =>
    Buffer.isBuffer(item) ? item.toString('utf8') : String(item),
  )
}
