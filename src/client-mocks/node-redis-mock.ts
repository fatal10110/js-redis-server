import { EventEmitter } from 'node:events'
import { createRedisCommandExecutor } from '../commands'
import { buildClusterNodes, type ClusterNodePipeline } from '../cluster'
import { ClientSession } from '../core/client-session'
import type { CommandExecutor } from '../core/command-executor'
import { RedisCommandError } from '../core/redis-error'
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
  if ('cluster' in options && options.cluster) {
    return NodeRedisMockCluster.create(options.cluster)
  }
  const databaseCount =
    ('databaseCount' in options ? options.databaseCount : undefined) ??
    DEFAULT_DATABASE_COUNT
  const state = new RedisServerState({ databaseCount })
  const executor = createRedisCommandExecutor()
  return new NodeRedisMockClient({ state, executor })
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
  async sendCommand(
    args: NodeRedisCommandArgument[],
  ): Promise<NodeRedisReply> {
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

  async expire(key: string, seconds: number): Promise<boolean> {
    return asNumber(await this.run(['EXPIRE', key, String(seconds)])) === 1
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
  private readonly session: ClientSession
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

  protected async run(
    args: NodeRedisCommandArgument[],
  ): Promise<RedisValue> {
    return runOnSession(this.session, args, this.closed)
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
    return new NodeRedisMockMulti(args =>
      runOnSession(this.session, args, this.closed),
    )
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
    void this.teardown()
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
    private readonly exec1: (
      args: NodeRedisCommandArgument[],
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
   * Replay the queued commands inside a real MULTI/EXEC. Returns an array of
   * decoded replies, or `null` if the transaction was aborted (a watched key
   * changed) — matching node-redis.
   */
  async exec(): Promise<NodeRedisReply[] | null> {
    this.assertOpen()
    this.settled = true

    await this.exec1(['MULTI'])
    for (const args of this.queued) {
      await this.exec1(args)
    }
    const result = await this.exec1(['EXEC'])

    if (result.kind === 'null' || result.kind === 'null-array') {
      return null
    }
    if (result.kind !== 'array' && result.kind !== 'set') {
      // Defensive: any non-array EXEC reply (shouldn't happen) → decode as-is.
      return [decodeReply(result)]
    }
    return result.items.map(item => decodeReply(item))
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

  protected async run(
    args: NodeRedisCommandArgument[],
  ): Promise<RedisValue> {
    const session = this.sessionForCommand(args)
    return runOnSession(session, args, this.closed)
  }

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
  }

  /**
   * Resolve (and cache) a session on the master that owns the slot for the
   * command's keys. Keyless commands run on the first master.
   */
  private sessionForCommand(
    args: NodeRedisCommandArgument[],
  ): ClientSession {
    const keys = extractRoutingKeys(args)
    const slot =
      keys.length > 0 ? this.topology.calculateSlotForKeys(keys) : null

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

function extractRoutingKeys(args: NodeRedisCommandArgument[]): Buffer[] {
  // The facade only needs slot routing, and every command it routes places its
  // key in the first argument slot. That covers the curated set + the common
  // single-key sendCommand cases; multi-key cross-slot commands are out of the
  // honest Tier-2 scope (documented).
  if (args.length < 2) {
    return []
  }
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
      if (typeof value.value === 'bigint') {
        return isSafeBigInt(value.value) ? Number(value.value) : value.value
      }
      return value.value
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
    case 'error':
      // Reconstruct the on-the-wire error text (`CODE message`) so the surfaced
      // message matches what node-redis parses from `-WRONGTYPE …`, `-ERR …`,
      // etc. The bare `value.message` would drop the code prefix.
      throw new RedisCommandError(
        value.code ? `${value.code} ${value.message}` : value.message,
        value.code,
      )
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

function isSafeBigInt(value: bigint): boolean {
  return (
    value >= BigInt(Number.MIN_SAFE_INTEGER) &&
    value <= BigInt(Number.MAX_SAFE_INTEGER)
  )
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
