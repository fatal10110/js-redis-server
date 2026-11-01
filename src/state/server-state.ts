import { RedisClusterTopology } from './cluster-topology'
import { RedisDatabase } from './database'
import { KeyspaceNotifier, type KeyspaceNotifyFlags } from './keyspace-notifier'
import { RedisMonitorFeed } from './monitor-feed'
import type { Unsubscribe } from './mutation-events'
import { RedisPubSubBroker } from './pubsub-broker'
import { RedisScriptCache } from './script-cache'
import { RedisFunctionRegistry } from './function-registry'
import { SerialTurnQueue } from '../core/turn-queue'
import { ClientPauseState } from './client-pause'
import { ClientTrackingTable } from './client-tracking'
import type { RedisClientSession } from '../core/redis-context'
import {
  createRedisLuaRuntime,
  type RedisLuaRuntime,
} from '../core/lua-runtime'
import {
  resolveCompatibilityProfile,
  type CompatibilityProfile,
  type CompatibilitySpec,
} from '../core/compatibility'

export type RedisServerStateOptions = {
  databaseCount?: number
  clusterTopology?: RedisClusterTopology
  monitorFeed?: RedisMonitorFeed
  pubsubBroker?: RedisPubSubBroker
  scriptCache?: RedisScriptCache
  functionRegistry?: RedisFunctionRegistry
  /**
   * Interval for the background active-expiry sweep. Set to `false` to disable
   * the timer for tests that need full manual control of expiration.
   */
  activeExpiryIntervalMs?: number | false
  /**
   * Optional server password (Redis `requirepass`). When set, connections start
   * unauthenticated and must `AUTH` before running most commands. When unset,
   * the default user is `nopass` and no authentication is enforced.
   */
  requirepass?: string
  compatibility?: CompatibilitySpec
}

const DEFAULT_ACTIVE_EXPIRY_INTERVAL_MS = 100

/** Redis' compiled-in default for `proto-max-bulk-len`: 512MB. */
const DEFAULT_PROTO_MAX_BULK_LEN = 536870912n

export class RedisServerState {
  readonly databases: RedisDatabase[]
  readonly scriptCache: RedisScriptCache
  readonly monitorFeed: RedisMonitorFeed
  readonly pubsubBroker: RedisPubSubBroker
  readonly functionRegistry: RedisFunctionRegistry
  readonly clusterTopology: RedisClusterTopology
  readonly requirepass?: string
  readonly profile: CompatibilityProfile
  /**
   * The server's single serialization turn. Every command on every database —
   * plus active expiry and seeding — runs under a turn from this queue, so the
   * mock is single-threaded across all databases exactly like real Redis. A
   * blocking command yields the turn while parked (`RedisTurnHandle.suspend`).
   * Each cluster node has its own `RedisServerState`, hence its own queue.
   */
  readonly turnQueue = new SerialTurnQueue()
  private readonly closeHooks = new Set<() => void>()
  /**
   * CLIENT PAUSE: server-wide, like real Redis. The client-pause execution
   * policy postpones commands while it is active.
   */
  readonly clientPause = new ClientPauseState()
  /** CLIENT TRACKING: the keys tracking clients read, and their modes. */
  readonly clientTracking = new ClientTrackingTable(() =>
    this.getConnectedClients(),
  )
  /**
   * Parsed `notify-keyspace-events` flags; empty disables keyspace
   * notifications. Parsed once by CONFIG SET (and rendered back to Redis'
   * canonical string by CONFIG GET), so the wired {@link KeyspaceNotifier}
   * reads it on each mutation without re-parsing.
   */
  notifyKeyspaceEvents: KeyspaceNotifyFlags = new Set()
  /**
   * Redis `proto-max-bulk-len` (default 512MB), as a bigint so the full
   * configurable range (up to int64 max) round-trips exactly. It caps how large
   * a single string value may grow: APPEND and SETRANGE refuse rather than
   * allocate past it. Managed via CONFIG GET/SET.
   */
  protoMaxBulkLen = DEFAULT_PROTO_MAX_BULK_LEN
  private readonly clientSessions = new Set<RedisClientSession>()
  private activeExpiryTimer: ReturnType<typeof setTimeout> | null = null
  private closed = false
  private readonly activeExpiryIntervalMs: number | false | undefined
  private luaRuntimePromise: Promise<RedisLuaRuntime> | null = null
  private luaRuntime: RedisLuaRuntime | null = null

  constructor(options?: RedisServerStateOptions) {
    this.requirepass = options?.requirepass
    this.profile = resolveCompatibilityProfile(options?.compatibility)
    const databaseCount = options?.databaseCount ?? 1
    if (!Number.isInteger(databaseCount) || databaseCount < 1) {
      throw new Error(`Invalid database count ${databaseCount}`)
    }

    this.databases = Array.from(
      { length: databaseCount },
      (_, index) => new RedisDatabase(index),
    )
    this.scriptCache = options?.scriptCache ?? new RedisScriptCache()
    this.functionRegistry =
      options?.functionRegistry ?? new RedisFunctionRegistry()
    this.monitorFeed = options?.monitorFeed ?? new RedisMonitorFeed()
    this.pubsubBroker = options?.pubsubBroker ?? new RedisPubSubBroker()
    this.clusterTopology =
      options?.clusterTopology ?? new RedisClusterTopology()

    // Bridge every database's mutation bus to the Pub/Sub broker so key
    // mutations surface as Redis keyspace/keyevent notifications. The bridge is
    // always wired; it is a cheap no-op while `notifyKeyspaceEvents` is empty.
    const notifier = new KeyspaceNotifier(this.pubsubBroker)
    for (const database of this.databases) {
      database.subscribe(event =>
        notifier.handle(event, this.notifyKeyspaceEvents),
      )
    }
    // Client-side caching invalidations (CLIENT TRACKING) follow the same
    // modified-key signals.
    for (const database of this.databases) {
      database.subscribe(event => this.clientTracking.handle(event))
    }

    this.activeExpiryIntervalMs = options?.activeExpiryIntervalMs
    this.startActiveExpiry(this.activeExpiryIntervalMs)
  }

  /**
   * Redis' `server.cluster_enabled`: true on a cluster node, false on a
   * standalone server, whose topology has no nodes. Commands that behave
   * differently in cluster mode — `HELLO`/`INFO`'s mode, SORT's BY/GET
   * pattern guard — branch on this rather than on the topology directly.
   */
  get clusterEnabled(): boolean {
    return this.clusterTopology.nodes.length > 0
  }

  /**
   * Returns this server's own Lua runtime, created lazily and memoized per
   * RedisServerState instance. Scoping the runtime here (rather than a
   * process-wide singleton) keeps each logical node's LuaEngine + script
   * re-entrancy guard isolated, so concurrent EVALs on independent
   * server/cluster nodes never collide (issue #130).
   *
   * A runtime whose engine faulted is dropped and the next call creates a
   * fresh one, as is a creation that failed (#539). Only the engine is
   * replaced: the script cache and the function libraries live here.
   */
  getLuaRuntime(): Promise<RedisLuaRuntime> {
    if (this.luaRuntime && !this.luaRuntime.usable) {
      this.luaRuntime.dispose()
      this.luaRuntime = null
      this.luaRuntimePromise = null
    }
    if (this.luaRuntimePromise) {
      return this.luaRuntimePromise
    }

    const created: Promise<RedisLuaRuntime> = createRedisLuaRuntime(
      this.profile,
    ).then(
      runtime => {
        if (this.luaRuntimePromise === created) {
          this.luaRuntime = runtime
        }
        return runtime
      },
      (err: unknown) => {
        if (this.luaRuntimePromise === created) {
          this.luaRuntimePromise = null
        }
        throw err
      },
    )
    this.luaRuntimePromise = created
    return created
  }

  getDatabase(id: number): RedisDatabase {
    const database = this.databases[id]
    if (!database) {
      throw new Error(`Database ${id} does not exist`)
    }

    return database
  }

  registerClientSession(session: RedisClientSession): Unsubscribe {
    this.clientSessions.add(session)
    return () => this.clientSessions.delete(session)
  }

  getConnectedClients(): readonly RedisClientSession[] {
    return [...this.clientSessions]
  }

  /**
   * Flushes keyspace data only. Redis script cache is server-wide state and
   * remains intact until SCRIPT FLUSH.
   */
  flushAllDatabases(): void {
    // One flush signal for the whole server, as real Redis gives FLUSHALL.
    this.clientTracking.coalesceFlush(undefined, false, () => {
      for (const database of this.databases) {
        database.flush()
      }
    })
  }

  sweepExpired(now = Date.now()): number {
    let count = 0
    for (const database of this.databases) {
      count += database.sweepExpired(now)
    }
    return count
  }

  close(): void {
    this.closed = true
    // Lifts a CLIENT PAUSE so its timer cannot keep the process alive.
    this.clientPause.unpause()
    if (this.activeExpiryTimer) {
      clearTimeout(this.activeExpiryTimer)
      this.activeExpiryTimer = null
    }
    // Frees the engine's WASM heap. dispose() swallows the engine's own
    // errors; the guard keeps any other from breaking close().
    try {
      this.luaRuntime?.dispose()
    } catch {
      // The runtime is dropped either way.
    }
    this.luaRuntime = null
    this.luaRuntimePromise = null
    // Outbound resources commands opened (MIGRATE's cached sockets).
    const hooks = [...this.closeHooks]
    this.closeHooks.clear()
    for (const hook of hooks) {
      hook()
    }
  }

  /**
   * Run `hook` once when the state is closed: how a command releases what it
   * keeps open past its own call (MIGRATE's cached target connections), so a
   * closed server leaves no socket or timer behind. Returns the function that
   * drops the hook again.
   */
  onClose(hook: () => void): () => void {
    this.closeHooks.add(hook)
    return () => {
      this.closeHooks.delete(hook)
    }
  }

  /**
   * Undo {@link close}: re-arm active expiry with the interval the state was
   * built with. Data, scripts and functions survived the close, and the Lua
   * runtime is rebuilt on first use, so nothing else needs restoring. A no-op
   * on an open state. Used when a client facade that owns the state is
   * re-opened by `connect()`.
   */
  reopen(): void {
    if (!this.closed) {
      return
    }
    this.closed = false
    this.startActiveExpiry(this.activeExpiryIntervalMs)
  }

  private startActiveExpiry(intervalMs: number | false | undefined): void {
    if (intervalMs === false) {
      return
    }

    const resolvedIntervalMs = intervalMs ?? DEFAULT_ACTIVE_EXPIRY_INTERVAL_MS
    if (!Number.isInteger(resolvedIntervalMs) || resolvedIntervalMs < 1) {
      throw new Error(`Invalid active expiry interval ${resolvedIntervalMs}`)
    }

    this.scheduleActiveExpiry(resolvedIntervalMs)
  }

  private scheduleActiveExpiry(intervalMs: number): void {
    // A tick still in flight across a close() + reopen() reschedules itself
    // too; keep only one timer either way.
    if (this.activeExpiryTimer) {
      clearTimeout(this.activeExpiryTimer)
    }
    const timer = setTimeout(() => {
      this.activeExpiryTimer = null
      void this.runActiveExpiryTick(intervalMs)
    }, intervalMs)

    ;(timer as ReturnType<typeof setTimeout> & { unref?: () => void }).unref?.()
    this.activeExpiryTimer = timer
  }

  private async runActiveExpiryTick(intervalMs: number): Promise<void> {
    try {
      await this.runActiveExpiry()
    } catch {
      // Active expiry is best-effort; command reads still lazily expire keys.
    } finally {
      if (!this.closed) {
        this.scheduleActiveExpiry(intervalMs)
      }
    }
  }

  private async runActiveExpiry(): Promise<void> {
    if (this.closed) {
      return
    }

    // One turn per tick sweeps every database.
    const turn = await this.turnQueue.waitTurn()
    try {
      if (this.closed) {
        return
      }

      this.sweepExpired(Date.now())
    } finally {
      turn.release()
    }
  }
}
