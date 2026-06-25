import { RedisClusterTopology } from './cluster-topology'
import { RedisDatabase } from './database'
import { KeyspaceNotifier } from './keyspace-notifier'
import { RedisMonitorFeed } from './monitor-feed'
import type { Unsubscribe } from './mutation-events'
import { RedisPubSubBroker } from './pubsub-broker'
import { RedisScriptCache } from './script-cache'
import { RedisFunctionRegistry } from './function-registry'
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
   * Normalized `notify-keyspace-events` flag string (Redis canonical form, e.g.
   * `"AKE"`). Empty string disables keyspace notifications. Managed via
   * CONFIG GET/SET; read by the wired {@link KeyspaceNotifier} on each mutation.
   */
  notifyKeyspaceEvents = ''
  private readonly clientSessions = new Set<RedisClientSession>()
  private activeExpiryTimer: ReturnType<typeof setTimeout> | null = null
  private closed = false
  private luaRuntimePromise: Promise<RedisLuaRuntime> | null = null

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
        notifier.handle(
          event,
          database.activeNotifyCommand,
          this.notifyKeyspaceEvents,
        ),
      )
    }

    this.startActiveExpiry(options?.activeExpiryIntervalMs)
  }

  /**
   * Returns this server's own Lua runtime, created lazily and memoized per
   * RedisServerState instance. Scoping the runtime here (rather than a
   * process-wide singleton) keeps each logical node's LuaEngine + script
   * re-entrancy guard isolated, so concurrent EVALs on independent
   * server/cluster nodes never collide (issue #130).
   */
  getLuaRuntime(): Promise<RedisLuaRuntime> {
    this.luaRuntimePromise ??= createRedisLuaRuntime()
    return this.luaRuntimePromise
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
    for (const database of this.databases) {
      database.flush()
    }
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
    if (this.activeExpiryTimer) {
      clearTimeout(this.activeExpiryTimer)
      this.activeExpiryTimer = null
    }
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

    const now = Date.now()
    for (const database of this.databases) {
      if (this.closed) {
        return
      }

      const turn = await database.turnQueue.waitTurn()
      try {
        if (!this.closed) {
          database.sweepExpired(now)
        }
      } finally {
        turn.release()
      }
    }
  }
}
