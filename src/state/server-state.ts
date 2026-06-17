import { RedisClusterTopology } from './cluster-topology'
import { RedisDatabase } from './database'
import { RedisMonitorFeed } from './monitor-feed'
import type { Unsubscribe } from './mutation-events'
import { RedisPubSubBroker } from './pubsub-broker'
import { RedisScriptCache } from './script-cache'
import type { RedisClientSession } from '../core/redis-context'
import {
  createRedisLuaRuntime,
  type RedisLuaRuntime,
} from '../core/lua-runtime'

export type RedisServerStateOptions = {
  databaseCount?: number
  clusterTopology?: RedisClusterTopology
  monitorFeed?: RedisMonitorFeed
  pubsubBroker?: RedisPubSubBroker
  scriptCache?: RedisScriptCache
  /**
   * Optional server password (Redis `requirepass`). When set, connections start
   * unauthenticated and must `AUTH` before running most commands. When unset,
   * the default user is `nopass` and no authentication is enforced.
   */
  requirepass?: string
}

export class RedisServerState {
  readonly databases: RedisDatabase[]
  readonly scriptCache: RedisScriptCache
  readonly monitorFeed: RedisMonitorFeed
  readonly pubsubBroker: RedisPubSubBroker
  readonly clusterTopology: RedisClusterTopology
  readonly requirepass?: string
  private readonly clientSessions = new Set<RedisClientSession>()
  private luaRuntimePromise: Promise<RedisLuaRuntime> | null = null

  constructor(options?: RedisServerStateOptions) {
    this.requirepass = options?.requirepass
    const databaseCount = options?.databaseCount ?? 1
    if (!Number.isInteger(databaseCount) || databaseCount < 1) {
      throw new Error(`Invalid database count ${databaseCount}`)
    }

    this.databases = Array.from(
      { length: databaseCount },
      (_, index) => new RedisDatabase(index),
    )
    this.scriptCache = options?.scriptCache ?? new RedisScriptCache()
    this.monitorFeed = options?.monitorFeed ?? new RedisMonitorFeed()
    this.pubsubBroker = options?.pubsubBroker ?? new RedisPubSubBroker()
    this.clusterTopology =
      options?.clusterTopology ?? new RedisClusterTopology()
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
}
