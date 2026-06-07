import { RedisClusterTopology } from './cluster-topology'
import { RedisDatabase } from './database'
import { RedisPubSubBroker } from './pubsub-broker'
import { RedisScriptCache } from './script-cache'

export type RedisServerStateOptions = {
  databaseCount?: number
  clusterTopology?: RedisClusterTopology
  pubsubBroker?: RedisPubSubBroker
  scriptCache?: RedisScriptCache
}

export class RedisServerState {
  readonly databases: RedisDatabase[]
  readonly scriptCache: RedisScriptCache
  readonly pubsubBroker: RedisPubSubBroker
  readonly clusterTopology: RedisClusterTopology

  constructor(options?: RedisServerStateOptions) {
    const databaseCount = options?.databaseCount ?? 1
    if (!Number.isInteger(databaseCount) || databaseCount < 1) {
      throw new Error(`Invalid database count ${databaseCount}`)
    }

    this.databases = Array.from(
      { length: databaseCount },
      (_, index) => new RedisDatabase(index),
    )
    this.scriptCache = options?.scriptCache ?? new RedisScriptCache()
    this.pubsubBroker = options?.pubsubBroker ?? new RedisPubSubBroker()
    this.clusterTopology =
      options?.clusterTopology ?? new RedisClusterTopology()
  }

  getDatabase(id: number): RedisDatabase {
    const database = this.databases[id]
    if (!database) {
      throw new Error(`Database ${id} does not exist`)
    }

    return database
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
