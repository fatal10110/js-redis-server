import { createRedisCommandExecutor } from './commands'
import { createClusterCommands } from './commands/cluster'
import { createClusterPolicy } from './core/execution-policies'
import {
  REDIS_CLUSTER_SLOT_COUNT,
  RedisDatabase,
  RedisClusterTopology,
  RedisServerState,
  type RedisClusterNode,
  type RedisClusterNodeRole,
  type RedisMutationEvent,
  type Unsubscribe,
} from './state'
import type { CommandExecutor } from './core/command-executor'
import type { Logger } from './logger'
import {
  resolveCompatibilityProfile,
  type CompatibilityProfile,
  type CompatibilitySpec,
} from './core/compatibility'

export type RedisClusterOptions = {
  masters: number
  replicasPerMaster?: number
  basePort: number
  host?: string
  databasesPerNode?: number
  replicaUpdateDelayMs?: number
  compatibility?: CompatibilitySpec
  logger?: Pick<Logger, 'error'>
}

/**
 * A single cluster node assembled without any TCP socket: its synthetic
 * `host:port`, replicated state, and the cluster-aware executor bound to it.
 */
export type ClusterNodePipeline = {
  id: string
  role: RedisClusterNodeRole
  host: string
  port: number
  state: RedisServerState
  executor: CommandExecutor
}

/**
 * The TCP-free product of {@link buildClusterNodes}: the shared topology, every
 * node pipeline, and the replication links wiring replicas to their masters
 * (the caller owns their teardown via `close()`).
 */
export type ClusterNodes = {
  topology: RedisClusterTopology
  nodes: readonly ClusterNodePipeline[]
  replicationLinks: readonly ReplicationLink[]
}

/**
 * Assembles cluster node pipelines — topology, per-node state + executor, and
 * the replica→master replication links — **without** binding any TCP socket.
 * Shared by the socket-backed {@link createRedisCluster} and the in-memory
 * client-mock cluster, so cluster routing/replication semantics never diverge.
 */
export function buildClusterNodes(options: RedisClusterOptions): ClusterNodes {
  validateOptions(options)

  const host = options.host ?? '127.0.0.1'
  const replicas = options.replicasPerMaster ?? 0
  const totalNodes = options.masters * (replicas + 1)
  const profile = resolveCompatibilityProfile(options.compatibility)

  if (options.basePort + totalNodes - 1 > 65535) {
    throw new Error('Cluster base-port range exceeds 65535')
  }

  const topologyNodes: RedisClusterNode[] = []
  let portOffset = 0
  // When basePort is 0 the OS assigns each server a random free port. Holding
  // all topology entries at 0 keeps listen() consistent across all nodes;
  // listen() backfills the real bound ports afterwards.
  const allocatePort = () =>
    options.basePort === 0 ? 0 : options.basePort + portOffset++

  for (let i = 0; i < options.masters; i++) {
    const masterId = `master-${i}`
    topologyNodes.push({
      id: masterId,
      role: 'master',
      host,
      port: allocatePort(),
      slots: [computeSlotRange(i, options.masters)],
    })
  }

  for (let i = 0; i < options.masters; i++) {
    const masterId = `master-${i}`
    for (let r = 0; r < replicas; r++) {
      topologyNodes.push({
        id: `replica-${r}-${masterId}`,
        role: 'replica',
        host,
        port: allocatePort(),
        masterId,
        slots: topologyNodes[i].slots,
      })
    }
  }

  const topology = new RedisClusterTopology(topologyNodes)

  const replicationLinks: ReplicationLink[] = []
  const nodeStates = createClusterNodeStates(
    topologyNodes,
    topology,
    options.databasesPerNode ?? 1,
    options.replicaUpdateDelayMs ?? 0,
    profile,
    replicationLinks,
  )

  const nodes = topologyNodes.map<ClusterNodePipeline>(node => {
    const state = nodeStates.get(node.id)
    if (!state) {
      throw new Error(`Missing state for cluster node ${node.id}`)
    }

    const executor = createRedisCommandExecutor({
      extraCommands: createClusterCommands(node.id),
      compatibility: profile,
      policies: [createClusterPolicy({ localNodeId: node.id, topology })],
    })

    return {
      id: node.id,
      role: node.role,
      host: node.host,
      port: node.port,
      state,
      executor,
    }
  })

  return { topology, nodes, replicationLinks }
}

export function computeSlotRange(
  masterIndex: number,
  masters: number,
): [number, number] {
  if (!Number.isInteger(masters) || masters < 1) {
    throw new Error(`Invalid masters count ${masters}`)
  }
  if (
    !Number.isInteger(masterIndex) ||
    masterIndex < 0 ||
    masterIndex >= masters
  ) {
    throw new Error(`Invalid master index ${masterIndex}`)
  }

  const start = Math.floor((REDIS_CLUSTER_SLOT_COUNT * masterIndex) / masters)
  const end =
    Math.floor((REDIS_CLUSTER_SLOT_COUNT * (masterIndex + 1)) / masters) - 1
  return [start, end]
}

function validateOptions(options: RedisClusterOptions): void {
  if (!Number.isInteger(options.masters) || options.masters < 1) {
    throw new Error(`Invalid masters count ${options.masters}`)
  }
  if (
    options.replicasPerMaster !== undefined &&
    (!Number.isInteger(options.replicasPerMaster) ||
      options.replicasPerMaster < 0)
  ) {
    throw new Error(`Invalid replicasPerMaster ${options.replicasPerMaster}`)
  }
  if (
    !Number.isInteger(options.basePort) ||
    options.basePort < 0 ||
    options.basePort > 65535
  ) {
    throw new Error(`Invalid basePort ${options.basePort}`)
  }
  if (
    options.replicaUpdateDelayMs !== undefined &&
    (!Number.isInteger(options.replicaUpdateDelayMs) ||
      options.replicaUpdateDelayMs < 0)
  ) {
    throw new Error(
      `Invalid replicaUpdateDelayMs ${options.replicaUpdateDelayMs}`,
    )
  }
}

export type ReplicationLink = {
  close(): void
}

function createClusterNodeStates(
  nodes: readonly RedisClusterNode[],
  topology: RedisClusterTopology,
  databaseCount: number,
  replicaUpdateDelayMs: number,
  profile: CompatibilityProfile,
  replicationLinks: ReplicationLink[],
): Map<string, RedisServerState> {
  const states = new Map<string, RedisServerState>()

  for (const node of nodes) {
    states.set(
      node.id,
      new RedisServerState({
        databaseCount,
        clusterTopology: topology,
        compatibility: profile,
        activeExpiryIntervalMs: node.role === 'replica' ? false : undefined,
      }),
    )
  }

  for (const node of nodes) {
    if (node.role !== 'replica') {
      continue
    }

    const master = node.masterId ? states.get(node.masterId) : undefined
    const replica = states.get(node.id)
    if (!master || !replica) {
      throw new Error(`Replica ${node.id} references missing master state`)
    }

    replicationLinks.push(
      ...createReplicationLinks(master, replica, replicaUpdateDelayMs),
    )
  }

  return states
}

function createReplicationLinks(
  master: RedisServerState,
  replica: RedisServerState,
  delayMs: number,
): ReplicationLink[] {
  return master.databases.map((masterDb, index) => {
    const replicaDb = replica.getDatabase(index)
    syncDatabase(masterDb, replicaDb)
    return new DatabaseReplicationLink(masterDb, replicaDb, delayMs)
  })
}

function syncDatabase(master: RedisDatabase, replica: RedisDatabase): void {
  replica.flush()
  for (const entry of master.entriesSnapshot()) {
    replica.set(entry.key, entry.value, { expiresAt: entry.expiresAt })
  }
}

class DatabaseReplicationLink implements ReplicationLink {
  private readonly unsubscribe: Unsubscribe
  private readonly timers = new Set<ReturnType<typeof setTimeout>>()

  constructor(
    master: RedisDatabase,
    private readonly replica: RedisDatabase,
    private readonly delayMs: number,
  ) {
    this.unsubscribe = master.subscribe(event => this.replicate(event))
  }

  close(): void {
    this.unsubscribe()
    for (const timer of this.timers) {
      clearTimeout(timer)
    }
    this.timers.clear()
  }

  private replicate(event: RedisMutationEvent): void {
    if (this.delayMs === 0) {
      applyReplicationEvent(this.replica, event)
      return
    }

    const timer = setTimeout(() => {
      this.timers.delete(timer)
      applyReplicationEvent(this.replica, event)
    }, this.delayMs)
    this.timers.add(timer)
  }
}

function applyReplicationEvent(
  replica: RedisDatabase,
  event: RedisMutationEvent,
): void {
  switch (event.type) {
    case 'write':
      replica.set(event.key, event.value, { expiresAt: event.expiresAt })
      return
    case 'delete':
    case 'evict':
      replica.delete(event.key)
      return
    case 'expire':
      replica.expire(event.key, event.expiresAt)
      return
    case 'persist':
      replica.persist(event.key)
      return
    case 'flush':
      replica.flush()
      return
  }
}
