import { createRedisCommandExecutor } from './commands'
import { createClusterCommands } from './commands/cluster'
import { createClusterPolicy } from './core/execution-policies'
import { Resp2Server } from './core/transports/resp2/server'
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

export type RedisClusterNodeHandle = {
  id: string
  role: 'master' | 'replica'
  host: string
  port: number
  server: RedisServerState
}

export class RedisCluster {
  readonly topology: RedisClusterTopology
  readonly nodes: readonly RedisClusterNodeHandle[]

  private readonly servers: Resp2Server[]
  private readonly replicationLinks: ReplicationLink[]

  constructor(
    topology: RedisClusterTopology,
    nodes: readonly RedisClusterNodeHandle[],
    servers: readonly Resp2Server[],
    replicationLinks: readonly ReplicationLink[],
  ) {
    this.topology = topology
    this.nodes = nodes
    this.servers = [...servers]
    this.replicationLinks = [...replicationLinks]
  }

  async listen(): Promise<void> {
    try {
      for (let index = 0; index < this.servers.length; index++) {
        await this.servers[index].listen(this.nodes[index].port)
      }
    } catch (err) {
      await this.close()
      throw err
    }

    // Backfill actual ports — when basePort is 0 the OS assigns random ports
    // and the handles/topology need to reflect what was actually bound.
    this.servers.forEach((server, index) => {
      const actualPort = server.getPort()
      ;(this.nodes[index] as { port: number }).port = actualPort
      ;(this.topology.nodes[index] as { port: number }).port = actualPort
    })
  }

  async close(): Promise<void> {
    for (const link of this.replicationLinks) {
      link.close()
    }
    const results = await Promise.allSettled(
      this.servers.map(server => server.close()),
    )
    for (const result of results) {
      if (result.status === 'fulfilled') {
        continue
      }
      if (isServerNotRunningError(result.reason)) {
        continue
      }
      throw result.reason
    }
  }

  getAddresses(): string[] {
    return this.nodes.map(node => `${node.host}:${node.port}`)
  }
}

function isServerNotRunningError(err: unknown): boolean {
  return (err as { code?: string }).code === 'ERR_SERVER_NOT_RUNNING'
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

/**
 * Builds an **un-started** cluster — call {@link RedisCluster.listen} yourself.
 *
 * @deprecated Prefer {@link createRedisServer} with the `cluster` option, which
 * builds *and* starts the cluster in one call (and is symmetric with
 * `createRedisMock({ cluster })`). This low-level builder remains for callers
 * that need to control when `listen()` runs.
 */
export function createRedisCluster(options: RedisClusterOptions): RedisCluster {
  const { topology, nodes, replicationLinks } = buildClusterNodes(options)

  const handles: RedisClusterNodeHandle[] = []
  const servers: Resp2Server[] = []

  for (const node of nodes) {
    const server = new Resp2Server({
      server: node.state,
      executor: node.executor,
      logger: options.logger,
      nodeRole: node.role,
    })

    handles.push({
      id: node.id,
      role: node.role,
      host: node.host,
      port: node.port,
      server: node.state,
    })
    servers.push(server)
  }

  return new RedisCluster(topology, handles, servers, replicationLinks)
}

/**
 * @deprecated Renamed to {@link createRedisCluster} for naming consistency with
 * `createRedisServer` / `createRedisMock`. This alias will be removed in a
 * future release.
 */
export const buildRedisCluster = createRedisCluster

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
