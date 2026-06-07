import { createRedisCommandExecutor } from './commands'
import { createClusterCommand } from './commands/cluster'
import { createClusterPolicy } from './core/execution-policies'
import { Resp2Server } from './core/transports/resp2/server'
import {
  REDIS_CLUSTER_SLOT_COUNT,
  RedisClusterTopology,
  RedisServerState,
  type RedisClusterNode,
} from './state'
import type { Logger } from './logger'

export type RedisClusterOptions = {
  masters: number
  replicasPerMaster?: number
  basePort: number
  host?: string
  databasesPerNode?: number
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

  constructor(
    topology: RedisClusterTopology,
    nodes: readonly RedisClusterNodeHandle[],
    servers: readonly Resp2Server[],
  ) {
    this.topology = topology
    this.nodes = nodes
    this.servers = [...servers]
  }

  async listen(): Promise<void> {
    await Promise.all(
      this.servers.map((server, index) =>
        server.listen(this.nodes[index].port),
      ),
    )
    // Backfill actual ports — when basePort is 0 the OS assigns random ports
    // and the handles/topology need to reflect what was actually bound.
    this.servers.forEach((server, index) => {
      const actualPort = server.getPort()
      ;(this.nodes[index] as { port: number }).port = actualPort
      ;(this.topology.nodes[index] as { port: number }).port = actualPort
    })
  }

  async close(): Promise<void> {
    await Promise.all(this.servers.map(server => server.close()))
  }

  getAddresses(): string[] {
    return this.nodes.map(node => `${node.host}:${node.port}`)
  }
}

export function buildRedisCluster(options: RedisClusterOptions): RedisCluster {
  validateOptions(options)

  const host = options.host ?? '127.0.0.1'
  const replicas = options.replicasPerMaster ?? 0
  const totalNodes = options.masters * (replicas + 1)

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

  const handles: RedisClusterNodeHandle[] = []
  const servers: Resp2Server[] = []

  for (const node of topologyNodes) {
    const state = new RedisServerState({
      databaseCount: options.databasesPerNode ?? 1,
      clusterTopology: topology,
    })
    const executor = createRedisCommandExecutor({
      extraCommands: [createClusterCommand(node.id)],
      policies: [createClusterPolicy({ localNodeId: node.id, topology })],
    })
    const server = new Resp2Server({
      server: state,
      executor,
      logger: options.logger,
    })

    handles.push({
      id: node.id,
      role: node.role,
      host: node.host,
      port: node.port,
      server: state,
    })
    servers.push(server)
  }

  return new RedisCluster(topology, handles, servers)
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
}
