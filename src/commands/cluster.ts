import {
  defineCommand,
  type CommandDefinition,
} from '../core/command-definition'
import { t } from '../core/command-schema'
import {
  UnknownClusterSubcommandError,
  WrongNumberOfArgumentsError,
} from '../core/redis-error'
import { RedisResult } from '../core/redis-result'
import { RedisValue } from '../core/redis-value'
import {
  REDIS_CLUSTER_SLOT_COUNT,
  type RedisClusterNode,
  type RedisClusterTopology,
} from '../state'

/**
 * Builds the CLUSTER command bound to a specific node id. Each cluster node
 * gets its own definition so that subcommands like NODES/MYID can report the
 * "myself" marker correctly. Topology is read from ctx.server at execute time.
 */
export function createClusterCommand(localNodeId: string): CommandDefinition {
  return defineCommand({
    name: 'cluster',
    schema: t.object({
      subcommand: t.string(),
      rest: t.variadic(t.bulk()),
    }),
    flags: ['admin'],
    keys: () => [],
    execute: (args, ctx) => {
      const topology = ctx.server.clusterTopology
      const subcommand = args.subcommand.toLowerCase()

      switch (subcommand) {
        case 'slots':
          expectClusterRestLength(args.rest, 'cluster|slots')
          return clusterSlots(topology)
        case 'shards':
          expectClusterRestLength(args.rest, 'cluster|shards')
          return clusterShards(topology)
        case 'nodes':
          expectClusterRestLength(args.rest, 'cluster|nodes')
          return clusterNodes(topology, localNodeId)
        case 'info':
          expectClusterRestLength(args.rest, 'cluster|info')
          return clusterInfo(topology, localNodeId)
        case 'myid':
          expectClusterRestLength(args.rest, 'cluster|myid')
          return RedisResult.create(
            RedisValue.bulkString(Buffer.from(localNodeId)),
          )
        default:
          throw new UnknownClusterSubcommandError(args.subcommand)
      }
    },
  })
}

function expectClusterRestLength(
  rest: readonly Buffer[],
  commandName: string,
): void {
  if (rest.length !== 0) {
    throw new WrongNumberOfArgumentsError(commandName)
  }
}

function masters(topology: RedisClusterTopology): RedisClusterNode[] {
  return topology.nodes.filter(node => node.role === 'master')
}

function replicasOf(
  topology: RedisClusterTopology,
  masterId: string,
): RedisClusterNode[] {
  return topology.nodes.filter(
    node => node.role === 'replica' && node.masterId === masterId,
  )
}

function nodeEntry(node: RedisClusterNode): RedisValue {
  return RedisValue.array([
    RedisValue.bulkString(Buffer.from(node.host)),
    RedisValue.integer(node.port),
    RedisValue.bulkString(Buffer.from(node.id)),
    RedisValue.array([]),
  ])
}

function clusterSlots(topology: RedisClusterTopology): RedisResult {
  const slots: RedisValue[] = []
  for (const master of masters(topology)) {
    const master_ = nodeEntry(master)
    const replicas = replicasOf(topology, master.id).map(nodeEntry)
    for (const [min, max] of master.slots) {
      slots.push(
        RedisValue.array([
          RedisValue.integer(min),
          RedisValue.integer(max),
          master_,
          ...replicas,
        ]),
      )
    }
  }
  return RedisResult.create(RedisValue.array(slots))
}

function clusterShards(topology: RedisClusterTopology): RedisResult {
  const shards: RedisValue[] = []
  for (const master of masters(topology)) {
    const slotRanges: RedisValue[] = []
    for (const [min, max] of master.slots) {
      slotRanges.push(RedisValue.integer(min), RedisValue.integer(max))
    }

    const shardNodes = [master, ...replicasOf(topology, master.id)].map(node =>
      RedisValue.array([
        RedisValue.bulkString(Buffer.from('id')),
        RedisValue.bulkString(Buffer.from(node.id)),
        RedisValue.bulkString(Buffer.from('port')),
        RedisValue.integer(node.port),
        RedisValue.bulkString(Buffer.from('ip')),
        RedisValue.bulkString(Buffer.from(node.host)),
        RedisValue.bulkString(Buffer.from('endpoint')),
        RedisValue.bulkString(Buffer.from(node.host)),
        RedisValue.bulkString(Buffer.from('role')),
        RedisValue.bulkString(Buffer.from(node.role)),
        RedisValue.bulkString(Buffer.from('replication-offset')),
        RedisValue.integer(0),
        RedisValue.bulkString(Buffer.from('health')),
        RedisValue.bulkString(Buffer.from('online')),
      ]),
    )

    shards.push(
      RedisValue.array([
        RedisValue.bulkString(Buffer.from('slots')),
        RedisValue.array(slotRanges),
        RedisValue.bulkString(Buffer.from('nodes')),
        RedisValue.array(shardNodes),
      ]),
    )
  }
  return RedisResult.create(RedisValue.array(shards))
}

function clusterNodes(
  topology: RedisClusterTopology,
  localNodeId: string,
): RedisResult {
  const masterList = masters(topology)
  const epochs = new Map<string, number>()
  masterList.forEach((master, index) => epochs.set(master.id, index + 1))

  const lines: string[] = []
  for (const node of topology.nodes) {
    const epoch =
      node.role === 'master'
        ? (epochs.get(node.id) ?? 0)
        : (epochs.get(node.masterId ?? '') ?? 0)
    lines.push(formatNodeLine(node, localNodeId, epoch))
  }

  return RedisResult.create(RedisValue.bulkString(Buffer.from(lines.join(''))))
}

function formatNodeLine(
  node: RedisClusterNode,
  localNodeId: string,
  configEpoch: number,
): string {
  const connection = `${node.host}:${node.port}@${node.port}`
  const myself = node.id === localNodeId ? 'myself,' : ''
  const roleField =
    node.role === 'master' ? 'master -' : `slave ${node.masterId}`
  const ping = `0 ${Date.now()}`
  const slots =
    node.role === 'master'
      ? node.slots.map(([min, max]) => ` ${min}-${max}`).join('')
      : ''
  return `${node.id} ${connection} ${myself}${roleField} ${ping} ${configEpoch} connected${slots}\n`
}

function clusterInfo(
  topology: RedisClusterTopology,
  localNodeId: string,
): RedisResult {
  const masterList = masters(topology)
  const knownNodes = topology.nodes.length
  const size = masterList.length

  const local = topology.getNode(localNodeId)
  const myMasterId =
    local?.role === 'replica' ? (local.masterId ?? localNodeId) : localNodeId
  const myEpoch = masterList.findIndex(master => master.id === myMasterId) + 1

  const lines = [
    'cluster_enabled:1',
    'cluster_state:ok',
    `cluster_slots_assigned:${REDIS_CLUSTER_SLOT_COUNT}`,
    `cluster_slots_ok:${REDIS_CLUSTER_SLOT_COUNT}`,
    'cluster_slots_pfail:0',
    'cluster_slots_fail:0',
    `cluster_known_nodes:${knownNodes}`,
    `cluster_size:${size}`,
    `cluster_current_epoch:${size}`,
    `cluster_my_epoch:${myEpoch}`,
    'cluster_stats_messages_sent:0',
    'cluster_stats_messages_received:0',
    'total_cluster_links_buffer_limit_exceeded:0',
  ]

  return RedisResult.create(
    RedisValue.bulkString(Buffer.from(`${lines.join('\r\n')}\r\n`)),
  )
}
