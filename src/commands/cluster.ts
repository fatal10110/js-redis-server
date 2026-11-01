import { asciiLowerCase } from '../core/ascii-case'
import type { CompatibilityProfile } from '../core/compatibility'
import {
  defineCommand,
  type CommandDefinition,
} from '../core/command-definition'
import { t } from '../core/command-schema'
import { formatHostPort } from '../core/network-address'
import {
  errors,
  RedisCommandError,
  WrongNumberOfArgumentsError,
} from '../core/redis-error'
import { RedisResult } from '../core/redis-result'
import { RedisValue } from '../core/redis-value'
import {
  keyHashSlot,
  REDIS_CLUSTER_SLOT_COUNT,
  type RedisClusterNode,
  type RedisClusterTopology,
  type RedisDatabase,
} from '../state'
import { parseInt64Token, unknownSubcommandError } from './helpers'
import { subcommandSyntaxError } from '../core/subcommand-errors'
import { commandSubcommandInfo } from './introspection'

const clusterSchema = t.object({
  // Raw bytes, not `t.string()`: the unknown-subcommand reply echoes the
  // name the client sent, and a UTF-8 decode here would lose its bytes.
  subcommand: t.bulk(),
  rest: t.variadic(t.bulk()),
})

// The subcommands COMMAND lists under `cluster`, in cluster and standalone
// mode alike: the ones a cluster node implements.
const clusterIntrospection = {
  subcommands: [
    commandSubcommandInfo('cluster|info', 2),
    commandSubcommandInfo('cluster|nodes', 2),
    commandSubcommandInfo('cluster|slots', 2),
    commandSubcommandInfo('cluster|shards', 2),
    commandSubcommandInfo('cluster|myid', 2),
    commandSubcommandInfo('cluster|keyslot', 3),
    commandSubcommandInfo('cluster|countkeysinslot', 3),
    commandSubcommandInfo('cluster|getkeysinslot', 4),
    commandSubcommandInfo('cluster|setslot', -4),
  ],
}

/**
 * Builds the CLUSTER command bound to a specific node id. Each cluster node
 * gets its own definition so that subcommands like NODES/MYID can report the
 * "myself" marker correctly. Topology is read from ctx.server at execute time.
 */
export function createClusterCommand(localNodeId: string): CommandDefinition {
  return defineCommand({
    name: 'cluster',
    schema: clusterSchema,
    flags: ['admin'],
    introspection: clusterIntrospection,
    keys: () => [],
    execute: (args, ctx) => {
      const topology = ctx.server.clusterTopology
      const subcommand = asciiLowerCase(args.subcommand.toString())

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
          return clusterInfo(topology, localNodeId, ctx.server.profile)
        case 'myid':
          expectClusterRestLength(args.rest, 'cluster|myid')
          return RedisResult.create(
            RedisValue.bulkString(Buffer.from(localNodeId)),
          )
        case 'keyslot':
          expectSubcommandArgs(args.rest, 1, args.subcommand, ctx)
          return RedisResult.create(
            RedisValue.integer(keyHashSlot(args.rest[0])),
          )
        case 'countkeysinslot': {
          expectSubcommandArgs(args.rest, 1, args.subcommand, ctx)
          const slot = parseInt64Token(args.rest[0])
          if (slot < 0n || slot >= BigInt(REDIS_CLUSTER_SLOT_COUNT)) {
            throw new RedisCommandError('Invalid slot')
          }
          return RedisResult.create(
            RedisValue.integer(
              keysInSlot(ctx.server.getDatabase(0), Number(slot)).length,
            ),
          )
        }
        case 'getkeysinslot': {
          expectSubcommandArgs(args.rest, 2, args.subcommand, ctx)
          const slot = parseInt64Token(args.rest[0])
          const count = parseInt64Token(args.rest[1])
          if (
            slot < 0n ||
            slot >= BigInt(REDIS_CLUSTER_SLOT_COUNT) ||
            count < 0n
          ) {
            throw new RedisCommandError('Invalid slot or number of keys')
          }
          const keys = keysInSlot(ctx.server.getDatabase(0), Number(slot))
          return RedisResult.create(
            RedisValue.array(
              keys
                .slice(
                  0,
                  count > BigInt(keys.length) ? keys.length : Number(count),
                )
                .map(key => RedisValue.bulkString(Buffer.from(key))),
            ),
          )
        }
        case 'setslot':
          if (args.rest.length < 2) {
            throw subcommandSyntaxError(
              'CLUSTER',
              args.subcommand,
              ctx.server.profile,
            )
          }
          return clusterSetSlot(
            args.rest,
            topology,
            localNodeId,
            ctx.server.getDatabase(0),
            ctx.server.profile,
          )
        default:
          throw unknownSubcommandError(
            'CLUSTER',
            args.subcommand,
            ctx.server.profile,
          )
      }
    },
  })
}

export function createClusterCommands(
  localNodeId: string,
): readonly CommandDefinition[] {
  return [
    createClusterCommand(localNodeId),
    readonlyCommand,
    readwriteCommand,
    askingCommand,
  ]
}

/**
 * ASKING: the client follows an `-ASK` redirect, so its next command may use
 * a slot this node is importing. The flag lasts one command, or until EXEC
 * when the client then opens MULTI (see `ClientSession.execute`). A script's
 * own ASKING changes nothing: Redis copies the caller's flag onto the script
 * client before every call.
 */
export const askingCommand = defineCommand({
  name: 'asking',
  schema: t.object({}),
  flags: ['fast'],
  keys: () => [],
  execute: (_args, ctx) => {
    if (!ctx.inScript) {
      ctx.session.setClusterAsking(true)
    }
    return RedisResult.ok()
  },
})

export const readonlyCommand = defineCommand({
  name: 'readonly',
  schema: t.object({}),
  flags: ['readonly', 'fast'],
  keys: () => [],
  execute: (_args, ctx) => {
    ctx.session.setClusterReadOnly(true)
    return RedisResult.ok()
  },
})

export const readwriteCommand = defineCommand({
  name: 'readwrite',
  schema: t.object({}),
  flags: ['readonly', 'fast'],
  keys: () => [],
  execute: (_args, ctx) => {
    ctx.session.setClusterReadOnly(false)
    return RedisResult.ok()
  },
})

/**
 * CLUSTER, READONLY and READWRITE as a standalone (`cluster-enabled no`)
 * server has them (#537): in the command table, so COMMAND lists them, but
 * refusing to act. Every standalone executor registers these;
 * `createRedisCommandRegistry` drops each one a cluster node's
 * {@link createClusterCommands} replaces.
 *
 * CLUSTER answers `This instance has cluster support disabled` for every
 * call that reaches it. From 7.0 an unknown subcommand, or a wrong argument
 * count for a known one, never does: command lookup refuses it first
 * (`CommandExecutor.plan()`), as on a real server. 6.2 has no such lookup,
 * so there everything after the container's own arity check is refused as
 * disabled. Captured from redis-server 7.0.15:
 *
 * ```
 * CLUSTER INFO        -> -ERR This instance has cluster support disabled
 * CLUSTER HELP        -> -ERR This instance has cluster support disabled
 * CLUSTER BOGUS       -> -ERR unknown subcommand 'BOGUS'. Try CLUSTER HELP.
 * CLUSTER INFO extra  -> -ERR wrong number of arguments for 'cluster|info' command
 * ```
 */
export const standaloneClusterCommand = defineCommand({
  name: 'cluster',
  schema: clusterSchema,
  flags: ['admin'],
  introspection: clusterIntrospection,
  keys: () => [],
  execute: () => {
    throw errors.clusterSupportDisabled()
  },
})

/**
 * Refused on a standalone server up to Valkey 8.0, which accepts it (see
 * `cluster.standalone-readonly-allowed`). Accepting it only sets the flag a
 * cluster node's routing reads; standalone mode has no such routing.
 */
export const standaloneReadonlyCommand = defineCommand({
  name: 'readonly',
  schema: t.object({}),
  flags: ['readonly', 'fast'],
  keys: () => [],
  execute: (_args, ctx) => {
    if (!ctx.server.profile.has('cluster.standalone-readonly-allowed')) {
      throw errors.clusterSupportDisabled()
    }
    ctx.session.setClusterReadOnly(true)
    return RedisResult.ok()
  },
})

/**
 * Accepted on a standalone 6.2, refused from Redis 7.0 / Valkey 7.2 (see
 * `cluster.standalone-readwrite-refused`), accepted again from Valkey 8.0.
 */
export const standaloneReadwriteCommand = defineCommand({
  name: 'readwrite',
  schema: t.object({}),
  flags: ['readonly', 'fast'],
  keys: () => [],
  execute: (_args, ctx) => {
    const profile = ctx.server.profile
    if (
      profile.has('cluster.standalone-readwrite-refused') &&
      !profile.has('cluster.standalone-readonly-allowed')
    ) {
      throw errors.clusterSupportDisabled()
    }
    ctx.session.setClusterReadOnly(false)
    return RedisResult.ok()
  },
})

/** ASKING on a standalone server: refused, on every version. */
export const standaloneAskingCommand = defineCommand({
  name: 'asking',
  schema: t.object({}),
  flags: ['fast'],
  keys: () => [],
  execute: () => {
    throw errors.clusterSupportDisabled()
  },
})

export const standaloneClusterCommands: readonly CommandDefinition[] = [
  standaloneClusterCommand,
  standaloneReadonlyCommand,
  standaloneReadwriteCommand,
  standaloneAskingCommand,
]

/**
 * A known subcommand given the wrong number of arguments. From 7.0 the
 * command table's arity refuses it before CLUSTER runs; 6.2 has no such
 * lookup and answers its syntax error.
 */
function expectSubcommandArgs(
  rest: readonly Buffer[],
  count: number,
  subcommand: Buffer,
  ctx: { server: { profile: CompatibilityProfile } },
): void {
  if (rest.length !== count) {
    throw subcommandSyntaxError('CLUSTER', subcommand, ctx.server.profile)
  }
}

/** The keys of `db` that hash to `slot`, in the database's order. */
function keysInSlot(db: RedisDatabase, slot: number): Buffer[] {
  return db.keys().filter(key => keyHashSlot(key) === slot)
}

/**
 * CLUSTER SETSLOT `<slot>` MIGRATING|IMPORTING|NODE `<id>` | STABLE
 * (redis 7.2.4 cluster.c, valkey 8.0.0 `clusterParseSetSlotCommand`). The
 * open-slot state is this node's own; NODE moves the slot for every node
 * at once (the mock has no gossip). A master that gives its last slot away
 * stays a master here, where a real one turns into a replica of the new
 * owner (`cluster-allow-replica-migration`).
 */
function clusterSetSlot(
  rest: readonly Buffer[],
  topology: RedisClusterTopology,
  localNodeId: string,
  db: RedisDatabase,
  profile: CompatibilityProfile,
): RedisResult {
  if (topology.getNode(localNodeId)?.role !== 'master') {
    throw new RedisCommandError('Please use SETSLOT only with masters.')
  }

  const slot = parseSlot(rest[0])
  const action = asciiLowerCase(rest[1].toString())
  const argc = rest.length + 2
  // Valkey 8.0+ parses optional arguments (TIMEOUT) after the action.
  const fits = (expected: number) =>
    profile.has('cluster.setslot-extra-args-ignored')
      ? argc >= expected
      : argc === expected
  const owner = topology.getSlotOwner(slot)
  const lookupMaster = (unknown: string): RedisClusterNode => {
    const id = rest[2].toString()
    const node = topology.getNode(id)
    if (!node) {
      throw new RedisCommandError(`${unknown} ${id}`)
    }
    if (node.role !== 'master') {
      throw new RedisCommandError('Target node is not a master')
    }
    return node
  }

  if (action === 'migrating' && fits(5)) {
    if (owner?.id !== localNodeId) {
      throw new RedisCommandError(`I'm not the owner of hash slot ${slot}`)
    }
    const target = lookupMaster("I don't know about node")
    topology.setMigrating(localNodeId, slot, target.id)
  } else if (action === 'importing' && fits(5)) {
    if (owner?.id === localNodeId) {
      throw new RedisCommandError(`I'm already the owner of hash slot ${slot}`)
    }
    const source = lookupMaster("I don't know about node")
    topology.setImporting(localNodeId, slot, source.id)
  } else if (action === 'stable' && fits(4)) {
    topology.setImporting(localNodeId, slot, null)
    topology.setMigrating(localNodeId, slot, null)
  } else if (action === 'node' && fits(5)) {
    const target = lookupMaster('Unknown node')
    const keyCount = keysInSlot(db, slot).length
    if (owner?.id === localNodeId && target.id !== localNodeId && keyCount) {
      throw new RedisCommandError(
        `Can't assign hashslot ${slot} to a different node while I still hold keys for this hash slot.`,
      )
    }
    if (keyCount === 0) {
      topology.setMigrating(localNodeId, slot, null)
    }
    topology.assignSlot(slot, target.id)
    if (target.id === localNodeId) {
      topology.setImporting(localNodeId, slot, null)
    }
  } else {
    throw new RedisCommandError(
      'Invalid CLUSTER SETSLOT action or number of arguments. Try CLUSTER HELP',
    )
  }

  return RedisResult.ok()
}

/** `getSlotOrReply`: an integer in 0..16383. */
function parseSlot(token: Buffer): number {
  let slot: bigint
  try {
    slot = parseInt64Token(token)
  } catch {
    throw new RedisCommandError('Invalid or out of range slot')
  }
  if (slot < 0n || slot >= BigInt(REDIS_CLUSTER_SLOT_COUNT)) {
    throw new RedisCommandError('Invalid or out of range slot')
  }
  return Number(slot)
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

// One entry per run of slots with one owner, in slot order
// (`clusterReplyMultiBulkSlots` walks slots 0..16383), so a slot moved by
// SETSLOT NODE splits its old owner's range around it.
function clusterSlots(topology: RedisClusterTopology): RedisResult {
  const slots: Array<{ min: number; entry: RedisValue }> = []
  for (const master of masters(topology)) {
    const master_ = nodeEntry(master)
    const replicas = replicasOf(topology, master.id).map(nodeEntry)
    for (const [min, max] of master.slots) {
      slots.push({
        min,
        entry: RedisValue.array([
          RedisValue.integer(min),
          RedisValue.integer(max),
          master_,
          ...replicas,
        ]),
      })
    }
  }
  slots.sort((a, b) => a.min - b.min)
  return RedisResult.create(RedisValue.array(slots.map(slot => slot.entry)))
}

function clusterShards(topology: RedisClusterTopology): RedisResult {
  const shards: RedisValue[] = []
  for (const master of masters(topology)) {
    const slotRanges: RedisValue[] = []
    for (const [min, max] of master.slots) {
      slotRanges.push(RedisValue.integer(min), RedisValue.integer(max))
    }

    const shardNodes = [master, ...replicasOf(topology, master.id)].map(node =>
      RedisValue.map([
        [
          RedisValue.bulkString(Buffer.from('id')),
          RedisValue.bulkString(Buffer.from(node.id)),
        ],
        [
          RedisValue.bulkString(Buffer.from('port')),
          RedisValue.integer(node.port),
        ],
        [
          RedisValue.bulkString(Buffer.from('ip')),
          RedisValue.bulkString(Buffer.from(node.host)),
        ],
        [
          RedisValue.bulkString(Buffer.from('endpoint')),
          RedisValue.bulkString(Buffer.from(node.host)),
        ],
        [
          RedisValue.bulkString(Buffer.from('role')),
          RedisValue.bulkString(Buffer.from(node.role)),
        ],
        [
          RedisValue.bulkString(Buffer.from('replication-offset')),
          RedisValue.integer(0),
        ],
        [
          RedisValue.bulkString(Buffer.from('health')),
          RedisValue.bulkString(Buffer.from('online')),
        ],
      ]),
    )

    shards.push(
      RedisValue.map([
        [
          RedisValue.bulkString(Buffer.from('slots')),
          RedisValue.array(slotRanges),
        ],
        [
          RedisValue.bulkString(Buffer.from('nodes')),
          RedisValue.array(shardNodes),
        ],
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
    lines.push(formatNodeLine(node, localNodeId, epoch, topology))
  }

  return RedisResult.create(RedisValue.bulkString(Buffer.from(lines.join(''))))
}

function formatNodeLine(
  node: RedisClusterNode,
  localNodeId: string,
  configEpoch: number,
  topology: RedisClusterTopology,
): string {
  const connection = `${formatHostPort(node.host, node.port)}@${node.port + 10000}`
  const myself = node.id === localNodeId ? 'myself,' : ''
  const roleField =
    node.role === 'master' ? 'master -' : `slave ${node.masterId}`
  const ping = `0 ${Date.now()}`
  const slots =
    node.role === 'master'
      ? node.slots
          .map(([min, max]) => (min === max ? ` ${min}` : ` ${min}-${max}`))
          .join('')
      : ''
  // Only the node's own line shows the slots it has open for resharding,
  // `[slot->-target]` and `[slot-<-source]` (clusterGenNodeDescription).
  const openSlots =
    node.id === localNodeId
      ? topology
          .openSlots(localNodeId)
          .map(({ slot, state, nodeId }) =>
            state === 'migrating'
              ? ` [${slot}->-${nodeId}]`
              : ` [${slot}-<-${nodeId}]`,
          )
          .join('')
      : ''
  return `${node.id} ${connection} ${myself}${roleField} ${ping} ${configEpoch} connected${slots}${openSlots}\n`
}

function clusterInfo(
  topology: RedisClusterTopology,
  localNodeId: string,
  profile: CompatibilityProfile,
): RedisResult {
  const masterList = masters(topology)
  const knownNodes = topology.nodes.length
  const size = masterList.length

  const local = topology.getNode(localNodeId)
  const myMasterId =
    local?.role === 'replica' ? (local.masterId ?? localNodeId) : localNodeId
  const myEpoch = masterList.findIndex(master => master.id === myMasterId) + 1

  // Real CLUSTER INFO has no cluster_enabled line (INFO's cluster section
  // does).
  const lines = [
    'cluster_state:ok',
    `cluster_slots_assigned:${REDIS_CLUSTER_SLOT_COUNT}`,
    `cluster_slots_ok:${REDIS_CLUSTER_SLOT_COUNT}`,
    'cluster_slots_pfail:0',
    'cluster_slots_fail:0',
    ...(profile.has('cluster.info-node-fail-counts')
      ? [
          'cluster_nodes_pfail:0',
          'cluster_nodes_fail:0',
          'cluster_voting_nodes_pfail:0',
          'cluster_voting_nodes_fail:0',
        ]
      : []),
    `cluster_known_nodes:${knownNodes}`,
    `cluster_size:${size}`,
    `cluster_current_epoch:${size}`,
    `cluster_my_epoch:${myEpoch}`,
    'cluster_stats_messages_sent:0',
    'cluster_stats_messages_received:0',
    ...(profile.has('cluster.info-links-buffer-line')
      ? ['total_cluster_links_buffer_limit_exceeded:0']
      : []),
  ]

  return RedisResult.create(
    RedisValue.bulkString(Buffer.from(`${lines.join('\r\n')}\r\n`)),
  )
}
