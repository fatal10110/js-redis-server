import { WrongNumberOfArguments } from '../../../../../core/errors'
import {
  Command,
  CommandResult,
  DiscoveryNode,
  DiscoveryService,
} from '../../../../../types'
import { defineCommand, CommandCategory } from '../../metadata'
import type { CommandDefinition } from '../../registry'

export const commandName = 'info'

export const ClusterInfoCommandDefinition: CommandDefinition = {
  metadata: defineCommand(`cluster|${commandName}`, {
    arity: 1, // CLUSTER INFO
    flags: {
      admin: true,
      readonly: true,
    },
    firstKey: -1,
    lastKey: -1,
    keyStep: 1,
    categories: [CommandCategory.CLUSTER],
  }),
  factory: deps => {
    if (!deps.discoveryService || !deps.mySelfId) {
      throw new Error('Cluster info requires discoveryService and mySelfId')
    }

    const me = deps.discoveryService.getById(deps.mySelfId)
    return new ClusterInfoCommand(me, deps.discoveryService)
  },
}

export class ClusterInfoCommand implements Command {
  readonly metadata = ClusterInfoCommandDefinition.metadata

  constructor(
    private readonly me: DiscoveryNode,
    private readonly disconveryService: DiscoveryService,
  ) {}
  getKeys(_rawCmd: Buffer, args: Buffer[]): Buffer[] {
    if (args.length > 0) {
      throw new WrongNumberOfArguments(this.metadata.name)
    }

    return []
  }
  run(rawCommand: Buffer, args: Buffer[]): Promise<CommandResult> {
    if (args.length > 0) {
      throw new WrongNumberOfArguments(this.metadata.name)
    }

    let nodesCount = 0
    let masters = 0
    let myEpoch = 0

    const myMaster = this.disconveryService.getMaster(this.me.id)

    for (const clusterNode of this.disconveryService.getAll()) {
      nodesCount++

      if (this.disconveryService.isMaster(clusterNode.id)) {
        masters++
      }

      if (this.me.id === clusterNode.id || myMaster.id === clusterNode.id) {
        // Unify epoch
        myEpoch = masters
      }
    }

    const values = [
      'cluster_state:ok',
      'cluster_slots_assigned:16384',
      'cluster_slots_ok:16384',
      'cluster_slots_pfail:0',
      'cluster_slots_fail:0',
      `cluster_known_nodes:${nodesCount}`,
      `cluster_size:${masters}`,
      `cluster_current_epoch:${masters}`,
      `cluster_my_epoch:${myEpoch}`,
      'cluster_stats_messages_ping_sent:66185',
      'cluster_stats_messages_pong_sent:66425',
      'cluster_stats_messages_meet_sent:1',
      'cluster_stats_messages_sent:132611',
      'cluster_stats_messages_ping_received:66425',
      'cluster_stats_messages_pong_received:66186',
      'cluster_stats_messages_received:132611',
      'total_cluster_links_buffer_limit_exceeded:0',
    ]

    // TODO handle with proper data
    return Promise.resolve({ response: Buffer.from(`${values.join('\n')}\n`) })
  }
}
