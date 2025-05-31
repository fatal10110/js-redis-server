import { WrongNumberOfArguments } from '../../../../../core/errors'
import {
  Command,
  CommandResult,
  DiscoveryNode,
  DiscoveryService,
} from '../../../../../types'

export const commandName = 'info'

export class ClusterInfoCommand implements Command {
  constructor(
    private readonly me: DiscoveryNode,
    private readonly disconveryService: DiscoveryService,
  ) {}
  getKeys(): Buffer[] {
    return []
  }
  run(rawCommand: Buffer, args: Buffer[]): Promise<CommandResult> {
    if (args.length > 0) {
      throw new WrongNumberOfArguments(`cluster|${commandName}`)
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
