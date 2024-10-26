import { Command, CommandResult } from '../../../../types'
import { ClusterNode } from '../../../cluster/clusterNode'

export const commandName = 'info'

export class ClusterInfoCommand implements Command {
  constructor(private readonly node: ClusterNode) {}
  getKeys(): Buffer[] {
    return []
  }

  run(rawCommand: Buffer, args: Buffer[]): CommandResult {
    let nodesCount = 0
    let masters = 0
    let myEpoch = 0

    for (const clusterNode of this.node.getClusterNodes()) {
      nodesCount++

      if (!clusterNode.masterNodeId) {
        masters++
      }

      if (
        this.node.id === clusterNode.id ||
        this.node.masterNodeId === clusterNode.id
      ) {
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
    return { response: Buffer.from(`${values.join('\n')}\n`) }
  }
}
