import { DiscoveryService } from '../../../../../types'
import { defineCommand, CommandCategory } from '../../metadata'
import { SchemaCommand, CommandContext, t } from '../../../schema'

export const commandName = 'info'

export class ClusterInfoCommand extends SchemaCommand<[]> {
  metadata = defineCommand(`cluster|${commandName}`, {
    arity: 1, // CLUSTER INFO
    flags: {
      admin: true,
      readonly: true,
    },
    firstKey: -1,
    lastKey: -1,
    keyStep: 1,
    categories: [CommandCategory.CLUSTER],
  })

  protected schema = t.tuple([])

  protected execute(
    _args: [],
    { discoveryService, mySelfId, transport }: CommandContext,
  ) {
    const service = discoveryService as DiscoveryService | undefined
    if (!service || !mySelfId) {
      throw new Error('Cluster info requires discoveryService and mySelfId')
    }
    const me = service.getById(mySelfId)
    let nodesCount = 0
    let masters = 0
    let myEpoch = 0
    const myMaster = service.getMaster(me.id)
    for (const clusterNode of service.getAll()) {
      nodesCount += 1
      if (service.isMaster(clusterNode.id)) {
        masters += 1
      }
      if (me.id === clusterNode.id || myMaster.id === clusterNode.id) {
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
    transport.write(Buffer.from(`${values.join('\n')}\n`))
  }
}
