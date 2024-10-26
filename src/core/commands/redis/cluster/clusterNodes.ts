import { Command, CommandResult } from '../../../../types'
import { ClusterNode } from '../../../cluster/clusterNode'

export const commandName = 'nodes'

export class ClusterNodesCommand implements Command {
  constructor(private readonly node: ClusterNode) {}

  getKeys(): Buffer[] {
    return []
  }

  run(rawCommand: Buffer, args: Buffer[]): CommandResult {
    const master: ClusterNode[] = []
    const replicas: ClusterNode[] = []

    for (const clusterNode of this.node.getClusterNodes()) {
      if (clusterNode.masterNodeId) {
        replicas.push(clusterNode)
      } else {
        master.push(clusterNode)
      }
    }

    const res: string[] = []
    const mapping: Record<string, number> = {}

    for (let i = 0; i < master.length; i++) {
      const clusterNode = master[i]
      const configEpoch = i + 1
      mapping[clusterNode.id] = configEpoch

      res.push(this.generateClusterNodeInfo(clusterNode, configEpoch))
    }

    for (const clusterNode of replicas) {
      res.push(
        this.generateClusterNodeInfo(
          clusterNode,
          mapping[clusterNode.masterNodeId!],
        ),
      )
    }

    return { response: Buffer.from(res.join('')) }
  }

  private generateClusterNodeInfo(
    clusterNode: ClusterNode,
    configEpoch: number,
  ) {
    const nodeAddpress = clusterNode.getAddress()
    const connectionDetails = `${nodeAddpress.host}:${nodeAddpress.port}@${nodeAddpress.port}`
    const myselfDefinition = this.node.id === clusterNode.id ? 'myself,' : ''
    const masterSlave = clusterNode.masterNodeId
      ? `slave ${clusterNode.masterNodeId}`
      : `master -`
    // TODO handle ping information
    const pingPong = `0 ${Date.now()}`

    if (!clusterNode.slotRange) {
      throw new Error(`unknonw slot range for node ${clusterNode.id}`)
    }

    const slots = clusterNode.masterNodeId
      ? ''
      : ` ${clusterNode.slotRange.min}-${clusterNode.slotRange.max}`

    return `${clusterNode.id} ${connectionDetails} ${myselfDefinition}${masterSlave} ${pingPong} ${configEpoch} connected${slots}\n`
  }
}
