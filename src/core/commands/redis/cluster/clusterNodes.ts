import { NodeCommand } from '..'
import { Discovery, DiscoveryService } from '../../../cluster/network'
import { Node } from '../../../node'

export class ClusterNodes implements NodeCommand {
  handle(
    discoveryService: DiscoveryService,
    node: Node,
    args: unknown[],
  ): string | Buffer {
    const master: Discovery[] = []
    const slave: Discovery[] = []

    for (const discovery of discoveryService.getNodesAndAddresses()) {
      if (discovery.node.master) {
        slave.push(discovery)
      } else {
        master.push(discovery)
      }
    }

    const res: string[] = []
    const mapping: Record<string, number> = {}

    for (let i = 0; i < master.length; i++) {
      const discovery = master[i]
      const configEpoch = i + 1
      mapping[discovery.node.id] = configEpoch

      res.push(this.generateClusterNodeInfo(discovery, node, configEpoch))
    }

    for (const discovery of slave) {
      res.push(
        this.generateClusterNodeInfo(
          discovery,
          node,
          mapping[discovery.node.master!.id],
        ),
      )
    }

    return Buffer.from(res.join(''))
  }

  private generateClusterNodeInfo(
    discovery: Discovery,
    currentNode: Node,
    configEpoch: number,
  ) {
    const connectionDetails = `${discovery.host}:${discovery.port}@${discovery.port}`
    const myselfDefinition =
      currentNode.id === discovery.node.id ? 'myself,' : ''
    const masterSlave = discovery.node.master
      ? `slave ${discovery.node.master.id}`
      : `master -`
    // TODO handle ping information
    const pingPong = `0 ${Date.now()}`

    if (!discovery.node.slotRange) {
      throw new Error(`unknonw slot range for node ${discovery.node.id}`)
    }

    const slots = discovery.node.master
      ? ''
      : ` ${discovery.node.slotRange.min}-${discovery.node.slotRange.max}`

    return `${discovery.node.id} ${connectionDetails} ${myselfDefinition}${masterSlave} ${pingPong} ${configEpoch} connected${slots}\n`
  }
}

export default new ClusterNodes()
