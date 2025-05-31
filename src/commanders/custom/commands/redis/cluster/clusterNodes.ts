import { WrongNumberOfArguments } from '../../../../../core/errors'
import {
  Command,
  CommandResult,
  DiscoveryNode,
  DiscoveryService,
} from '../../../../../types'

export const commandName = 'nodes'

export class ClusterNodesCommand implements Command {
  constructor(
    private readonly me: DiscoveryNode,
    private readonly discoveryService: DiscoveryService,
  ) {}

  getKeys(): Buffer[] {
    return []
  }

  run(rawCommand: Buffer, args: Buffer[]): Promise<CommandResult> {
    if (args.length > 0) {
      throw new WrongNumberOfArguments(`cluster|${commandName}`)
    }

    const master: DiscoveryNode[] = []
    const replicas: DiscoveryNode[] = []

    for (const clusterNode of this.discoveryService.getAll()) {
      if (this.discoveryService.isMaster(clusterNode.id)) {
        master.push(clusterNode)
      } else {
        replicas.push(clusterNode)
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
      const master = this.discoveryService.getMaster(clusterNode.id)

      res.push(this.generateClusterNodeInfo(clusterNode, mapping[master.id]))
    }

    return Promise.resolve({ response: Buffer.from(res.join('')) })
  }

  private generateClusterNodeInfo(
    clusterNode: DiscoveryNode,
    configEpoch: number,
  ) {
    const master =
      !this.discoveryService.isMaster(clusterNode.id) &&
      this.discoveryService.getMaster(clusterNode.id)
    const connectionDetails = `${clusterNode.host}:${clusterNode.port}@${clusterNode.port}`
    const myselfDefinition = this.me.id === clusterNode.id ? 'myself,' : ''
    const masterSlave = master ? `slave ${master.id}` : `master -`
    // TODO handle ping information
    const pingPong = `0 ${Date.now()}`

    if (!clusterNode.slots) {
      throw new Error(`unknonw slot range for node ${clusterNode.id}`)
    }

    const slots = master
      ? ''
      : ` ${clusterNode.slots.map(slot => `${slot[0]}-${slot[1]}`).join(',')}`

    return `${clusterNode.id} ${connectionDetails} ${myselfDefinition}${masterSlave} ${pingPong} ${configEpoch} connected${slots}\n`
  }
}
