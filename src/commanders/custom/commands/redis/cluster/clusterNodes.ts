import { WrongNumberOfArguments } from '../../../../../core/errors'
import {
  Command,
  CommandResult,
  DiscoveryNode,
  DiscoveryService,
} from '../../../../../types'
import { defineCommand, CommandCategory } from '../../metadata'
import type { CommandDefinition } from '../../registry'

export const commandName = 'nodes'

export const ClusterNodesCommandDefinition: CommandDefinition = {
  metadata: defineCommand(`cluster|${commandName}`, {
    arity: 1, // CLUSTER NODES
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
      throw new Error('Cluster nodes requires discoveryService and mySelfId')
    }

    const me = deps.discoveryService.getById(deps.mySelfId)
    return new ClusterNodesCommand(me, deps.discoveryService)
  },
}

export class ClusterNodesCommand implements Command {
  readonly metadata = ClusterNodesCommandDefinition.metadata

  constructor(
    private readonly me: DiscoveryNode,
    private readonly discoveryService: DiscoveryService,
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
