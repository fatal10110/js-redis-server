import { DiscoveryNode, DiscoveryService } from '../../../../../types'
import { defineCommand, CommandCategory } from '../../metadata'
import { SchemaCommand, CommandContext, t } from '../../../schema'

export const commandName = 'nodes'

export class ClusterNodesCommand extends SchemaCommand<[]> {
  constructor(
    private readonly discoveryService: DiscoveryService,
    private readonly mySelfId: string,
  ) {
    super()
  }

  metadata = defineCommand(`cluster|${commandName}`, {
    arity: 1, // CLUSTER NODES
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

  protected execute(_args: [], { transport }: CommandContext) {
    const me = this.discoveryService.getById(this.mySelfId)
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
      res.push(
        generateClusterNodeInfo(
          me,
          this.discoveryService,
          clusterNode,
          configEpoch,
        ),
      )
    }
    for (const clusterNode of replicas) {
      const masterNode = this.discoveryService.getMaster(clusterNode.id)
      res.push(
        generateClusterNodeInfo(
          me,
          this.discoveryService,
          clusterNode,
          mapping[masterNode.id],
        ),
      )
    }
    transport.write(Buffer.from(res.join('')))
  }
}

function generateClusterNodeInfo(
  me: DiscoveryNode,
  discoveryService: {
    isMaster(id: string): boolean
    getMaster(id: string): DiscoveryNode
  },
  clusterNode: DiscoveryNode,
  configEpoch: number,
) {
  const master =
    !discoveryService.isMaster(clusterNode.id) &&
    discoveryService.getMaster(clusterNode.id)
  const connectionDetails = `${clusterNode.host}:${clusterNode.port}@${clusterNode.port}`
  const myselfDefinition = me.id === clusterNode.id ? 'myself,' : ''
  const masterSlave = master ? `slave ${master.id}` : `master -`
  const pingPong = `0 ${Date.now()}`
  if (!clusterNode.slots) {
    throw new Error(`unknonw slot range for node ${clusterNode.id}`)
  }
  const slots = master
    ? ''
    : ` ${clusterNode.slots.map(slot => `${slot[0]}-${slot[1]}`).join(',')}`
  return `${clusterNode.id} ${connectionDetails} ${myselfDefinition}${masterSlave} ${pingPong} ${configEpoch} connected${slots}\n`
}
