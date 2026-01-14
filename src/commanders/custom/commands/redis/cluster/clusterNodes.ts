import { DiscoveryNode, DiscoveryService } from '../../../../../types'
import { DB } from '../../../db'
import { defineCommand, CommandCategory } from '../../metadata'
import {
  createSchemaCommand,
  SchemaCommandRegistration,
  t,
} from '../../../schema'

export const commandName = 'nodes'

const metadata = defineCommand(`cluster|${commandName}`, {
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

export const ClusterNodesCommandDefinition: SchemaCommandRegistration<[]> = {
  metadata,
  schema: t.tuple([]),
  handler: async (_args, { discoveryService, mySelfId }) => {
    const service = discoveryService as DiscoveryService | undefined
    if (!service || !mySelfId) {
      throw new Error('Cluster nodes requires discoveryService and mySelfId')
    }

    const me = service.getById(mySelfId)
    const master: DiscoveryNode[] = []
    const replicas: DiscoveryNode[] = []

    for (const clusterNode of service.getAll()) {
      if (service.isMaster(clusterNode.id)) {
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

      res.push(generateClusterNodeInfo(me, service, clusterNode, configEpoch))
    }

    for (const clusterNode of replicas) {
      const masterNode = service.getMaster(clusterNode.id)

      res.push(
        generateClusterNodeInfo(
          me,
          service,
          clusterNode,
          mapping[masterNode.id],
        ),
      )
    }

    return { response: Buffer.from(res.join('')) }
  },
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

export default function (
  db: DB,
  discoveryService: DiscoveryService,
  mySelfId: string,
) {
  return createSchemaCommand(ClusterNodesCommandDefinition, {
    db,
    discoveryService,
    mySelfId,
  })
}
