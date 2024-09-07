import { Server } from 'net'
import { Node, SlotRange } from '../node'
import { Logger, ServerNetwork } from '../server'
import { DB } from '../db'
import commands from '../commands/redis'

export type Discovery = { node: Node; port: number; host: string }

const slots = 16384

export interface DiscoveryService {
  getNodeAndAdressBySlot(slot: number): Discovery
  getNodesAndAddresses(): Discovery[]
}

export class ClusterNetwork implements DiscoveryService {
  private readonly cluster: Map<Server, Node> = new Map()
  private readonly serverNetwork: ServerNetwork

  constructor(private readonly logger: Logger) {
    this.serverNetwork = new ServerNetwork(logger)
  }

  getNodesAndAddresses(): Discovery[] {
    const discoveries: Discovery[] = []

    for (const [server, node] of this.cluster.entries()) {
      discoveries.push(toDiscovery(node, server))
    }

    return discoveries
  }

  getNodeAndAdressBySlot(slot: number): Discovery {
    for (const [server, node] of this.cluster.entries()) {
      if (!node.slotRange) {
        throw new Error(`Missing slot range on node ${node.id}`)
      }

      if (node.slotRange.max >= slot && node.slotRange.min <= slot) {
        return toDiscovery(node, server)
      }
    }

    throw new Error(`unknown slot ${slot}`)
  }

  async init(params: { masters: number; slaves: number }) {
    const db = new DB()

    for (let i = 0; i < params.masters; i++) {
      const slotRange: SlotRange = {
        min: Math.round((slots * i) / params.masters),
        max: Math.round((slots * (i + 1)) / params.masters),
      }
      const node = new Node(db, commands, slotRange, this)
      const server = this.serverNetwork.createInterface(node)
      this.cluster.set(server, node)

      for (let j = 0; j < params.slaves; j++) {
        const slave = node.createReplica()
        const server = this.serverNetwork.createInterface(slave)
        this.cluster.set(server, slave)
      }
    }

    let size = this.cluster.size

    await new Promise<void>(resolve => {
      let i = 0
      for (const server of this.cluster.keys()) {
        server
          .on('listening', () => {
            // @ts-ignore
            this.logger.info(`listening on port ${server.address().port}`)

            if (--size === 0) {
              resolve()
            }
          })
          .listen(8010 - i++)
      }
    })
  }

  async shutdown() {
    let size = this.cluster.size

    await new Promise<void>(resolve => {
      for (const server of this.cluster.keys()) {
        server.on('close', () => {
          if (--size === 0) {
            resolve()
          }
        })

        server.close(console.error)
      }
    })
  }
}

function toDiscovery(node: Node, server: Server): Discovery {
  const serverAddresses = server.address()

  if (!serverAddresses) {
    throw new Error(`Network error: no address for node ${node.id}`)
  }

  if (typeof serverAddresses === 'string') {
    const [host, port] = serverAddresses.split(':')
    return { node, host, port: Number(port) }
  } else {
    const { port } = serverAddresses
    const host = '127.0.0.1' // TODO
    return { node, host, port }
  }
}
