import { createCustomClusterCommander } from '../../commanders/custom/clusterCommander'
import { IORedisMockClusterCommanderFactory } from '../../commanders/ioredis-mock'
import {
  ClusterCommanderFactory,
  DiscoveryNode,
  DiscoveryService,
  Logger,
  SlotRange,
} from '../../types'
import { Resp2Transport } from '../transports/resp2'

const slots = 16384

export class ClusterNetwork implements DiscoveryService {
  private readonly slotMapping: Record<string, SlotRange[]> = {}
  private readonly transports: Record<string, Resp2Transport> = {}
  private commanderFactory?: ClusterCommanderFactory

  constructor(private readonly logger: Logger) {}

  private createMasterId(index: number) {
    return `master-${index}`
  }

  private createReplicaId(masterId: string, index: number) {
    return `replica-${index}-${masterId}`
  }

  getMaster(id: string): DiscoveryNode {
    const masterInex = Number(id.split('-').at(-1))

    return this.getById(this.createMasterId(masterInex))
  }

  isMaster(id: string): boolean {
    return id.startsWith('master-')
  }

  getById(id: string): DiscoveryNode {
    const [host, port] = this.transports[id].getAddress().split(':')

    return {
      id,
      host,
      port: Number(port),
      slots: this.slotMapping[id],
    }
  }

  getAll(): DiscoveryNode[] {
    return Object.entries(this.transports).map<DiscoveryNode>(
      ([id, transport]) => {
        const [host, port] = transport.getAddress().split(':')
        return { host, port: Number(port), slots: this.slotMapping[id], id }
      },
    )
  }

  getBySlot(slot: number): DiscoveryNode {
    for (const node of this.getAll()) {
      for (const [min, max] of node.slots) {
        if (max >= slot && min <= slot) {
          return node
        }
      }
    }

    throw new Error(`unknown slot ${slot}`)
  }

  async init(params: { masters: number; slaves: number }) {
    this.commanderFactory = await createCustomClusterCommander(console, this)

    for (let i = 0; i < params.masters; i++) {
      const slotRange: SlotRange = [
        Math.round((slots * i) / params.masters),
        Math.round((slots * (i + 1)) / params.masters) - 1,
      ]

      const id = this.createMasterId(i)
      this.transports[id] = new Resp2Transport(
        this.logger,
        (() => this.commanderFactory!.createCommander(id)).bind(this),
      )
      this.slotMapping[id] = [slotRange]

      for (let j = 0; j < params.slaves; j++) {
        const replicaId = this.createReplicaId(id, j)
        this.transports[replicaId] = new Resp2Transport(
          this.logger,
          (() =>
            this.commanderFactory!.createReadOnlyCommander(replicaId)).bind(
            this,
          ),
        )
        this.slotMapping[replicaId] = [slotRange]
      }
    }

    await Promise.all(Object.values(this.transports).map(t => t.listen()))
  }

  async shutdown() {
    try {
      await Promise.all(Object.values(this.transports).map(t => t.close()))
      await this.commanderFactory?.shutdown()
    } catch (err) {
      this.logger.error('Error shutting down cluster network', err)
    }
  }
}
