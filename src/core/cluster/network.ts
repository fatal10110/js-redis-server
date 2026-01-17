import { createCustomClusterCommander } from '../../commanders/custom/clusterCommander'
import {
  ClusterCommanderFactory,
  DiscoveryNode,
  DiscoveryService,
  Logger,
  SlotRange,
} from '../../types'
import { Resp2Transport } from '../transports/resp2'

const slots = 16384

export function computeSlotRange(index: number, masters: number): SlotRange {
  if (!Number.isInteger(masters) || masters < 1) {
    throw new Error(`Invalid masters count ${masters}`)
  }
  if (!Number.isInteger(index) || index < 0 || index >= masters) {
    throw new Error(`Invalid master index ${index}`)
  }

  const start = Math.floor((slots * index) / masters)
  const end = Math.floor((slots * (index + 1)) / masters) - 1
  return [start, end]
}

export class ClusterNetwork implements DiscoveryService {
  private readonly slotMapping: Record<string, SlotRange[]> = {}
  private readonly transports: Record<string, Resp2Transport> = {}
  private commanderFactory?: ClusterCommanderFactory

  constructor(private readonly logger: Logger) {}

  private resolveAddress(id: string): { host: string; port: number } {
    const transport = this.transports[id]
    if (!transport) {
      throw new Error(`Transport not found for ${id}`)
    }

    const address = transport.server.address()
    if (!address || typeof address === 'string') {
      throw new Error(`Transport address not ready for ${id}`)
    }

    return { host: '127.0.0.1', port: address.port }
  }

  private createMasterId(index: number) {
    return `master-${index}`
  }

  private createReplicaId(masterId: string, index: number) {
    return `replica-${index}-${masterId}`
  }

  getMaster(id: string): DiscoveryNode {
    const masterIndex = Number(id.split('-').at(-1))
    if (!Number.isInteger(masterIndex) || masterIndex < 0) {
      throw new Error(`Invalid node id ${id}`)
    }

    return this.getById(this.createMasterId(masterIndex))
  }

  isMaster(id: string): boolean {
    return id.startsWith('master-')
  }

  getById(id: string): DiscoveryNode {
    if (!this.slotMapping[id]) {
      throw new Error(`Slot mapping not found for ${id}`)
    }
    const { host, port } = this.resolveAddress(id)

    return {
      id,
      host,
      port,
      slots: this.slotMapping[id],
    }
  }

  getAll(): DiscoveryNode[] {
    return Object.entries(this.transports).map<DiscoveryNode>(([id]) => {
      const { host, port } = this.resolveAddress(id)
      return { host, port, slots: this.slotMapping[id], id }
    })
  }

  getBySlot(slot: number): DiscoveryNode {
    for (const node of this.getAll()) {
      const nodeSlots = node.slots ?? []
      for (const [min, max] of nodeSlots) {
        if (max >= slot && min <= slot) {
          return node
        }
      }
    }

    throw new Error(`unknown slot ${slot}`)
  }

  async init(params: { masters: number; slaves: number; basePort?: number }) {
    if (!Number.isInteger(params.masters) || params.masters < 1) {
      throw new Error(`Invalid masters count ${params.masters}`)
    }
    if (!Number.isInteger(params.slaves) || params.slaves < 0) {
      throw new Error(`Invalid slaves count ${params.slaves}`)
    }
    if (
      params.basePort !== undefined &&
      (!Number.isInteger(params.basePort) ||
        params.basePort < 0 ||
        params.basePort > 65535)
    ) {
      throw new Error(`Invalid basePort ${params.basePort}`)
    }

    this.commanderFactory = await createCustomClusterCommander(
      this.logger,
      this,
    )
    const listenPorts: Record<string, number> = {}
    let portOffset = 0

    for (let i = 0; i < params.masters; i++) {
      const slotRange = computeSlotRange(i, params.masters)

      const id = this.createMasterId(i)
      this.transports[id] = new Resp2Transport(
        this.logger,
        // TODO
        this.commanderFactory!.createCommander(id),
      )
      this.slotMapping[id] = [slotRange]
      if (params.basePort !== undefined) {
        listenPorts[id] = params.basePort + portOffset
        portOffset += 1
      }

      for (let j = 0; j < params.slaves; j++) {
        const replicaId = this.createReplicaId(id, j)
        this.transports[replicaId] = new Resp2Transport(
          this.logger,
          // TODO
          this.commanderFactory!.createReadOnlyCommander(replicaId),
        )
        this.slotMapping[replicaId] = [slotRange]
        if (params.basePort !== undefined) {
          listenPorts[replicaId] = params.basePort + portOffset
          portOffset += 1
        }
      }
    }

    await Promise.all(
      Object.entries(this.transports).map(([id, t]) =>
        t.listen(listenPorts[id]),
      ),
    )
  }

  async shutdown() {
    await Promise.all(Object.values(this.transports).map(t => t.close()))
    await this.commanderFactory?.shutdown()
  }
}
