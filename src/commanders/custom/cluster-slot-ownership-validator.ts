import { DiscoveryService, SlotOwnershipValidator } from '../../types'
import clusterKeySlot from 'cluster-key-slot'

/**
 * ClusterSlotOwnershipValidator resolves whether a slot is local for Redis Cluster.
 */
export class ClusterSlotOwnershipValidator implements SlotOwnershipValidator {
  constructor(
    private readonly discoveryService: DiscoveryService,
    private readonly mySelfId: string,
  ) {}

  private isLocalSlot(slot: number): boolean {
    const myself = this.discoveryService.getById(this.mySelfId)

    for (const [min, max] of myself.slots) {
      if (slot >= min && slot <= max) {
        return true
      }
    }
    return false
  }

  getLocalSlot(keys: Buffer[]): number | null {
    if (keys.length === 0) {
      return null
    }

    const slot = clusterKeySlot.generateMulti(keys)
    if (slot === -1) {
      return null
    }

    if (!this.isLocalSlot(slot)) {
      return null
    }

    return slot
  }
}
