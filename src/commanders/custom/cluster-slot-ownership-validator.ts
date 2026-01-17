import { MovedError } from '../../core/errors'
import { DiscoveryService, SlotOwnershipValidator } from '../../types'

/**
 * ClusterSlotOwnershipValidator enforces local slot ownership for Redis Cluster.
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

  validateSlotOwnership(slot: number): void {
    if (this.isLocalSlot(slot)) {
      return
    }

    const owner = this.discoveryService.getBySlot(slot)
    throw new MovedError(owner.host, owner.port, slot)
  }
}
