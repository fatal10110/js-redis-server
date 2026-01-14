import { CorssSlot, MovedError } from '../../core/errors'
import { Command, DiscoveryNode, DiscoveryService } from '../../types'
import clusterKeySlot from 'cluster-key-slot'

/**
 * ClusterState interface for topology management.
 * Wraps DiscoveryService to provide a cleaner API for the router.
 */
export interface ClusterState {
  /** Check if the given slot is owned by this node */
  isLocal(slot: number): boolean
  /** Get the owner node for a slot */
  getOwner(slot: number): { ip: string; port: number }
}

/**
 * Creates a ClusterState from DiscoveryService and the current node.
 */
export function createClusterState(
  discoveryService: DiscoveryService,
  myself: DiscoveryNode,
): ClusterState {
  return {
    isLocal(slot: number): boolean {
      for (const [min, max] of myself.slots) {
        if (slot >= min && slot <= max) {
          return true
        }
      }
      return false
    },

    getOwner(slot: number): { ip: string; port: number } {
      const node = discoveryService.getBySlot(slot)
      return { ip: node.host, port: node.port }
    },
  }
}

/**
 * SlotValidationResult contains slot information for a command.
 */
export interface SlotValidationResult {
  /** The computed slot number, or null if the command has no keys */
  slot: number | null
  /** The keys extracted from the command */
  keys: Buffer[]
}

/**
 * ClusterRouter implements schema-driven routing for Redis Cluster.
 *
 * This router sits between the Session and the Kernel, providing:
 * 1. Generic key extraction based on command metadata
 * 2. Automatic slot calculation
 * 3. Cross-slot validation
 * 4. MOVED error generation for topology mismatches
 *
 * Benefits:
 * - Zero boilerplate: New commands support Cluster automatically if their schema defines keys correctly
 * - Centralized logic: MOVED/ASK/CROSSSLOT logic exists in one place
 * - Performance: Non-cluster mode simply bypasses the slot check steps
 */
export class ClusterRouter {
  constructor(private readonly clusterState: ClusterState) {}

  /**
   * Extract keys from a command using its metadata or custom getKeys function.
   * This is the "schema-driven" part - we use the registry to understand commands.
   */
  extractKeys(command: Command, rawCmd: Buffer, args: Buffer[]): Buffer[] {
    return command.getKeys(rawCmd, args)
  }

  /**
   * Calculate the slot for a single key.
   */
  calculateSlot(key: Buffer): number {
    return clusterKeySlot(key)
  }

  /**
   * Validate a command's keys against cluster slot requirements.
   * Returns the slot if valid, null if the command has no keys.
   *
   * @param command The command to validate
   * @param rawCmd The raw command buffer
   * @param args The command arguments
   * @param requiredSlot Optional slot constraint (for transactions)
   * @throws CorssSlot if keys hash to different slots
   * @throws MovedError if the slot is not owned by this node
   */
  validateSlot(
    command: Command,
    rawCmd: Buffer,
    args: Buffer[],
    requiredSlot?: number,
  ): number | null {
    const keys = this.extractKeys(command, rawCmd, args)

    if (keys.length === 0) {
      return null
    }

    // Calculate slot for all keys
    const slot = clusterKeySlot.generateMulti(keys)

    // Cross-slot check: generateMulti returns -1 if keys hash to different slots
    if (slot === -1) {
      throw new CorssSlot()
    }

    // Check against pinned slot (for Transactions)
    if (requiredSlot !== undefined && slot !== requiredSlot) {
      throw new CorssSlot()
    }

    // Check topology: is this slot owned by our node?
    if (!this.clusterState.isLocal(slot)) {
      const owner = this.clusterState.getOwner(slot)
      throw new MovedError(owner.ip, owner.port, slot)
    }

    return slot
  }
}
