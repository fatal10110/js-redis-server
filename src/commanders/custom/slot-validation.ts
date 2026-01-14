import { CorssSlot, MovedError } from '../../core/errors'
import { Command, DiscoveryNode, DiscoveryService } from '../../types'
import clusterKeySlot from 'cluster-key-slot'
import { ClusterRouter, createClusterState } from './cluster-router'

export interface Validator {
  validate(cmd: Command, rawCmd: Buffer, args: Buffer[]): void
}

/**
 * SlotValidator implements the Validator interface using ClusterRouter.
 * This provides backward compatibility while leveraging the new schema-driven routing.
 */
export class SlotValidator implements Validator {
  private readonly router: ClusterRouter

  constructor(discoveryService: DiscoveryService, myself: DiscoveryNode) {
    const clusterState = createClusterState(discoveryService, myself)
    // Note: We pass an empty registry since we use the Command directly
    // The router will use the command's getKeys method
    this.router = new ClusterRouter(
      null as any, // Registry not needed when using validateSlot with Command directly
      clusterState,
    )
  }

  validate(cmd: Command, rawCmd: Buffer, args: Buffer[]): void {
    // Use the router's validateSlot method
    this.router.validateSlot(cmd, rawCmd, args)
  }

  /**
   * Validate a command against a specific slot constraint.
   * Used by ClusterTransactionState to ensure all commands in a transaction
   * hash to the same slot.
   *
   * @param cmd The command to validate
   * @param rawCmd The raw command buffer
   * @param args The command arguments
   * @param requiredSlot The slot that all commands must hash to
   * @returns The computed slot, or null if the command has no keys
   */
  validateSlot(
    cmd: Command,
    rawCmd: Buffer,
    args: Buffer[],
    requiredSlot?: number,
  ): number | null {
    return this.router.validateSlot(cmd, rawCmd, args, requiredSlot)
  }

  /**
   * Get the underlying router for direct access.
   */
  getRouter(): ClusterRouter {
    return this.router
  }
}
