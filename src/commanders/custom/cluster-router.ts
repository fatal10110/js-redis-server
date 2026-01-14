import { CorssSlot, MovedError } from '../../core/errors'
import { Command, DiscoveryService } from '../../types'
import { SlotValidator } from '../../core/transports/session-state'
import clusterKeySlot from 'cluster-key-slot'

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
export class ClusterRouter implements SlotValidator {
  constructor(
    private readonly discoveryService: DiscoveryService,
    private readonly mySelfId: string,
    private readonly commands: Record<string, Command>,
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
   * Validate a command's slot by name (implements SlotValidator interface).
   * Looks up the command and delegates to validateCommandSlot.
   *
   * @param commandName The command name (case-insensitive)
   * @param args The command arguments
   * @param pinnedSlot Optional slot constraint (for transactions)
   * @returns The slot number, or null if the command has no keys
   * @throws CorssSlot if keys hash to different slots
   * @throws MovedError if the slot is not owned by this node
   */
  validateSlot(
    commandName: string,
    args: Buffer[],
    pinnedSlot?: number,
  ): number | null {
    const command = this.commands[commandName.toLowerCase()]
    if (!command) {
      return null
    }
    return this.validateCommandSlot(
      command,
      Buffer.from(commandName),
      args,
      pinnedSlot,
    )
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
  validateCommandSlot(
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
    if (!this.isLocalSlot(slot)) {
      const owner = this.discoveryService.getBySlot(slot)
      throw new MovedError(owner.host, owner.port, slot)
    }

    return slot
  }
}
