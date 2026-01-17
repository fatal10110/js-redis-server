import { CorssSlot } from '../../core/errors'
import { Command, SlotValidator } from '../../types'
import clusterKeySlot from 'cluster-key-slot'

/**
 * ClusterSlotValidator encapsulates schema-driven slot validation for Redis Cluster.
 * It is shared by routing and execution paths (including Lua) to enforce slot rules.
 */
export class ClusterSlotValidator implements SlotValidator {
  constructor(private readonly commands: Record<string, Command>) {}

  /**
   * Extract keys from a command using its metadata or custom getKeys function.
   */
  private extractKeys(
    command: Command,
    rawCmd: Buffer,
    args: Buffer[],
  ): Buffer[] {
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

    return slot
  }
}
