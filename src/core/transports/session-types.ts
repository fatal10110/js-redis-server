import { Transport } from '../../types'
import { CommandRequest } from '../../commanders/custom/redis-kernel'

/**
 * SessionState represents the current mode of a client connection.
 * Implements the State pattern for handling MULTI/EXEC transactions.
 */
export interface SessionState {
  /**
   * Handle a command in the current state.
   * Returns the command request to execute, or null if the command was handled internally.
   */
  handle(
    transport: Transport,
    command: Buffer,
    args: Buffer[],
  ): SessionStateTransition

  /**
   * Watch keys for optimistic locking.
   * Keys are monitored for modifications until EXEC/DISCARD.
   */
  watch?(keys: Buffer[]): void

  /**
   * Unwatch all keys.
   */
  unwatch?(): void
}

/**
 * Result of handling a command in a session state.
 * Contains the next state and optionally a command to execute or a batch to execute.
 */
export type SessionStateTransition = {
  nextState: SessionState
  /** Single command to execute immediately */
  executeCommand?: CommandRequest
  /** Batch of commands to execute atomically (for EXEC) */
  executeBatch?: CommandRequest[]
}

/**
 * Interface for command validation.
 * Used by TransactionState to validate commands before buffering.
 */
export interface CommandValidator {
  validate(command: string, args: Buffer[]): void
}

/**
 * Interface for slot validation in cluster mode.
 * Optional - when provided, TransactionState will validate slot constraints.
 */
export interface SlotValidator {
  /**
   * Validate the command's slot against a pinned slot.
   * Returns the computed slot, or null if the command has no keys.
   * @throws CorssSlot if keys hash to different slots
   * @throws MovedError if the slot is not owned by this node
   */
  validateSlot(
    command: string,
    args: Buffer[],
    pinnedSlot?: number,
  ): number | null
}
