import { Transport } from '../../types'
import {
  NestedMulti,
  TransactionDiscardedWithError,
  UserFacedError,
} from '../errors'
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

/**
 * NormalState: Commands are executed immediately.
 * MULTI transitions to TransactionState.
 *
 * Supports optional slot validation for cluster mode.
 */
export class NormalState implements SessionState {
  constructor(
    private readonly commandValidator: CommandValidator,
    private readonly slotValidator?: SlotValidator,
  ) {}

  handle(
    transport: Transport,
    command: Buffer,
    args: Buffer[],
  ): SessionStateTransition {
    const cmdName = command.toString().toLowerCase()

    if (cmdName === 'multi') {
      transport.write('OK')
      transport.flush()
      return {
        nextState: new TransactionState(
          this,
          this.commandValidator,
          this.slotValidator,
        ),
      }
    }

    // Pass through to kernel for immediate execution
    return {
      nextState: this,
      executeCommand: {
        command,
        args,
        transport,
        signal: new AbortController().signal,
      },
    }
  }
}

/**
 * TransactionState: Commands are buffered until EXEC or DISCARD.
 * EXEC executes the entire buffer atomically.
 * DISCARD returns to NormalState without executing.
 *
 * In cluster mode (when slotValidator is provided), implements slot pinning:
 * 1. The first command with keys "pins" the transaction to a specific slot.
 * 2. Every subsequent command must hash to the same slot.
 */
export class TransactionState implements SessionState {
  private readonly buffer: CommandRequest[] = []
  private shouldDiscard = false
  private pinnedSlot: number | undefined

  constructor(
    private readonly normalState: SessionState,
    private readonly validator: CommandValidator,
    private readonly slotValidator?: SlotValidator,
  ) {}

  handle(
    transport: Transport,
    command: Buffer,
    args: Buffer[],
  ): SessionStateTransition {
    const cmdName = command.toString().toLowerCase()

    if (cmdName === 'exec') {
      if (this.shouldDiscard) {
        transport.write(new TransactionDiscardedWithError())
        transport.flush()
        return {
          nextState: this.normalState,
        }
      }

      // Execute the entire buffer as a single atomic batch
      return {
        nextState: this.normalState,
        executeBatch: this.buffer,
      }
    }

    if (cmdName === 'discard') {
      transport.write('OK')
      transport.flush()
      return {
        nextState: this.normalState,
      }
    }

    if (cmdName === 'multi') {
      transport.write(new NestedMulti())
      transport.flush()
      return {
        nextState: this,
      }
    }

    // Validate the command (arity, syntax)
    try {
      this.validator.validate(cmdName, args)
    } catch (err) {
      if (err instanceof UserFacedError) {
        transport.write(err)
        transport.flush()
        this.shouldDiscard = true
        return {
          nextState: this,
        }
      }
      throw err
    }

    // Validate slot constraints in cluster mode
    if (this.slotValidator) {
      try {
        const slot = this.slotValidator.validateSlot(
          cmdName,
          args,
          this.pinnedSlot,
        )

        // Pin the slot on first command with keys
        if (this.pinnedSlot === undefined && slot !== null) {
          this.pinnedSlot = slot
        }
      } catch (err) {
        if (err instanceof UserFacedError) {
          transport.write(err)
          transport.flush()
          this.shouldDiscard = true
          return {
            nextState: this,
          }
        }
        throw err
      }
    }

    // Buffer the command
    this.buffer.push({
      command,
      args,
      transport,
      signal: new AbortController().signal,
    })

    transport.write('QUEUED')
    transport.flush()

    return {
      nextState: this,
    }
  }
}
