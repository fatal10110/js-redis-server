import { Transport } from '../../types'
import { UnknownCommand, UserFacedError } from '../errors'
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
 * NormalState: Commands are executed immediately.
 * MULTI transitions to TransactionState.
 */
export class NormalState implements SessionState {
  constructor(private readonly transactionCommandValidator: CommandValidator) {}

  handle(
    transport: Transport,
    command: Buffer,
    args: Buffer[],
  ): SessionStateTransition {
    const cmdName = command.toString().toLowerCase()

    if (cmdName === 'multi') {
      transport.write('OK')
      return {
        nextState: new TransactionState(this, this.transactionCommandValidator),
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
 */
export class TransactionState implements SessionState {
  private readonly buffer: CommandRequest[] = []
  private shouldDiscard = false

  constructor(
    private readonly normalState: SessionState,
    private readonly validator: CommandValidator,
  ) {}

  handle(
    transport: Transport,
    command: Buffer,
    args: Buffer[],
  ): SessionStateTransition {
    const cmdName = command.toString().toLowerCase()

    if (cmdName === 'exec') {
      if (this.shouldDiscard) {
        transport.write(
          new Error(
            'EXECABORT Transaction discarded because of previous errors.',
          ),
        )
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
      return {
        nextState: this.normalState,
      }
    }

    if (cmdName === 'multi') {
      transport.write(new Error('ERR MULTI calls can not be nested'))
      return {
        nextState: this,
      }
    }

    // Validate the command
    try {
      this.validator.validate(cmdName, args)
    } catch (err) {
      if (err instanceof UserFacedError) {
        transport.write(err)
        this.shouldDiscard = true
        return {
          nextState: this,
        }
      }
      throw err
    }

    // Buffer the command
    this.buffer.push({
      command,
      args,
      transport,
      signal: new AbortController().signal,
    })

    transport.write('QUEUED')

    return {
      nextState: this,
    }
  }
}
