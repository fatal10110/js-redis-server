import { Transport, Command } from '../../types'
import { UserFacedError } from '../errors'
import { CommandRequest } from '../../commanders/custom/redis-kernel'
import { SlotValidator } from '../../commanders/custom/slot-validation'
import {
  SessionState,
  SessionStateTransition,
  CommandValidator,
} from './session-state'

/**
 * ClusterCommandValidator extends CommandValidator to also validate slot constraints.
 * Used by ClusterTransactionState to ensure all commands in a transaction
 * hash to the same slot.
 */
export class ClusterCommandValidator implements CommandValidator {
  constructor(
    private readonly baseValidator: CommandValidator,
    private readonly commands: Record<string, Command>,
    private readonly slotValidator: SlotValidator,
  ) {}

  validate(command: string, args: Buffer[]): void {
    // First, run base validation (arity, syntax, etc.)
    this.baseValidator.validate(command, args)
  }

  /**
   * Validate the command's slot against a pinned slot.
   * Returns the computed slot, or null if the command has no keys.
   */
  validateSlot(
    command: string,
    args: Buffer[],
    pinnedSlot?: number,
  ): number | null {
    const cmd = this.commands[command.toLowerCase()]
    if (!cmd) {
      return null
    }

    return this.slotValidator.validateSlot(
      cmd,
      Buffer.from(command),
      args,
      pinnedSlot,
    )
  }
}

/**
 * ClusterNormalState: Normal command execution in cluster mode.
 * MULTI transitions to ClusterTransactionState.
 */
export class ClusterNormalState implements SessionState {
  constructor(
    private readonly baseValidator: CommandValidator,
    private readonly clusterValidator: ClusterCommandValidator,
  ) {}

  handle(
    transport: Transport,
    command: Buffer,
    args: Buffer[],
  ): SessionStateTransition {
    const cmdName = command.toString().toLowerCase()

    if (cmdName === 'multi') {
      transport.write('OK')
      return {
        nextState: new ClusterTransactionState(
          this,
          this.baseValidator,
          this.clusterValidator,
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
 * ClusterTransactionState: Handles MULTI/EXEC with slot pinning.
 *
 * In Redis Cluster, all keys in a transaction must hash to the same slot.
 * This state implements "slot pinning":
 * 1. The first command with keys "pins" the transaction to a specific slot.
 * 2. Every subsequent command must hash to the same slot.
 *
 * This ensures:
 * - All commands can be executed atomically on a single node
 * - No cross-slot errors during EXEC
 */
export class ClusterTransactionState implements SessionState {
  private readonly buffer: CommandRequest[] = []
  private shouldDiscard = false
  private pinnedSlot: number | undefined

  constructor(
    private readonly normalState: SessionState,
    private readonly baseValidator: CommandValidator,
    private readonly clusterValidator: ClusterCommandValidator,
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

    // Validate the command using base validator (arity, syntax)
    try {
      this.baseValidator.validate(cmdName, args)
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

    // Validate slot constraints in cluster mode
    try {
      const slot = this.clusterValidator.validateSlot(
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
