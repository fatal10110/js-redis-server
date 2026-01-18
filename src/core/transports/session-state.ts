import { Transport } from '../../types'
import {
  NestedMulti,
  TransactionDiscardedWithError,
  UserFacedError,
  WrongNumberOfArguments,
  WatchInsideMulti,
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

/**
 * NormalState: Commands are executed immediately.
 * MULTI transitions to TransactionState.
 *
 * Supports optional slot validation for cluster mode.
 * Supports WATCH for optimistic locking.
 */
export class NormalState implements SessionState {
  private watchedKeys: Set<string> = new Set()
  private watchedKeysModified = false
  private listeners: Map<string, (event: any) => void> = new Map()

  constructor(
    private readonly commandValidator: CommandValidator,
    private readonly slotValidator?: SlotValidator,
    private readonly db?: any, // DB instance for event listening
  ) {}

  watch(keys: Buffer[]): void {
    if (!this.db) return

    for (const key of keys) {
      const keyStr = key.toString('binary')
      if (this.watchedKeys.has(keyStr)) continue

      this.watchedKeys.add(keyStr)

      // Listen for modifications to this key
      const eventName = `key:${keyStr}` as const
      const listener = (event: any) => {
        // Mark as modified if the key was set or deleted
        if (
          event.type === 'set' ||
          event.type === 'del' ||
          event.type === 'expire'
        ) {
          this.watchedKeysModified = true
        }
      }

      this.listeners.set(keyStr, listener)
      this.db.on(eventName, listener)
    }
  }

  unwatch(): void {
    if (!this.db) return

    // Remove all listeners
    for (const [keyStr, listener] of this.listeners) {
      const eventName = `key:${keyStr}` as const
      this.db.off(eventName, listener)
    }

    this.watchedKeys.clear()
    this.listeners.clear()
    this.watchedKeysModified = false
  }

  handle(
    transport: Transport,
    command: Buffer,
    args: Buffer[],
  ): SessionStateTransition {
    const cmdName = command.toString().toLowerCase()

    if (cmdName === 'watch') {
      if (args.length === 0) {
        transport.write(new WrongNumberOfArguments('WATCH'))
        transport.flush()
        return { nextState: this }
      }

      this.watch(args)
      transport.write('OK')
      transport.flush()
      return { nextState: this }
    }

    if (cmdName === 'unwatch') {
      this.unwatch()
      transport.write('OK')
      transport.flush()
      return { nextState: this }
    }

    if (cmdName === 'multi') {
      transport.write('OK')
      transport.flush()
      return {
        nextState: new TransactionState(
          this,
          this.commandValidator,
          this.slotValidator,
          this.watchedKeysModified,
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
 *
 * Supports WATCH: If any watched keys were modified, EXEC will abort.
 */
export class TransactionState implements SessionState {
  private readonly buffer: CommandRequest[] = []
  private shouldDiscard = false
  private pinnedSlot: number | undefined

  constructor(
    private readonly normalState: SessionState,
    private readonly validator: CommandValidator,
    private readonly slotValidator?: SlotValidator,
    private readonly watchedKeysModified: boolean = false,
  ) {}

  handle(
    transport: Transport,
    command: Buffer,
    args: Buffer[],
  ): SessionStateTransition {
    const cmdName = command.toString().toLowerCase()

    if (cmdName === 'exec') {
      // Clean up watched keys
      if (this.normalState.unwatch) {
        this.normalState.unwatch()
      }

      // Check if watched keys were modified
      if (this.watchedKeysModified || this.shouldDiscard) {
        transport.write(null)
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
      // Clean up watched keys
      if (this.normalState.unwatch) {
        this.normalState.unwatch()
      }

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

    if (cmdName === 'watch') {
      transport.write(new WatchInsideMulti())
      transport.flush()
      this.shouldDiscard = true
      return {
        nextState: this,
      }
    }

    if (cmdName === 'unwatch') {
      // UNWATCH is allowed in MULTI but does nothing
      transport.write('QUEUED')
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
