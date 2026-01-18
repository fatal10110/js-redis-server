import { Transport } from '../../types'
import {
  NestedMulti,
  TransactionDiscardedWithError,
  UserFacedError,
  WatchInsideMulti,
} from '../errors'
import { CommandRequest } from '../../commanders/custom/redis-kernel'
import {
  SessionState,
  SessionStateTransition,
  CommandValidator,
  SlotValidator,
} from './session-types'

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

      // Check if watched keys were modified or transaction should be discarded
      if (this.watchedKeysModified || this.shouldDiscard) {
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
