import { Transport } from '../../types'
import { WrongNumberOfArguments } from '../errors'
import {
  SessionState,
  SessionStateTransition,
  CommandValidator,
  SlotValidator,
} from './session-types'
import { ReactiveDB, ReactiveStoreEvent } from './reactive-db'
import { TransactionState } from './transaction-state'

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
  private listeners: Map<string, (event: ReactiveStoreEvent) => void> =
    new Map()

  constructor(
    private readonly commandValidator: CommandValidator,
    private readonly db: ReactiveDB,
    private readonly slotValidator?: SlotValidator,
  ) {}

  watch(keys: Buffer[]): void {
    for (const key of keys) {
      const keyStr = key.toString('hex')
      if (this.watchedKeys.has(keyStr)) continue

      this.watchedKeys.add(keyStr)

      // Listen for modifications to this key
      const eventName = `key:${keyStr}` as const
      const listener = (event: ReactiveStoreEvent) => {
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
