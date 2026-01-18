/**
 * Event types emitted by the reactive data store.
 * These events are used for implementing WATCH functionality.
 */
export type ReactiveStoreEvent =
  | { type: 'set'; key: Buffer }
  | { type: 'del'; key: Buffer }
  | { type: 'expire'; key: Buffer }
  | { type: 'evict'; key: Buffer }
  | { type: 'flush' }

/**
 * Interface for a reactive data store that emits events on mutations.
 * This enables optimistic locking via WATCH/MULTI/EXEC patterns.
 *
 * The DB implementation should emit events on:
 * - 'change': Global event for any mutation
 * - 'key:<keyString>': Key-specific events for targeted watching
 */
export interface ReactiveDB {
  /**
   * Register a listener for an event.
   * @param event - Event name ('change' or 'key:<keyString>')
   * @param listener - Callback function invoked when the event occurs
   */
  on(event: string, listener: (event: ReactiveStoreEvent) => void): void

  /**
   * Register a one-time listener for an event.
   * The listener is automatically removed after being invoked once.
   * @param event - Event name ('change' or 'key:<keyString>')
   * @param listener - Callback function invoked when the event occurs
   */
  once(event: string, listener: (event: ReactiveStoreEvent) => void): void

  /**
   * Remove a listener for an event.
   * @param event - Event name ('change' or 'key:<keyString>')
   * @param listener - The listener function to remove
   */
  off(event: string, listener: (event: ReactiveStoreEvent) => void): void
}
