import type { Unsubscribe } from './mutation-events'

/**
 * Real Redis' `signalKeyAsReady` on its own: wakes the clients blocked on a key
 * without the key counting as modified. It is not a mutation event, so it
 * never dirties a WATCH or fires a keyspace notification.
 *
 * `XGROUP DESTROY` sends it so a blocked `XREADGROUP` on that group re-runs
 * and replies NOGROUP. Mutations wake blocked clients through the
 * {@link RedisMutationBus} instead.
 */
export class KeyReadySignals {
  private readonly listeners = new Map<string, Set<() => void>>()

  subscribe(key: Buffer, listener: () => void): Unsubscribe {
    const id = key.toString('hex')
    let listeners = this.listeners.get(id)
    if (!listeners) {
      listeners = new Set()
      this.listeners.set(id, listeners)
    }

    listeners.add(listener)
    return () => {
      listeners.delete(listener)
      if (listeners.size === 0) {
        this.listeners.delete(id)
      }
    }
  }

  signal(key: Buffer): void {
    const listeners = this.listeners.get(key.toString('hex'))
    if (!listeners) return
    for (const listener of Array.from(listeners)) listener()
  }
}
