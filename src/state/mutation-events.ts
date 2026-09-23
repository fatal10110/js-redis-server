import { cloneRedisDataValue, type RedisDataValue } from './data-types'

/**
 * A keyspace mutation. Every kind except `notify` is a *modified-key* signal
 * (real Redis' `signalModifiedKey`): it dirties a WATCH, can wake a blocked
 * client, and is replicated. `notify` is the keyspace-notification signal on
 * its own (`notifyKeyspaceEvent` without `signalModifiedKey`) — see
 * {@link RedisMutationBus.emit} for how the two are routed.
 *
 * `command` is the name of the command the event was emitted on behalf of
 * (see `RedisDatabase.withOrigin`); keyspace notifications name write events
 * after it. Absent for a mutation made through the database itself rather
 * than a command's handle — active expiry, replication, and (for now) MOVE /
 * COPY ... DB writes into another database.
 */
export type RedisMutationEvent = (
  | {
      type: 'write'
      database: number
      key: Buffer
      value: RedisDataValue
      expiresAt?: number
    }
  | {
      type: 'delete'
      database: number
      key: Buffer
    }
  | {
      type: 'expire'
      database: number
      key: Buffer
      expiresAt: number
    }
  | {
      type: 'persist'
      database: number
      key: Buffer
    }
  | {
      type: 'evict'
      database: number
      key: Buffer
    }
  | {
      type: 'flush'
      database: number
    }
  | {
      // An existing key changed in a way real Redis announces but does not
      // treat as modifying it: stream consumer-group / last-id metadata, and
      // the removal (hdel, lpop, ...) that empties a collection, which the
      // `delete` right after it signals instead.
      type: 'notify'
      database: number
      key: Buffer
      valueType: RedisDataValue['type']
    }
) & { command?: string }

export type RedisMutationListener = (event: RedisMutationEvent) => void

export type Unsubscribe = () => void

export class RedisMutationBus {
  private readonly globalListeners = new Set<RedisMutationListener>()
  private readonly keyListeners = new Map<string, Set<RedisMutationListener>>()

  subscribe(listener: RedisMutationListener): Unsubscribe {
    this.globalListeners.add(listener)
    return () => {
      this.globalListeners.delete(listener)
    }
  }

  subscribeKey(key: Buffer, listener: RedisMutationListener): Unsubscribe {
    const id = keyId(key)
    let listeners = this.keyListeners.get(id)
    if (!listeners) {
      listeners = new Set()
      this.keyListeners.set(id, listeners)
    }

    listeners.add(listener)
    return () => {
      listeners.delete(listener)
      if (listeners.size === 0) {
        this.keyListeners.delete(id)
      }
    }
  }

  /**
   * Fan `event` out. Global listeners (keyspace notifications, replication)
   * see every event. Per-key listeners — WATCH and blocked clients — see only
   * modified-key signals, never `notify`.
   */
  emit(event: RedisMutationEvent): void {
    for (const listener of Array.from(this.globalListeners)) {
      listener(cloneMutationEvent(event))
    }

    if (event.type === 'notify') {
      return
    }

    if (event.type === 'flush') {
      for (const listeners of Array.from(this.keyListeners.values())) {
        for (const listener of Array.from(listeners)) {
          listener(cloneMutationEvent(event))
        }
      }
      return
    }

    const listeners = this.keyListeners.get(keyId(event.key))
    if (!listeners) {
      return
    }

    for (const listener of Array.from(listeners)) {
      listener(cloneMutationEvent(event))
    }
  }
}

function keyId(key: Buffer): string {
  return key.toString('hex')
}

function cloneMutationEvent(event: RedisMutationEvent): RedisMutationEvent {
  switch (event.type) {
    case 'write':
      return {
        ...event,
        key: Buffer.from(event.key),
        value: cloneRedisDataValue(event.value),
      }
    case 'delete':
    case 'expire':
    case 'persist':
    case 'evict':
    case 'notify':
      return {
        ...event,
        key: Buffer.from(event.key),
      }
    case 'flush':
      return event
  }
}
