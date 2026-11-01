import { cloneRedisDataValue, type RedisDataValue } from './data-types'

/**
 * A keyspace mutation. Every kind except `notify`, `new`, `keymiss` and
 * `propagate` is a *modified-key* signal (real Redis' `signalModifiedKey`): it
 * dirties a WATCH, can wake a blocked client, and is replicated. `notify` is
 * the keyspace-notification signal on its own (`notifyKeyspaceEvent` without
 * `signalModifiedKey`), and `propagate` the replication signal on its own
 * (`alsoPropagate`) — see {@link RedisMutationBus.emit} for how they are
 * routed.
 *
 * `command` is the name of the command the event was emitted on behalf of
 * (see `RedisDatabase.withOrigin`); keyspace notifications name write events
 * after it. Absent for a mutation made through the database itself rather
 * than a command's handle — active expiry and replication. A command that
 * writes into a database other than its own (MOVE, COPY ... DB) tags that one
 * itself with `withOrigin`.
 *
 * `client` is the connection whose command made the change (the caller of a
 * script, the client running EXEC), compared by identity only: client-side
 * caching skips a NOLOOP client's own writes and holds its own invalidations
 * until its reply. Absent wherever `command` is, and for FLUSHALL.
 *
 * `viaScript` marks a change made by a script's `redis.call`: real Redis
 * makes it on the script's own client, so a BCAST NOLOOP caller still hears
 * of it (redis@7.2.4:src/tracking.c `trackingRememberKeyToBroadcast`).
 */
export type RedisMutationEvent = (
  | {
      type: 'write'
      database: number
      key: Buffer
      // Cloned lazily, on first read — see cloneMutationEvent. A listener
      // that keeps the event past the emit (delayed replication) must read it
      // synchronously to snapshot the value as of this write.
      value: RedisDataValue
      valueType: RedisDataValue['type']
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
  | {
      // Replication-only: an existing key changed in place in a way real
      // Redis propagates to its replicas but does not treat as modifying it,
      // and may not announce at all — stream consumer-group / pending /
      // last-id metadata (XACK, XREADGROUP, XCLAIM, ... propagate their
      // effect; XGROUP and XSETID announce it too, in a `notify` emitted
      // before this). Carries the whole new value, cloned lazily like a
      // `write`'s, for the replica to store. Emits no keyspace notification.
      type: 'propagate'
      database: number
      key: Buffer
      value: RedisDataValue
      valueType: RedisDataValue['type']
      expiresAt?: number
    }
  | {
      // Notification-only, like `notify`: a key was added to the database
      // (real Redis' `dbAdd`, which publishes `new`). Emitted before the
      // write that stores it, so `new` precedes the command's own event.
      type: 'new'
      database: number
      key: Buffer
    }
  | {
      // Notification-only: a read lookup of a key that does not exist (real
      // Redis' `lookupKeyRead` miss, which publishes `keymiss`).
      type: 'keymiss'
      database: number
      key: Buffer
    }
) & { command?: string; client?: object; viaScript?: boolean }

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
   * modified-key signals, never the notification-only `notify`, `new` and
   * `keymiss`, nor the replication-only `propagate`.
   */
  emit(event: RedisMutationEvent): void {
    for (const listener of Array.from(this.globalListeners)) {
      listener(cloneMutationEvent(event))
    }

    if (
      event.type === 'notify' ||
      event.type === 'propagate' ||
      event.type === 'new' ||
      event.type === 'keymiss'
    ) {
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
    case 'propagate': {
      // Most listeners (WATCH, blocked clients, keyspace notifications) never
      // read the value, and cloning a whole collection for every listener on
      // every write made filling one hash element by element quadratic. So
      // the copy is made on first read, once per listener's event.
      const source = event.value
      let copy: RedisDataValue | undefined
      const cloned = { ...event, key: Buffer.from(event.key) }
      Object.defineProperty(cloned, 'value', {
        enumerable: true,
        configurable: true,
        get: () => (copy ??= cloneRedisDataValue(source)),
      })
      return cloned
    }
    case 'delete':
    case 'expire':
    case 'persist':
    case 'evict':
    case 'notify':
    case 'new':
    case 'keymiss':
      return {
        ...event,
        key: Buffer.from(event.key),
      }
    case 'flush':
      return event
  }
}
