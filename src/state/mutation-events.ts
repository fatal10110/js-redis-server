import { cloneRedisDataValue, type RedisDataValue } from './data-types'

export type RedisMutationEvent =
  | {
      type: 'write'
      database: number
      key: Buffer
      value: RedisDataValue
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

  emit(event: RedisMutationEvent): void {
    for (const listener of Array.from(this.globalListeners)) {
      listener(cloneMutationEvent(event))
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
      return {
        ...event,
        key: Buffer.from(event.key),
      }
    case 'flush':
      return event
  }
}
