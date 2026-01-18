import crypto from 'node:crypto'
import { EventEmitter } from 'node:events'
import { HashDataType } from './data-structures/hash'
import { ListDataType } from './data-structures/list'
import { SetDataType } from './data-structures/set'
import { StreamDataType } from './data-structures/stream'
import { StringDataType } from './data-structures/string'
import { SortedSetDataType } from './data-structures/zset'
import { ReactiveDB } from '../../core/transports/reactive-db'

export type DataTypes =
  | HashDataType
  | ListDataType
  | SetDataType
  | StreamDataType
  | StringDataType
  | SortedSetDataType

/**
 * Store event types emitted by the reactive data store
 */
export type StoreEvent =
  | { type: 'set'; key: Buffer; value: DataTypes }
  | { type: 'del'; key: Buffer }
  | { type: 'expire'; key: Buffer; expiration: number }
  | { type: 'evict'; key: Buffer }
  | { type: 'flush' }

/**
 * Event names emitted by the store:
 * - 'change': Emitted on every mutation with StoreEvent payload
 * - 'key:<keyString>': Emitted for key-specific changes (for WATCH)
 */
export interface StoreEvents {
  change: [event: StoreEvent]
  [key: `key:${string}`]: [event: StoreEvent]
}

export class DB extends EventEmitter<StoreEvents> implements ReactiveDB {
  // TODO solve this, storing more than twice the size of the key
  private readonly mapping = new Map<string, Buffer>()
  private readonly timings = new Map<Buffer, number>()
  private readonly data = new Map<Buffer, DataTypes>()
  private readonly scriptsStore = new Map<string, Buffer>()

  constructor() {
    super()
  }

  tryEvict(key: Buffer) {
    const now = Date.now()
    const timing = this.timings.get(key)

    if (timing && timing <= now) {
      this.timings.delete(key)
      this.data.delete(key)
      this.mapping.delete(key.toString('hex'))

      const event: StoreEvent = { type: 'evict', key }
      this.emit('change', event)
      this.emit(`key:${key.toString('hex')}`, event)

      return true
    }

    return false
  }

  get(rawKey: Buffer) {
    const key = this.mapping.get(rawKey.toString('hex'))

    if (!key) {
      return null
    }

    this.tryEvict(key)

    return this.data.get(key) || null
  }

  set(rawKey: Buffer, val: DataTypes, expiration?: number) {
    const stringifiedKey = rawKey.toString('hex')
    let key = this.mapping.get(stringifiedKey)

    if (!key) {
      key = rawKey
      this.mapping.set(stringifiedKey, rawKey)
    }

    this.tryEvict(key)

    this.data.set(key, val)

    if (expiration) {
      this.timings.set(key, expiration)
    }

    const event: StoreEvent = { type: 'set', key, value: val }
    this.emit('change', event)
    this.emit(`key:${stringifiedKey}`, event)
  }

  del(rawKey: Buffer) {
    const stringifiedKey = rawKey.toString('hex')
    const existingRef = this.mapping.get(stringifiedKey)

    if (!existingRef) {
      return false
    }

    if (this.tryEvict(existingRef)) {
      return false
    }

    this.timings.delete(existingRef)
    this.data.delete(existingRef)
    this.mapping.delete(stringifiedKey)

    const event: StoreEvent = { type: 'del', key: existingRef }
    this.emit('change', event)
    this.emit(`key:${stringifiedKey}`, event)

    return true
  }

  addScript(script: Buffer): string {
    const sha = crypto.createHash('sha1').update(script).digest('hex')
    this.scriptsStore.set(sha, script)
    return sha
  }

  getScript(sha: string) {
    return this.scriptsStore.get(sha)
  }

  flushScripts() {
    return this.scriptsStore.clear()
  }

  getTtl(rawKey: Buffer): number {
    const stringifiedKey = rawKey.toString('hex')
    const key = this.mapping.get(stringifiedKey)

    if (!key || this.tryEvict(key)) {
      return -2 // Key does not exist
    }

    const timing = this.timings.get(key)
    if (!timing) {
      return -1 // Key exists but has no expiration
    }

    return timing
  }

  setExpiration(rawKey: Buffer, expiration: number): boolean {
    const stringifiedKey = rawKey.toString('hex')
    const key = this.mapping.get(stringifiedKey)

    if (!key) {
      return false // Key does not exist
    }

    // Check if key still exists (hasn't expired)
    const existing = this.get(rawKey)
    if (existing === null) {
      return false // Key has expired or doesn't exist
    }

    this.timings.set(key, expiration)

    const event: StoreEvent = { type: 'expire', key, expiration }
    this.emit('change', event)
    this.emit(`key:${stringifiedKey}`, event)

    return true // Successfully set expiration
  }

  /**
   * Remove expiration from a key
   */
  persist(rawKey: Buffer): boolean {
    const stringifiedKey = rawKey.toString('hex')
    const key = this.mapping.get(stringifiedKey)

    if (!key) {
      return false // Key does not exist
    }

    // Check if key still exists (hasn't expired)
    const existing = this.get(rawKey)
    if (existing === null) {
      return false // Key has expired or doesn't exist
    }

    const hadExpiration = this.timings.has(key)
    if (hadExpiration) {
      this.timings.delete(key)
    }

    return hadExpiration // Return true only if expiration was removed
  }

  /**
   * Remove all keys from the current database
   */
  flushdb(): void {
    this.mapping.clear()
    this.timings.clear()
    this.data.clear()
    this.flushScripts()

    const event: StoreEvent = { type: 'flush' }
    this.emit('change', event)
  }

  /**
   * Remove all keys from all databases (same as flushdb in single-database implementation)
   */
  flushall(): void {
    this.flushdb()
  }

  /**
   * Get the number of keys in the current database
   * This excludes expired keys
   */
  size(): number {
    let count = 0

    for (const [, keyBuffer] of this.mapping) {
      if (!this.tryEvict(keyBuffer)) {
        count++
      }
    }

    return count
  }
}
