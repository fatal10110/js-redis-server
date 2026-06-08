import {
  cloneRedisDataValue,
  type RedisDataValue,
  type RedisDataTypeName,
} from './data-types'
import { RedisMutationBus } from './mutation-events'

export type ExpirationState =
  | { kind: 'missing' }
  | { kind: 'persistent' }
  | { kind: 'expires'; expiresAt: number }

export type KeyspaceEntry = {
  key: Buffer
  value: RedisDataValue
  expiresAt?: number
}

export type SetOptions = {
  expiresAt?: number
  keepTtl?: boolean
}

export class WrongRedisTypeError extends Error {
  constructor(
    public readonly expected: RedisDataTypeName,
    public readonly actual: RedisDataTypeName,
  ) {
    super(`Expected ${expected}, got ${actual}`)
    this.name = 'WrongRedisTypeError'
  }
}

export class RedisKeyspace {
  private readonly entries = new Map<string, KeyspaceEntry>()

  constructor(
    private readonly database: number,
    private readonly mutations: RedisMutationBus,
  ) {}

  get(key: Buffer): RedisDataValue | null {
    const entry = this.getLiveEntry(key)
    if (!entry) {
      return null
    }

    return cloneRedisDataValue(entry.value)
  }

  getType(key: Buffer): RedisDataTypeName | null {
    return this.getLiveEntry(key)?.value.type ?? null
  }

  set(key: Buffer, value: RedisDataValue, options?: SetOptions): void {
    const id = keyId(key)
    const existing = this.getLiveEntry(key)
    const expiresAt = options?.keepTtl
      ? existing?.expiresAt
      : options?.expiresAt
    const entry: KeyspaceEntry = {
      key: Buffer.from(key),
      value: cloneRedisDataValue(value),
      expiresAt,
    }

    this.entries.set(id, entry)
    this.emitWrite(entry)
  }

  delete(key: Buffer): boolean {
    const id = keyId(key)
    const existing = this.getLiveEntry(key)
    if (!existing) {
      return false
    }

    this.entries.delete(id)
    this.mutations.emit({
      type: 'delete',
      database: this.database,
      key: existing.key,
    })
    return true
  }

  expire(key: Buffer, expiresAt: number): boolean {
    const entry = this.getLiveEntry(key)
    if (!entry) {
      return false
    }

    entry.expiresAt = expiresAt
    this.mutations.emit({
      type: 'expire',
      database: this.database,
      key: entry.key,
      expiresAt,
    })
    return true
  }

  persist(key: Buffer): boolean {
    const entry = this.getLiveEntry(key)
    if (!entry || entry.expiresAt === undefined) {
      return false
    }

    delete entry.expiresAt
    this.mutations.emit({
      type: 'persist',
      database: this.database,
      key: entry.key,
    })
    return true
  }

  getExpiration(key: Buffer): ExpirationState {
    const entry = this.getLiveEntry(key)
    if (!entry) {
      return { kind: 'missing' }
    }

    if (entry.expiresAt === undefined) {
      return { kind: 'persistent' }
    }

    return { kind: 'expires', expiresAt: entry.expiresAt }
  }

  update<TValue extends RedisDataValue, TResult>(
    key: Buffer,
    expectedType: TValue['type'],
    createValue: () => TValue,
    mutator: (value: TValue) => TResult,
  ): TResult {
    let entry = this.getLiveEntry(key)

    if (!entry) {
      entry = {
        key: Buffer.from(key),
        value: createValue(),
      }
      this.entries.set(keyId(key), entry)
    }

    if (entry.value.type !== expectedType) {
      throw new WrongRedisTypeError(expectedType, entry.value.type)
    }

    const result = mutator(entry.value as TValue)
    this.emitWrite(entry)
    return result
  }

  flush(): void {
    this.entries.clear()
    this.mutations.emit({
      type: 'flush',
      database: this.database,
    })
  }

  size(): number {
    let count = 0

    for (const entry of Array.from(this.entries.values())) {
      if (!this.evictIfExpired(entry)) {
        count += 1
      }
    }

    return count
  }

  entriesSnapshot(): KeyspaceEntry[] {
    const entries: KeyspaceEntry[] = []

    for (const entry of Array.from(this.entries.values())) {
      if (this.evictIfExpired(entry)) {
        continue
      }

      entries.push({
        key: Buffer.from(entry.key),
        value: cloneRedisDataValue(entry.value),
        expiresAt: entry.expiresAt,
      })
    }

    return entries
  }

  private getLiveEntry(key: Buffer): KeyspaceEntry | null {
    const entry = this.entries.get(keyId(key))
    if (!entry) {
      return null
    }

    if (this.evictIfExpired(entry)) {
      return null
    }

    return entry
  }

  private evictIfExpired(entry: KeyspaceEntry): boolean {
    if (entry.expiresAt === undefined || entry.expiresAt > Date.now()) {
      return false
    }

    this.entries.delete(keyId(entry.key))
    this.mutations.emit({
      type: 'evict',
      database: this.database,
      key: entry.key,
    })
    return true
  }

  private emitWrite(entry: KeyspaceEntry): void {
    this.mutations.emit({
      type: 'write',
      database: this.database,
      key: entry.key,
      value: entry.value,
      expiresAt: entry.expiresAt,
    })
  }
}

function keyId(key: Buffer): string {
  return key.toString('hex')
}
