import {
  createHashData,
  createListData,
  createSetData,
  createSortedSetData,
  createStreamData,
  createStringData,
  type RedisDataValue,
  type RedisHashData,
  type RedisListData,
  type RedisSetData,
  type RedisSortedSetData,
  type RedisStreamData,
} from './data-types'
import {
  ExpirationState,
  KeyspaceEntry,
  RedisKeyspace,
  SetOptions,
  WrongRedisTypeError,
} from './keyspace'
import {
  RedisMutationBus,
  RedisMutationListener,
  Unsubscribe,
} from './mutation-events'
import { SerialTurnQueue, type RedisTurnQueue } from '../core/turn-queue'
import { WrongTypeRedisError } from '../core/redis-error'

export class RedisDatabase {
  readonly mutations = new RedisMutationBus()
  /**
   * Per-database serialization turn. All sessions targeting this database
   * acquire turns from here so writes do not interleave. Sessions on other
   * databases run on independent queues, which means the mock allows
   * cross-database parallelism — real Redis is single-threaded across all
   * databases. Acceptable for a mock; do not rely on cross-database
   * serialization in tests.
   */
  readonly turnQueue: RedisTurnQueue = new SerialTurnQueue()
  private readonly keyspace: RedisKeyspace

  constructor(public readonly id: number) {
    this.keyspace = new RedisKeyspace(this.id, this.mutations)
  }

  get(key: Buffer): RedisDataValue | null {
    return this.keyspace.get(key)
  }

  getString(key: Buffer): Buffer | null {
    const value = this.keyspace.get(key)
    if (!value || value.type !== 'string') {
      return null
    }

    return Buffer.from(value.value)
  }

  getType(key: Buffer): RedisDataValue['type'] | null {
    return this.keyspace.getType(key)
  }

  set(key: Buffer, value: RedisDataValue, options?: SetOptions): void {
    this.keyspace.set(key, value, options)
  }

  setString(key: Buffer, value: Buffer, options?: SetOptions): void {
    this.keyspace.set(key, createStringData(value), options)
  }

  delete(key: Buffer): boolean {
    return this.keyspace.delete(key)
  }

  expire(key: Buffer, expiresAt: number): boolean {
    return this.keyspace.expire(key, expiresAt)
  }

  persist(key: Buffer): boolean {
    return this.keyspace.persist(key)
  }

  getExpiration(key: Buffer): ExpirationState {
    return this.keyspace.getExpiration(key)
  }

  getHash(key: Buffer): RedisHashData | null {
    return this.getTyped<RedisHashData>(key, 'hash')
  }

  getList(key: Buffer): RedisListData | null {
    return this.getTyped<RedisListData>(key, 'list')
  }

  getSet(key: Buffer): RedisSetData | null {
    return this.getTyped<RedisSetData>(key, 'set')
  }

  getSortedSet(key: Buffer): RedisSortedSetData | null {
    return this.getTyped<RedisSortedSetData>(key, 'zset')
  }

  getStream(key: Buffer): RedisStreamData | null {
    return this.getTyped<RedisStreamData>(key, 'stream')
  }

  updateHash<TResult>(
    key: Buffer,
    mutator: (hash: RedisHashData) => TResult,
  ): TResult {
    return this.updateTyped(key, 'hash', createHashData, mutator)
  }

  updateList<TResult>(
    key: Buffer,
    mutator: (list: RedisListData) => TResult,
  ): TResult {
    return this.updateTyped(key, 'list', createListData, mutator)
  }

  updateSet<TResult>(
    key: Buffer,
    mutator: (set: RedisSetData) => TResult,
  ): TResult {
    return this.updateTyped(key, 'set', createSetData, mutator)
  }

  updateSortedSet<TResult>(
    key: Buffer,
    mutator: (zset: RedisSortedSetData) => TResult,
  ): TResult {
    return this.updateTyped(key, 'zset', createSortedSetData, mutator)
  }

  updateStream<TResult>(
    key: Buffer,
    mutator: (stream: RedisStreamData) => TResult,
  ): TResult {
    return this.updateTyped(key, 'stream', createStreamData, mutator)
  }

  private getTyped<TValue extends RedisDataValue>(
    key: Buffer,
    expectedType: TValue['type'],
  ): TValue | null {
    const value = this.keyspace.get(key)
    if (!value) return null
    if (value.type !== expectedType) throw new WrongTypeRedisError()
    return value as TValue
  }

  private updateTyped<TValue extends RedisDataValue, TResult>(
    key: Buffer,
    expectedType: TValue['type'],
    createValue: () => TValue,
    mutator: (value: TValue) => TResult,
  ): TResult {
    try {
      return this.keyspace.update(key, expectedType, createValue, mutator)
    } catch (err) {
      if (err instanceof WrongRedisTypeError) throw new WrongTypeRedisError()
      throw err
    }
  }

  flush(): void {
    this.keyspace.flush()
  }

  size(): number {
    return this.keyspace.size()
  }

  entriesSnapshot(): KeyspaceEntry[] {
    return this.keyspace.entriesSnapshot()
  }

  subscribe(listener: RedisMutationListener): Unsubscribe {
    return this.mutations.subscribe(listener)
  }

  subscribeKey(key: Buffer, listener: RedisMutationListener): Unsubscribe {
    return this.mutations.subscribeKey(key, listener)
  }
}
