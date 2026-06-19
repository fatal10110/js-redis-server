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
  type KeyspaceMutationTracker,
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
import {
  TrackedHashData,
  TrackedListData,
  TrackedSetData,
  TrackedSortedSetData,
  TrackedStreamData,
} from './tracked-values'

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
  /**
   * Name of the command currently executing against this database, set by the
   * CommandExecutor around `definition.execute`. Keyspace notifications read it
   * to name write events after the originating command (e.g. LPUSH → `lpush`),
   * which the mutation bus itself does not carry. `null` outside command
   * execution.
   */
  activeNotifyCommand: string | null = null
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
    mutator: (hash: TrackedHashData) => TResult,
  ): TResult {
    return this.updateTyped(
      key,
      'hash',
      createHashData,
      mutator,
      (value, tracker) => new TrackedHashData(value, tracker),
    )
  }

  updateList<TResult>(
    key: Buffer,
    mutator: (list: TrackedListData) => TResult,
  ): TResult {
    return this.updateTyped(
      key,
      'list',
      createListData,
      mutator,
      (value, tracker) => new TrackedListData(value, tracker),
    )
  }

  updateSet<TResult>(
    key: Buffer,
    mutator: (set: TrackedSetData) => TResult,
  ): TResult {
    return this.updateTyped(
      key,
      'set',
      createSetData,
      mutator,
      (value, tracker) => new TrackedSetData(value, tracker),
    )
  }

  updateSortedSet<TResult>(
    key: Buffer,
    mutator: (zset: TrackedSortedSetData) => TResult,
  ): TResult {
    return this.updateTyped(
      key,
      'zset',
      createSortedSetData,
      mutator,
      (value, tracker) => new TrackedSortedSetData(value, tracker),
    )
  }

  updateStream<TResult>(
    key: Buffer,
    mutator: (stream: TrackedStreamData) => TResult,
  ): TResult {
    return this.updateTyped(
      key,
      'stream',
      createStreamData,
      mutator,
      (value, tracker) => new TrackedStreamData(value, tracker),
    )
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

  private updateTyped<TValue extends RedisDataValue, TTracked, TResult>(
    key: Buffer,
    expectedType: TValue['type'],
    createValue: () => TValue,
    mutator: (value: TTracked) => TResult,
    track: (value: TValue, tracker: KeyspaceMutationTracker) => TTracked,
  ): TResult {
    try {
      return this.keyspace.update(
        key,
        expectedType,
        createValue,
        (value, tracker) => mutator(track(value as TValue, tracker)),
      )
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

  sweepExpired(now?: number): number {
    return this.keyspace.sweepExpired(now)
  }

  subscribe(listener: RedisMutationListener): Unsubscribe {
    return this.mutations.subscribe(listener)
  }

  subscribeKey(key: Buffer, listener: RedisMutationListener): Unsubscribe {
    return this.mutations.subscribeKey(key, listener)
  }
}
