import {
  cloneRedisDataValue,
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
  SetOptions,
} from './keyspace'
import {
  RedisMutationBus,
  RedisMutationListener,
  Unsubscribe,
} from './mutation-events'
import { SerialTurnQueue } from '../core/turn-queue'
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
  readonly turnQueue = new SerialTurnQueue()
  /**
   * Name of the command currently executing against this database, set by the
   * CommandExecutor around `definition.execute`. Keyspace notifications read it
   * to name write events after the originating command (e.g. LPUSH → `lpush`),
   * which the mutation bus itself does not carry. `null` outside command
   * execution.
   */
  activeNotifyCommand: string | null = null
  private readonly entries = new Map<string, KeyspaceEntry>()

  constructor(public readonly id: number) {}

  get(key: Buffer): RedisDataValue | null {
    const entry = this.getLiveEntry(key)
    if (!entry) {
      return null
    }

    return cloneRedisDataValue(entry.value)
  }

  getString(key: Buffer): Buffer | null {
    const value = this.get(key)
    if (!value || value.type !== 'string') {
      return null
    }

    return Buffer.from(value.value)
  }

  getType(key: Buffer): RedisDataValue['type'] | null {
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

  setString(key: Buffer, value: Buffer, options?: SetOptions): void {
    this.set(key, createStringData(value), options)
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
      database: this.id,
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
      database: this.id,
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
      database: this.id,
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

  /**
   * Read-modify-write a single key under its expected type, shared by the typed
   * `updateHash`/`updateList`/... wrappers. Private on purpose: it hands the
   * mutator the *untracked* value, which skips the `Tracked*` layer and with it
   * hash-field TTL expiry. Go through a wrapper.
   *
   * The mutator gets the value plus a tracker, and must mark what it did:
   * `markChanged` for a WATCH-dirtying write, `markCommitted` to persist an
   * in-place change to an **already-existing** key without dirtying. Creating
   * the key dirties either way — see `if (dirty || !existing)` below. An
   * unmarked mutation emits no event.
   *
   * Two sharp edges, both pre-existing:
   *
   * - Only a **brand-new** key is rolled back on a throw. For an existing key
   *   the mutator writes straight through the stored object (`getLiveEntry`
   *   returns the entry, not a copy), so a mutator that throws or forgets to
   *   mark leaves its partial edit in the keyspace with no event emitted — e.g.
   *   a ghost empty hash that `getType` still reports as `hash`, a state real
   *   Redis cannot represent.
   * - Where `markCommitted` does suppress — an in-place change to an existing
   *   key — it suppresses the mutation event *outright*, and that same bus also
   *   drives keyspace notifications. So the WATCH semantics below are faithful
   *   to real Redis, but the notification that real Redis would still fire is
   *   lost with it — real Redis keeps `signalModifiedKey` and
   *   `notifyKeyspaceEvent` independent. See #379.
   */
  private update<TValue extends RedisDataValue, TResult>(
    key: Buffer,
    expectedType: TValue['type'],
    createValue: () => TValue,
    mutator: (value: TValue, tracker: KeyspaceMutationTracker) => TResult,
  ): TResult {
    const existing = this.getLiveEntry(key)

    if (existing && existing.value.type !== expectedType) {
      throw new WrongTypeRedisError()
    }

    // For a new key, mutate a not-yet-committed entry: if the mutator throws,
    // the keyspace is left untouched (no ghost empty collection persists).
    const entry: KeyspaceEntry = existing ?? {
      key: Buffer.from(key),
      value: createValue(),
    }

    let dirty = false
    let committed = false
    const tracker: KeyspaceMutationTracker = {
      markChanged: () => {
        dirty = true
      },
      markCommitted: () => {
        committed = true
      },
    }

    const result = mutator(entry.value as TValue, tracker)
    const id = keyId(key)

    if (!dirty && !committed) {
      return result
    }

    // Centralized "delete the key when its collection is empty" rule, so each
    // command no longer has to remember to clean up emptied hashes/lists/etc.
    if (isEmptyCollection(entry.value)) {
      if (existing) {
        this.entries.delete(id)
        this.mutations.emit({
          type: 'delete',
          database: this.id,
          key: entry.key,
        })
      }
      return result
    }

    this.entries.set(id, entry)
    // A markChanged write always dirties WATCH. A markCommitted-only change
    // dirties only when it creates the key (`!existing`): real Redis treats
    // bringing a watched key into existence as a write, but leaves a WATCH
    // intact for in-place metadata changes to an already-existing key.
    if (dirty || !existing) {
      this.emitWrite(entry)
    }
    return result
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
    const value = this.get(key)
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
    return this.update(key, expectedType, createValue, (value, tracker) =>
      mutator(track(value as TValue, tracker)),
    )
  }

  flush(): void {
    this.entries.clear()
    this.mutations.emit({
      type: 'flush',
      database: this.id,
    })
  }

  size(): number {
    this.sweepExpired()
    return this.entries.size
  }

  entriesSnapshot(): KeyspaceEntry[] {
    this.sweepExpired()
    const entries: KeyspaceEntry[] = []

    for (const entry of this.entries.values()) {
      entries.push({
        key: Buffer.from(entry.key),
        value: cloneRedisDataValue(entry.value),
        expiresAt: entry.expiresAt,
      })
    }

    return entries
  }

  sweepExpired(now = Date.now()): number {
    let count = 0

    for (const entry of Array.from(this.entries.values())) {
      if (this.evictIfExpired(entry, now)) {
        count += 1
      }
    }

    return count
  }

  subscribe(listener: RedisMutationListener): Unsubscribe {
    return this.mutations.subscribe(listener)
  }

  subscribeKey(key: Buffer, listener: RedisMutationListener): Unsubscribe {
    return this.mutations.subscribeKey(key, listener)
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

  private evictIfExpired(entry: KeyspaceEntry, now = Date.now()): boolean {
    if (entry.expiresAt === undefined || entry.expiresAt > now) {
      return false
    }

    this.entries.delete(keyId(entry.key))
    this.mutations.emit({
      type: 'evict',
      database: this.id,
      key: entry.key,
    })
    return true
  }

  private emitWrite(entry: KeyspaceEntry): void {
    this.mutations.emit({
      type: 'write',
      database: this.id,
      key: entry.key,
      value: entry.value,
      expiresAt: entry.expiresAt,
    })
  }
}

function keyId(key: Buffer): string {
  return key.toString('hex')
}

// A collection-typed value is "empty" when it holds no elements; such keys are
// deleted from the keyspace (matching real Redis). Strings are always a real
// value (even ""), and empty streams persist (e.g. XGROUP CREATE MKSTREAM), so
// neither is ever auto-deleted here.
function isEmptyCollection(value: RedisDataValue): boolean {
  switch (value.type) {
    case 'hash':
      return value.fields.size === 0
    case 'list':
      return value.values.length === 0
    case 'set':
    case 'zset':
      return value.members.size === 0
    // 'string' is unreachable today and therefore untested: `update` is private
    // and its only caller, `updateTyped`, is reached through the five typed
    // wrappers, none of which passes 'string' (there is no `updateString` —
    // strings are written whole via `set`/`setString`, which has no
    // empty-collection rule). Kept for exhaustiveness: the switch has no
    // `default`, so deleting the arm breaks the build. If a future
    // `updateString` wrapper appears, or `update` is widened, this arm becomes
    // live and must stay `false` — otherwise `SET k ""` starts deleting the key.
    case 'string':
    case 'stream':
      return false
  }
}
