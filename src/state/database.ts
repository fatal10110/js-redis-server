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
  type RedisMutationEvent,
  RedisMutationListener,
  Unsubscribe,
} from './mutation-events'
import { WrongTypeRedisError } from '../core/redis-error'
import { KeyReadySignals } from './key-ready-signals'
import {
  TrackedHashData,
  TrackedListData,
  TrackedSetData,
  TrackedSortedSetData,
  TrackedStreamData,
} from './tracked-values'

export class RedisDatabase {
  readonly mutations = new RedisMutationBus()
  private readonly readySignals = new KeyReadySignals()
  /**
   * The command this handle mutates on behalf of, stamped on every event it
   * emits as {@link RedisMutationEvent.command}. `undefined` on the database
   * itself; set only on a {@link withOrigin} view.
   */
  readonly origin: string | undefined = undefined
  private readonly entries = new Map<string, KeyspaceEntry>()
  /**
   * For each hash key that may hold a field with a TTL: a lower bound on its
   * earliest field deadline. Only a hash whose bound is due is ever scanned for
   * expired fields, so writes to a hash with no due field cost O(1) here.
   *
   * The bound may be too early, never too late. A write that sets a field TTL
   * lowers it (`noteHashFieldExpiry`); removing or extending a TTL leaves it
   * stale-early, and the next due scan then finds nothing and resets it to the
   * exact earliest deadline — or drops the key once no field has a TTL.
   */
  private readonly hashFieldExpiry = new Map<string, number>()

  constructor(public readonly id: number) {}

  /**
   * A handle onto this same database whose mutation events carry `command`
   * as their origin, so keyspace notifications can name them after it. The
   * executor hands every command such a view as `ctx.db`.
   *
   * The name travels with the handle, not with the database: a command that
   * parks (BLPOP) and resumes — in any order relative to others — still
   * writes under its own name, and never lends it to another command writing
   * into the same database meanwhile (#444).
   *
   * The view is a prototype link, not a copy: all state (`entries`,
   * `mutations`) is read through to this database, and only
   * `origin` is its own. RedisDatabase methods therefore must never assign a
   * field on `this` — the write would land on the view.
   */
  withOrigin(command: string): RedisDatabase {
    return Object.create(this, {
      origin: { value: command, enumerable: true },
    }) as RedisDatabase
  }

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
    this.hashFieldExpiry.delete(id)
    if (entry.value.type === 'hash') {
      for (const field of entry.value.fields.values()) {
        if (field.expiresAt !== undefined) {
          this.noteHashFieldExpiry(id, field.expiresAt)
        }
      }
    }
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
    this.emit({
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
    this.emit({
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
    this.emit({
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
   * in-place change to an **already-existing** key that is announced (a
   * `notify` event) without dirtying. Creating the key dirties either way — see
   * `if (dirty || !existing)` below. An unmarked mutation emits no event.
   *
   * Sharp edge (pre-existing): only a **brand-new** key is rolled back on a
   * throw. For an existing key the mutator writes straight through the stored
   * object (`getLiveEntry` returns the entry, not a copy), so a mutator that
   * throws or forgets to mark leaves its partial edit in the keyspace with no
   * event emitted — e.g. a ghost empty hash that `getType` still reports as
   * `hash`, a state real Redis cannot represent.
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
    // Like real Redis, the removal itself (hdel, lpop, ...) is announced first,
    // then `del`; the `delete` alone is the modified-key signal.
    if (isEmptyCollection(entry.value)) {
      if (existing) {
        this.entries.delete(id)
        this.emitNotify(entry)
        this.emit({
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
    // intact for in-place metadata changes to an already-existing key — while
    // still announcing them, hence `notify`.
    if (dirty || !existing) {
      this.emitWrite(entry)
    } else {
      this.emitNotify(entry)
    }
    return result
  }

  updateHash<TResult>(
    key: Buffer,
    mutator: (hash: TrackedHashData) => TResult,
  ): TResult {
    this.purgeExpiredHashFields(key)
    const id = keyId(key)
    return this.updateTyped(
      key,
      'hash',
      createHashData,
      mutator,
      (value, tracker) =>
        new TrackedHashData(value, tracker, expiresAt =>
          this.noteHashFieldExpiry(id, expiresAt),
        ),
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

  /**
   * Drop a hash's expired fields as a mutation of their own, published as
   * `hexpired` (then `del` if that empties the hash) — never under the name of
   * the command that happened to touch the key.
   *
   * Two callers, mirroring real Redis in its default mode (active expiry on):
   * - the active sweep ({@link sweepExpired}, every server tick), which is what
   *   publishes `hexpired` / `del` with no access to the key at all, and makes
   *   `EXISTS` report the emptied hash as gone;
   * - `updateHash`, as a lazy fallback for a field that expired since the last
   *   tick, so no command ever observes an expired field.
   *
   * The lazy fallback differs from real Redis with active expiry *disabled*
   * (`DEBUG SET-ACTIVE-EXPIRE 0`), where only field lookups (HGET, HEXISTS)
   * expire a field and whole-hash reads (HGETALL, HLEN, ...) leave it in
   * place, publish nothing and keep a WATCH intact; here any hash access
   * purges, publishes `hexpired` and dirties a WATCH on the key.
   */
  private purgeExpiredHashFields(key: Buffer, now = Date.now()): void {
    const id = keyId(key)
    const due = this.hashFieldExpiry.get(id)
    if (due === undefined || due > now) {
      return
    }

    const entry = this.getLiveEntry(key)
    if (!entry || entry.value.type !== 'hash') {
      this.hashFieldExpiry.delete(id)
      return
    }

    const expired: string[] = []
    let next = Infinity
    for (const [fieldId, field] of entry.value.fields) {
      if (field.expiresAt === undefined) continue
      if (field.expiresAt <= now) expired.push(fieldId)
      else if (field.expiresAt < next) next = field.expiresAt
    }
    if (next === Infinity) {
      this.hashFieldExpiry.delete(id)
    } else {
      this.hashFieldExpiry.set(id, next)
    }
    if (expired.length === 0) {
      return
    }

    this.withOrigin('hexpired').update(
      key,
      'hash',
      createHashData,
      (hash, tracker) => {
        for (const fieldId of expired) {
          hash.fields.delete(fieldId)
        }
        tracker.markChanged()
      },
    )
  }

  // Lower the key's earliest-field-deadline bound to `expiresAt` if earlier.
  private noteHashFieldExpiry(id: string, expiresAt: number): void {
    const due = this.hashFieldExpiry.get(id)
    if (due === undefined || expiresAt < due) {
      this.hashFieldExpiry.set(id, expiresAt)
    }
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
    this.hashFieldExpiry.clear()
    this.emit({
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

  /**
   * Active expiry: evict expired keys, then purge expired fields of the
   * hashes whose earliest field deadline is due. Returns the number of keys
   * removed.
   */
  sweepExpired(now = Date.now()): number {
    let count = 0

    for (const entry of Array.from(this.entries.values())) {
      if (this.evictIfExpired(entry, now)) {
        count += 1
      }
    }

    for (const [id, due] of Array.from(this.hashFieldExpiry)) {
      if (due > now) continue
      const entry = this.entries.get(id)
      if (!entry) {
        this.hashFieldExpiry.delete(id)
        continue
      }

      this.purgeExpiredHashFields(entry.key, now)
      if (!this.entries.has(id)) count += 1
    }

    return count
  }

  subscribe(listener: RedisMutationListener): Unsubscribe {
    return this.mutations.subscribe(listener)
  }

  subscribeKey(key: Buffer, listener: RedisMutationListener): Unsubscribe {
    return this.mutations.subscribeKey(key, listener)
  }

  /**
   * Wake the clients blocked on `key` without modifying it (real Redis'
   * `signalKeyAsReady`): no WATCH is dirtied and no notification fires. See
   * {@link KeyReadySignals}.
   */
  signalKeyReady(key: Buffer): void {
    this.readySignals.signal(key)
  }

  subscribeKeyReady(key: Buffer, listener: () => void): Unsubscribe {
    return this.readySignals.subscribe(key, listener)
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
    this.emit({
      type: 'evict',
      database: this.id,
      key: entry.key,
    })
    return true
  }

  private emitWrite(entry: KeyspaceEntry): void {
    this.emit({
      type: 'write',
      database: this.id,
      key: entry.key,
      value: entry.value,
      valueType: entry.value.type,
      expiresAt: entry.expiresAt,
    })
  }

  private emit(event: RedisMutationEvent): void {
    this.mutations.emit(
      this.origin === undefined ? event : { ...event, command: this.origin },
    )
  }

  private emitNotify(entry: KeyspaceEntry): void {
    this.emit({
      type: 'notify',
      database: this.id,
      key: entry.key,
      valueType: entry.value.type,
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
