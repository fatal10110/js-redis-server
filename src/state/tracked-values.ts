import type {
  RedisHashData,
  RedisHashField,
  RedisListData,
  RedisSetData,
  RedisSortedSetData,
  RedisSortedSetMember,
  RedisStreamConsumer,
  RedisStreamConsumerGroup,
  RedisStreamData,
  RedisStreamEntry,
  StreamId,
} from './data-types'
import type { KeyspaceMutationTracker } from './keyspace'
import {
  ensureConsumer,
  findEntry,
  pendingEntriesSorted,
} from './stream-groups'
import {
  cloneStreamId,
  compareStreamId,
  MIN_ID,
  streamIdKey,
} from './stream-ids'

// Dirty tracking is operation-based, not a final-value diff. An effective
// mutating helper marks the key dirty when the operation changes structure, and
// commands pass forceDirty for Redis writes that must dirty WATCH even when the
// final value is equal (for example identical STORE rewrites or full-range
// LTRIM). Stream consumer-group / pending / last-id mutations deliberately do
// NOT mark the key dirty: real Redis leaves a WATCH on the stream key intact for
// those, only entry-set changes (XADD/XDEL/…) touch it.

export class TrackedHashData {
  constructor(
    private readonly hash: RedisHashData,
    private readonly tracker: KeyspaceMutationTracker,
  ) {}

  get size(): number {
    this.deleteExpiredFields()
    return this.hash.fields.size
  }

  getField(field: Buffer): RedisHashField | undefined {
    const id = field.toString('hex')
    const entry = this.hash.fields.get(id)
    if (!entry) {
      return undefined
    }

    if (isHashFieldExpired(entry)) {
      this.hash.fields.delete(id)
      this.tracker.markChanged()
      return undefined
    }

    return entry
  }

  hasField(field: Buffer): boolean {
    return this.getField(field) !== undefined
  }

  setField(
    field: Buffer,
    value: Buffer,
    options: { forceDirty?: boolean; keepTtl?: boolean } = {},
  ): { added: boolean; valueChanged: boolean } {
    const hex = field.toString('hex')
    const existing = this.getField(field)
    const valueChanged = !existing || !existing.value.equals(value)
    const expiresAt = options.keepTtl ? existing?.expiresAt : undefined
    const ttlChanged = existing?.expiresAt !== expiresAt

    this.hash.fields.set(hex, { field, value, expiresAt })
    if (options.forceDirty || valueChanged || ttlChanged) {
      this.tracker.markChanged()
    }
    return { added: existing === undefined, valueChanged }
  }

  setFieldIfAbsent(field: Buffer, value: Buffer): boolean {
    if (this.hasField(field)) {
      return false
    }

    this.setField(field, value)
    return true
  }

  deleteField(field: Buffer): boolean {
    const existing = this.getField(field)
    if (!existing) {
      return false
    }

    const deleted = this.hash.fields.delete(field.toString('hex'))
    if (deleted) {
      this.tracker.markChanged()
    }
    return deleted
  }

  setFieldExpiration(field: Buffer, expiresAt: number): boolean {
    const entry = this.getField(field)
    if (!entry) {
      return false
    }

    if (entry.expiresAt !== expiresAt) {
      entry.expiresAt = expiresAt
      this.tracker.markChanged()
    }
    return true
  }

  clearFieldExpiration(field: Buffer): boolean {
    const entry = this.getField(field)
    if (!entry || entry.expiresAt === undefined) {
      return false
    }

    delete entry.expiresAt
    this.tracker.markChanged()
    return true
  }

  entries(): IterableIterator<RedisHashField> {
    this.deleteExpiredFields()
    return this.hash.fields.values()
  }

  private deleteExpiredFields(): void {
    const now = Date.now()
    let deleted = false

    for (const [id, entry] of this.hash.fields) {
      if (isHashFieldExpired(entry, now)) {
        this.hash.fields.delete(id)
        deleted = true
      }
    }

    if (deleted) {
      this.tracker.markChanged()
    }
  }
}

function isHashFieldExpired(field: RedisHashField, now = Date.now()): boolean {
  return field.expiresAt !== undefined && field.expiresAt <= now
}

export class TrackedListData {
  constructor(
    private readonly list: RedisListData,
    private readonly tracker: KeyspaceMutationTracker,
  ) {}

  get length(): number {
    return this.list.values.length
  }

  pushLeft(values: readonly Buffer[]): number {
    for (const value of values) {
      this.list.values.unshift(value)
    }
    this.tracker.markChanged()
    return this.list.values.length
  }

  pushRight(values: readonly Buffer[]): number {
    for (const value of values) {
      this.list.values.push(value)
    }
    this.tracker.markChanged()
    return this.list.values.length
  }

  pop(side: 'left' | 'right'): Buffer | null {
    const value =
      side === 'left' ? this.list.values.shift() : this.list.values.pop()
    if (value !== undefined) {
      this.tracker.markChanged()
    }
    return value ?? null
  }

  popMany(side: 'left' | 'right', count: number): Buffer[] {
    const values =
      side === 'left'
        ? this.list.values.splice(0, count)
        : this.list.values.splice(Math.max(0, this.list.values.length - count))
    if (side === 'right') {
      values.reverse()
    }
    if (values.length > 0) {
      this.tracker.markChanged()
    }
    return values
  }

  setAt(index: number, value: Buffer): void {
    this.list.values[index] = value
    this.tracker.markChanged()
  }

  insertRelativeTo(
    pivot: Buffer,
    element: Buffer,
    position: 'before' | 'after',
  ): number {
    const pivotIndex = this.list.values.findIndex(value => value.equals(pivot))
    if (pivotIndex === -1) {
      return -1
    }

    const insertIndex = position === 'before' ? pivotIndex : pivotIndex + 1
    this.list.values.splice(insertIndex, 0, element)
    this.tracker.markChanged()
    return this.list.values.length
  }

  removeMatching(count: number, element: Buffer): number {
    const target = element.toString('binary')
    let removed = 0

    if (count === 0) {
      for (let i = this.list.values.length - 1; i >= 0; i--) {
        if (this.list.values[i].toString('binary') === target) {
          this.list.values.splice(i, 1)
          removed++
        }
      }
    } else if (count > 0) {
      for (let i = 0; i < this.list.values.length && removed < count; i++) {
        if (this.list.values[i].toString('binary') === target) {
          this.list.values.splice(i, 1)
          removed++
          i--
        }
      }
    } else {
      const absCount = Math.abs(count)
      for (
        let i = this.list.values.length - 1;
        i >= 0 && removed < absCount;
        i--
      ) {
        if (this.list.values[i].toString('binary') === target) {
          this.list.values.splice(i, 1)
          removed++
        }
      }
    }

    if (removed > 0) {
      this.tracker.markChanged()
    }
    return removed
  }

  trim(start: number, stop: number, options: { forceDirty?: boolean } = {}) {
    const originalLength = this.list.values.length
    const changed = start > stop || start > 0 || stop < originalLength - 1

    if (start > stop) {
      this.list.values.length = 0
    } else {
      this.list.values.splice(0, start)
      this.list.values.splice(stop - start + 1)
    }
    if (options.forceDirty || changed) {
      this.tracker.markChanged()
    }
    return { empty: this.list.values.length === 0 }
  }
}

export class TrackedSetData {
  constructor(
    private readonly set: RedisSetData,
    private readonly tracker: KeyspaceMutationTracker,
  ) {}

  get size(): number {
    return this.set.members.size
  }

  hasMember(member: Buffer): boolean {
    return this.set.members.has(member.toString('hex'))
  }

  addMember(member: Buffer): boolean {
    const hex = member.toString('hex')
    if (this.set.members.has(hex)) {
      return false
    }

    this.set.members.set(hex, member)
    this.tracker.markChanged()
    return true
  }

  deleteMember(member: Buffer): boolean {
    const deleted = this.set.members.delete(member.toString('hex'))
    if (deleted) {
      this.tracker.markChanged()
    }
    return deleted
  }

  deleteMemberId(hex: string): boolean {
    const deleted = this.set.members.delete(hex)
    if (deleted) {
      this.tracker.markChanged()
    }
    return deleted
  }

  randomMemberEntries(): [string, Buffer][] {
    return Array.from(this.set.members.entries())
  }

  replaceMembers(
    hexSet: Set<string>,
    bufferMap: Map<string, Buffer>,
    options: { forceDirty?: boolean } = {},
  ): void {
    const changed =
      options.forceDirty ||
      this.set.members.size !== hexSet.size ||
      Array.from(hexSet).some(hex => !this.set.members.has(hex))

    this.set.members.clear()
    for (const hex of hexSet) {
      const buf = bufferMap.get(hex)!
      this.set.members.set(hex, buf)
    }

    if (changed) {
      this.tracker.markChanged()
    }
  }
}

export class TrackedSortedSetData {
  constructor(
    private readonly zset: RedisSortedSetData,
    private readonly tracker: KeyspaceMutationTracker,
  ) {}

  get size(): number {
    return this.zset.members.size
  }

  getMember(member: Buffer): RedisSortedSetMember | undefined {
    return this.zset.members.get(member.toString('hex'))
  }

  setScore(
    member: Buffer,
    score: number,
    options: { forceDirty?: boolean } = {},
  ): { added: boolean; scoreChanged: boolean } {
    const hex = member.toString('hex')
    const existing = this.zset.members.get(hex)
    const scoreChanged = !existing || existing.score !== score
    this.zset.members.set(hex, { member, score })
    if (options.forceDirty || scoreChanged) {
      this.tracker.markChanged()
    }
    return { added: existing === undefined, scoreChanged }
  }

  deleteMember(member: Buffer): boolean {
    return this.deleteMemberId(member.toString('hex'))
  }

  deleteMemberId(hex: string): boolean {
    const deleted = this.zset.members.delete(hex)
    if (deleted) {
      this.tracker.markChanged()
    }
    return deleted
  }

  entries(): IterableIterator<[string, RedisSortedSetMember]> {
    return this.zset.members.entries()
  }

  replaceMembers(
    members: Map<string, RedisSortedSetMember>,
    options: { forceDirty?: boolean } = {},
  ): void {
    const changed =
      options.forceDirty ||
      this.zset.members.size !== members.size ||
      Array.from(members).some(([hex, member]) => {
        const current = this.zset.members.get(hex)
        return !current || current.score !== member.score
      })

    this.zset.members.clear()
    for (const [hex, member] of members) {
      this.zset.members.set(hex, member)
    }

    if (changed) {
      this.tracker.markChanged()
    }
  }
}

// XREADGROUP delivery: a present entry carries its fields; a history read of an
// entry that has since been deleted carries fields === null.
export type StreamDelivery = { id: StreamId; fields: Buffer[] | null }
export type ClaimedEntry = { id: StreamId; fields: Buffer[] }
export type AutoClaimResult = {
  nextStartId: StreamId
  claimed: ClaimedEntry[]
  deleted: StreamId[]
}

export class TrackedStreamData {
  constructor(
    private readonly stream: RedisStreamData,
    private readonly tracker: KeyspaceMutationTracker,
  ) {}

  get value(): RedisStreamData {
    return this.stream
  }

  get lastId(): StreamId {
    return this.stream.lastId
  }

  appendEntry(id: StreamId, fields: Buffer[]): void {
    this.stream.entries.push({ id, fields })
    this.stream.lastId = id
    this.stream.entriesAdded++
    this.tracker.markChanged()
  }

  trim(callback: (stream: RedisStreamData) => number): number {
    const removed = callback(this.stream)
    if (removed > 0) {
      this.tracker.markChanged()
    }
    return removed
  }

  deleteEntries(
    targets: readonly StreamId[],
    compare: (left: StreamId, right: StreamId) => number,
    onDelete: (entry: RedisStreamEntry) => void,
  ): number {
    let count = 0
    for (const target of targets) {
      const idx = this.stream.entries.findIndex(
        entry => compare(entry.id, target) === 0,
      )
      if (idx !== -1) {
        onDelete(this.stream.entries[idx])
        this.stream.entries.splice(idx, 1)
        count++
      }
    }

    if (count > 0) {
      this.tracker.markChanged()
    }
    return count
  }

  // XACK. Clearing pending entries mutates the live group in place (the stream
  // key already exists), so no commit is needed; real Redis does not dirty a
  // WATCH on the stream key for a PEL change, so no markChanged().
  ack(group: RedisStreamConsumerGroup, ids: readonly StreamId[]): number {
    let count = 0
    for (const id of ids) {
      if (group.pending.delete(streamIdKey(id))) count++
    }
    return count
  }

  // XSETID. The stream already exists (the command rejects a missing key), so
  // mutating the live value in place is enough to persist; real Redis does not
  // touch a WATCH on the stream key for a last-id change, so no markChanged().
  setId(
    id: StreamId,
    options: { entriesAdded: number | null; maxDeletedId: StreamId | null },
  ): void {
    this.stream.lastId = cloneStreamId(id)
    if (options.entriesAdded !== null) {
      this.stream.entriesAdded = options.entriesAdded
    }
    if (options.maxDeletedId !== null) {
      this.stream.maxDeletedEntryId = cloneStreamId(options.maxDeletedId)
    }
  }

  // XGROUP SETID. Consumer-group metadata changes do not dirty a WATCH on the
  // stream key in real Redis, so this never calls markChanged().
  setGroupId(
    group: RedisStreamConsumerGroup,
    lastDeliveredId: StreamId,
    entriesRead: number | null,
  ): void {
    group.lastDeliveredId = cloneStreamId(lastDeliveredId)
    group.entriesRead = entriesRead
  }

  // XREADGROUP for a single stream. Returns the delivered entries (fields === null
  // marks a history entry that was deleted from the stream) for the command to
  // shape into a reply. Delivering messages / advancing the PEL does not dirty a
  // WATCH on the stream key in real Redis.
  readGroup(
    group: RedisStreamConsumerGroup,
    consumerName: Buffer,
    id: StreamId | '>',
    options: { count: number | null; noack: boolean },
    now: number,
  ): StreamDelivery[] {
    ensureConsumer(group, consumerName, now).activeAt = now
    const consumerId = consumerName.toString('hex')
    const { count, noack } = options
    const delivered: StreamDelivery[] = []
    const limited = count !== null && count > 0

    if (id === '>') {
      for (const entry of this.stream.entries) {
        if (compareStreamId(entry.id, group.lastDeliveredId) <= 0) continue

        delivered.push({ id: entry.id, fields: entry.fields })
        group.lastDeliveredId = cloneStreamId(entry.id)
        group.entriesRead = (group.entriesRead ?? 0) + 1

        if (!noack) {
          group.pending.set(streamIdKey(entry.id), {
            id: cloneStreamId(entry.id),
            consumerId,
            deliveredAt: now,
            deliveryCount: 1,
          })
        }

        if (limited && delivered.length >= count) break
      }
      return delivered
    }

    for (const pending of pendingEntriesSorted(group)) {
      if (pending.consumerId !== consumerId) continue
      if (compareStreamId(pending.id, id) <= 0) continue

      const entry = findEntry(this.stream, pending.id)
      delivered.push(
        entry
          ? { id: entry.id, fields: entry.fields }
          : { id: pending.id, fields: null },
      )

      if (limited && delivered.length >= count) break
    }
    return delivered
  }

  // XCLAIM. Returns the claimed entries for the command to shape into a reply
  // (justId vs full). Reassigning pending ownership does not dirty a WATCH on the
  // stream key in real Redis.
  claim(
    group: RedisStreamConsumerGroup,
    consumerName: Buffer,
    ids: readonly StreamId[],
    options: {
      minIdleMs: number
      idleMs: number | null
      timeMs: number | null
      retryCount: number | null
      force: boolean
      justId: boolean
      lastId: StreamId | null
    },
    now: number,
  ): ClaimedEntry[] {
    ensureConsumer(group, consumerName, now).activeAt = now
    const consumerId = consumerName.toString('hex')
    if (options.lastId) group.lastDeliveredId = cloneStreamId(options.lastId)

    const claimed: ClaimedEntry[] = []
    for (const id of ids) {
      const pendingId = streamIdKey(id)
      const entry = findEntry(this.stream, id)
      let pending = group.pending.get(pendingId)

      if (!pending && options.force && entry) {
        pending = {
          id: cloneStreamId(id),
          consumerId,
          deliveredAt: now,
          deliveryCount: 0,
        }
        group.pending.set(pendingId, pending)
      }

      if (!pending) continue
      if (!entry) {
        group.pending.delete(pendingId)
        continue
      }

      const idleTime = Math.max(0, now - pending.deliveredAt)
      if (idleTime < options.minIdleMs) continue

      pending.consumerId = consumerId
      pending.deliveredAt =
        options.timeMs ?? (options.idleMs !== null ? now - options.idleMs : now)
      if (options.retryCount !== null) {
        pending.deliveryCount = options.retryCount
      } else if (!options.justId) {
        pending.deliveryCount++
      }

      claimed.push({ id: entry.id, fields: entry.fields })
    }
    return claimed
  }

  // XAUTOCLAIM. Returns the next cursor, the claimed entries, and the ids of
  // pending entries dropped because their stream entry was gone. Does not dirty a
  // WATCH on the stream key in real Redis.
  autoClaim(
    group: RedisStreamConsumerGroup,
    consumerName: Buffer,
    options: {
      minIdleMs: number
      start: StreamId
      count: number
      justId: boolean
    },
    now: number,
  ): AutoClaimResult {
    ensureConsumer(group, consumerName, now).activeAt = now
    const consumerId = consumerName.toString('hex')
    const claimed: ClaimedEntry[] = []
    const deleted: StreamId[] = []
    let nextStartId: StreamId = MIN_ID

    for (const pending of pendingEntriesSorted(group)) {
      if (compareStreamId(pending.id, options.start) < 0) continue

      const entry = findEntry(this.stream, pending.id)
      if (!entry) {
        group.pending.delete(streamIdKey(pending.id))
        deleted.push(pending.id)
        continue
      }

      const idleTime = Math.max(0, now - pending.deliveredAt)
      if (idleTime < options.minIdleMs) continue

      pending.consumerId = consumerId
      pending.deliveredAt = now
      if (!options.justId) pending.deliveryCount++
      claimed.push({ id: entry.id, fields: entry.fields })

      if (claimed.length >= options.count) {
        const next = pendingEntriesSorted(group).find(
          item => compareStreamId(item.id, pending.id) > 0,
        )
        nextStartId = next ? cloneStreamId(next.id) : MIN_ID
        break
      }
    }

    return { nextStartId, claimed, deleted }
  }

  // XGROUP CREATE. Adding a group does not dirty a WATCH on an existing stream
  // key in real Redis, but XGROUP CREATE ... MKSTREAM can create a brand-new
  // key that still must be persisted (and, as a key creation, does dirty the
  // WATCH). markCommitted() commits the value while letting keyspace.update
  // decide the dirty signal based on whether the key already existed.
  addGroup(groupId: string, group: RedisStreamConsumerGroup): void {
    this.stream.groups.set(groupId, group)
    this.tracker.markCommitted()
  }

  // XGROUP DESTROY. In-place change to an existing stream key; real Redis does
  // not dirty a WATCH on the stream key, so no markChanged().
  deleteGroup(groupId: string): boolean {
    return this.stream.groups.delete(groupId)
  }

  // XGROUP CREATECONSUMER. In-place change to an existing stream key; real
  // Redis does not dirty a WATCH on the stream key, so no markChanged().
  addConsumer(
    group: RedisStreamConsumerGroup,
    consumerId: string,
    consumer: RedisStreamConsumer,
  ): boolean {
    if (group.consumers.has(consumerId)) {
      return false
    }

    group.consumers.set(consumerId, consumer)
    return true
  }

  // XGROUP DELCONSUMER. In-place change to an existing stream key; real Redis
  // does not dirty a WATCH on the stream key, so no markChanged().
  deleteConsumer(group: RedisStreamConsumerGroup, consumerId: string): number {
    if (!group.consumers.delete(consumerId)) {
      return 0
    }

    let removedPending = 0
    for (const [pendingId, pending] of Array.from(group.pending)) {
      if (pending.consumerId !== consumerId) continue
      group.pending.delete(pendingId)
      removedPending++
    }
    return removedPending
  }
}
