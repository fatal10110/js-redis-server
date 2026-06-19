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

// Dirty tracking is operation-based, not a final-value diff. An effective
// mutating helper marks the key dirty when the operation changes structure, and
// commands use forceDirty/forceWrite for Redis writes that must dirty WATCH even
// when the final value is equal (for example identical STORE rewrites or
// full-range LTRIM).

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

  ack(group: RedisStreamConsumerGroup, ids: readonly StreamId[]): number {
    let count = 0
    for (const id of ids) {
      if (group.pending.delete(streamIdKey(id))) count++
    }
    if (count > 0) {
      this.tracker.markChanged()
    }
    return count
  }

  forceWrite(): void {
    this.tracker.markChanged()
  }

  addGroup(groupId: string, group: RedisStreamConsumerGroup): void {
    this.stream.groups.set(groupId, group)
    this.tracker.markChanged()
  }

  deleteGroup(groupId: string): boolean {
    const removed = this.stream.groups.delete(groupId)
    if (removed) {
      this.tracker.markChanged()
    }
    return removed
  }

  addConsumer(
    group: RedisStreamConsumerGroup,
    consumerId: string,
    consumer: RedisStreamConsumer,
  ): boolean {
    if (group.consumers.has(consumerId)) {
      return false
    }

    group.consumers.set(consumerId, consumer)
    this.tracker.markChanged()
    return true
  }

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
    this.tracker.markChanged()
    return removedPending
  }
}

function streamIdKey(id: StreamId): string {
  return `${id.ms}-${id.seq}`
}
