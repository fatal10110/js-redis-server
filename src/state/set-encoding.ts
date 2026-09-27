import type { RedisSetData } from './data-types'

// Real Redis stores a set of integers as an intset, which keeps its members
// sorted by value; any other set is a listpack (insertion order) or, when
// large, a hashtable (no defined order). SMEMBERS, SSCAN, SORT and every
// other reader walk the set in that storage order (#504).
//
// The mock keeps a set's `members` Map in exactly that order: ascending by
// value while `intset` is true, insertion order otherwise. A listpack and a
// hashtable are not told apart — a hashtable's order is undefined, so
// insertion order is as good an answer as any. The helpers below mirror the
// conversions in Redis's t_set.c, so a set becomes and stops being an intset
// at the same moments a real one does.

/** The part of the server configuration that decides a set's encoding. */
export type SetEncodingRules = {
  /** `set-max-intset-entries`: an intset holding more converts away. */
  readonly maxIntsetEntries: number
  /**
   * Redis 7.2 / Valkey 7.2 added listpack sets and, with them, a size hint:
   * SADD creates a key as an intset only when the number of members it was
   * given fits the intset limit, and converts an existing intset away before
   * adding more members than the limit. SPOP's rebuild of a large pop also
   * creates a listpack, rather than an intset, from a non-intset set. Before
   * 7.2 there is no hint and the rebuilt set starts from its first member.
   */
  readonly listpack: boolean
}

export const DEFAULT_SET_ENCODING_RULES: SetEncodingRules = {
  maxIntsetEntries: 512,
  listpack: true,
}

const INT64_MIN = -(2n ** 63n)
const INT64_MAX = 2n ** 63n - 1n

/**
 * The value an intset would store `member` as, or null when it is not one:
 * `string2ll()` takes no sign but '-', no leading zeros, no "-0", and only
 * the 64-bit signed range.
 */
export function intsetValue(member: Buffer): bigint | null {
  if (member.length === 0 || member.length > 20) return null
  const text = member.toString('latin1')
  if (!/^(?:0|-?[1-9][0-9]*)$/.test(text)) return null
  const value = BigInt(text)
  return value < INT64_MIN || value > INT64_MAX ? null : value
}

/** `intsetMaxEntries()`: the configured limit, capped at 2^30. */
function intsetLimit(rules: SetEncodingRules): number {
  return Math.min(rules.maxIntsetEntries, 2 ** 30)
}

/**
 * `setTypeCreate()` / `setTypeMaybeConvert()`, which SADD and SMOVE run before
 * adding `sizeHint` members starting with `first`. A new (empty) set starts
 * as an intset when `first` is an integer; from 7.2 only when `sizeHint`
 * also fits the intset limit. From 7.2 an existing intset about to receive
 * more members than the limit converts to a hashtable up front.
 */
export function prepareSetForAdd(
  set: RedisSetData,
  first: Buffer,
  sizeHint: number,
  rules: SetEncodingRules,
): void {
  if (set.members.size === 0) {
    set.intset =
      intsetValue(first) !== null &&
      (!rules.listpack || sizeHint <= rules.maxIntsetEntries)
    return
  }
  if (set.intset && rules.listpack && sizeHint > rules.maxIntsetEntries) {
    set.intset = false
  }
}

/**
 * `setTypeAdd()`: adds `member` unless present. An intset takes an integer in
 * value order and converts away once it holds more than the intset limit; a
 * non-integer converts it, keeping the integers in their sorted order ahead
 * of the new member. Any other set appends.
 */
export function addSetMember(
  set: RedisSetData,
  member: Buffer,
  rules: SetEncodingRules,
): boolean {
  const hex = member.toString('hex')
  if (set.members.has(hex)) return false

  if (!set.intset) {
    set.members.set(hex, member)
    return true
  }

  const value = intsetValue(member)
  if (value === null) {
    set.intset = false
    set.members.set(hex, member)
    return true
  }

  insertInValueOrder(set, hex, member, value)
  if (set.members.size > intsetLimit(rules)) {
    set.intset = false
  }
  return true
}

/**
 * Inserts an integer into an intset's value order. A Map only appends, so an
 * insert anywhere else rebuilds it: O(n), fine at the default limit of 512.
 * The position is found by binary search, parsing O(log n) members.
 */
function insertInValueOrder(
  set: RedisSetData,
  hex: string,
  member: Buffer,
  value: bigint,
): void {
  const entries = Array.from(set.members)
  const last = entries[entries.length - 1]
  if (!last || intsetValue(last[1])! < value) {
    set.members.set(hex, member)
    return
  }
  let low = 0
  let high = entries.length - 1
  while (low < high) {
    const mid = (low + high) >>> 1
    if (intsetValue(entries[mid][1])! < value) low = mid + 1
    else high = mid
  }
  const index = low
  entries.splice(index, 0, [hex, member])
  set.members.clear()
  for (const [entryHex, entryMember] of entries) {
    set.members.set(entryHex, entryMember)
  }
}

/**
 * `maybeConvertToIntset()`: a set whose members are all integers, and few
 * enough for an intset, becomes one, sorted by value.
 */
export function convertToIntsetIfPossible(
  set: RedisSetData,
  rules: SetEncodingRules,
): void {
  if (set.intset) return
  if (set.members.size > intsetLimit(rules)) return

  const numbered: Array<{ hex: string; member: Buffer; value: bigint }> = []
  for (const [hex, member] of set.members) {
    const value = intsetValue(member)
    if (value === null) return
    numbered.push({ hex, member, value })
  }
  numbered.sort((a, b) => (a.value < b.value ? -1 : a.value > b.value ? 1 : 0))
  set.members.clear()
  for (const entry of numbered) {
    set.members.set(entry.hex, entry.member)
  }
  set.intset = true
}
