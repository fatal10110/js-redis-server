import type {
  RedisSortedSetData,
  RedisSortedSetMember,
  ZsetEncoding,
} from './data-types'

export type { ZsetEncoding }

// Real Redis keeps a small sorted set as a listpack (a ziplist on 6.2) and a
// large one as a skiplist, and a few replies depend on which one it is
// (#512):
//
//  - ZSCAN walks a listpack in (score, member) order and spells each score
//    like any score reply, but walks a skiplist's hash table and spells each
//    score with `ld2string(…, LD_STR_AUTO)`, `%.17Lg` on every version;
//  - from Redis 7.0 a listpack stores an integral score as an integer, so -0
//    reads back as 0, where a skiplist (and 6.2's ziplist) keeps -0.
//
// A sorted set's `encoding` records which one a real server would hold. The
// helpers below mirror the conversions in t_zset.c: a listpack becomes a
// skiplist when it grows past `zset-max-listpack-entries` or receives a
// member longer than `zset-max-listpack-value`, and never converts back on
// its own; only the STORE commands build their result as a listpack again
// when it fits. OBJECT ENCODING (#117) can read the same field.

/** The part of the server configuration that decides a zset's encoding. */
export type ZsetEncodingRules = {
  /** `zset-max-listpack-entries` (`zset-max-ziplist-entries` on 6.2). */
  readonly maxEntries: number
  /** `zset-max-listpack-value` (`zset-max-ziplist-value` on 6.2). */
  readonly maxValue: number
  /**
   * Redis 7.2 / Valkey 7.2: ZADD passes its pair count to `zsetTypeCreate()`
   * and `zsetTypeMaybeConvert()`, so a ZADD of more pairs than the entry
   * limit makes (or converts to) a skiplist up front, duplicates included.
   */
  readonly sizeHint: boolean
  /**
   * Valkey 8.0: ZADD also passes its longest member, where Redis passes only
   * the first member's length when creating the key, and nothing for an
   * existing one.
   */
  readonly longestMemberHint: boolean
  /** Redis 7.0+: a listpack stores an integral score as an integer. */
  readonly integerScores: boolean
}

export const DEFAULT_ZSET_ENCODING_RULES: ZsetEncodingRules = {
  maxEntries: 128,
  maxValue: 64,
  sizeHint: true,
  longestMemberHint: false,
  integerScores: true,
}

/** The encoding a real server would hold `zset` in. */
export function zsetEncoding(zset: RedisSortedSetData): ZsetEncoding {
  return zset.encoding ?? 'listpack'
}

/**
 * ZADD's (and so ZINCRBY's and GEOADD's) set-up before adding `members`
 * (one per pair, in argument order): `zsetTypeCreate()` for a new, empty
 * set, `zsetTypeMaybeConvert()` for an existing one.
 */
export function prepareZsetForAdd(
  zset: RedisSortedSetData,
  members: readonly Buffer[],
  rules: ZsetEncodingRules,
): void {
  const pairs = members.length
  let longest = 0
  for (const member of members) longest = Math.max(longest, member.length)

  if (zset.members.size === 0) {
    // 6.2 / 7.0 / Redis: the first member's length; Valkey 8.0: the longest.
    const lengthHint = rules.longestMemberHint
      ? longest
      : (members[0]?.length ?? 0)
    const tooMany = rules.sizeHint ? pairs > rules.maxEntries : false
    zset.encoding =
      rules.maxEntries === 0 || tooMany || lengthHint > rules.maxValue
        ? 'skiplist'
        : 'listpack'
    return
  }

  if (
    zsetEncoding(zset) === 'listpack' &&
    rules.sizeHint &&
    (pairs > rules.maxEntries ||
      (rules.longestMemberHint && longest > rules.maxValue))
  ) {
    zset.encoding = 'skiplist'
  }
}

/**
 * `zsetAdd()` writing `score` for `member`: a listpack about to receive a
 * new member it cannot hold converts first, and a listpack stores -0 as 0
 * from 7.0. Returns the score actually stored. Updating an existing member
 * never converts.
 */
export function writeZsetScore(
  zset: RedisSortedSetData,
  member: Buffer,
  score: number,
  rules: ZsetEncodingRules,
): number {
  const hex = member.toString('hex')
  if (
    zsetEncoding(zset) === 'listpack' &&
    !zset.members.has(hex) &&
    (zset.members.size + 1 > rules.maxEntries || member.length > rules.maxValue)
  ) {
    zset.encoding = 'skiplist'
  }
  const stored = listpackStoredScore(zset, score, rules)
  zset.members.set(hex, { member, score: stored })
  return stored
}

function listpackStoredScore(
  zset: RedisSortedSetData,
  score: number,
  rules: ZsetEncodingRules,
): number {
  return rules.integerScores &&
    zsetEncoding(zset) === 'listpack' &&
    Object.is(score, -0)
    ? 0
    : score
}

/**
 * The value ZUNIONSTORE / ZINTERSTORE / ZDIFFSTORE and the GEO STORE
 * variants write: built as a skiplist, then `zsetConvertToListpackIfNeeded()`
 * makes it a listpack when every member fits.
 */
export function storedZsetConvertedIfFits(
  members: Map<string, RedisSortedSetMember>,
  rules: ZsetEncodingRules,
): RedisSortedSetData {
  let fits = members.size <= rules.maxEntries
  for (const entry of members.values()) {
    if (!fits) break
    fits = entry.member.length <= rules.maxValue
  }
  const zset: RedisSortedSetData = {
    type: 'zset',
    members,
    encoding: fits ? 'listpack' : 'skiplist',
  }
  for (const [hex, entry] of members) {
    const stored = listpackStoredScore(zset, entry.score, rules)
    if (!Object.is(stored, entry.score)) {
      members.set(hex, { member: entry.member, score: stored })
    }
  }
  return zset
}

/**
 * The value ZRANGESTORE writes: `zsetTypeCreate(length, 0)` (a listpack
 * when the result count fits the entry limit, on every version), then
 * `zsetAdd()` for each member in range order.
 */
export function storedZsetByInsertion(
  entries: readonly RedisSortedSetMember[],
  rules: ZsetEncodingRules,
): RedisSortedSetData {
  const zset: RedisSortedSetData = {
    type: 'zset',
    members: new Map(),
    encoding: entries.length > rules.maxEntries ? 'skiplist' : 'listpack',
  }
  for (const entry of entries) {
    writeZsetScore(zset, entry.member, entry.score, rules)
  }
  return zset
}

/**
 * Entries in the order ZSCAN walks them: a listpack's (score, member) order,
 * or, for a skiplist, its hash table's — undefined on a real server, so the
 * mock keeps insertion order.
 */
export function zsetScanOrder(
  zset: RedisSortedSetData,
): RedisSortedSetMember[] {
  const entries = Array.from(zset.members.values())
  if (zsetEncoding(zset) === 'skiplist') return entries
  return entries.sort(
    (a, b) => a.score - b.score || Buffer.compare(a.member, b.member),
  )
}
