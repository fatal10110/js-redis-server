// DUMP / RESTORE payloads (cluster.c createDumpPayload / verifyDumpPayload):
//
//   <RDB type byte> <rdbSaveObject body> <RDB version, 2 bytes LE> <CRC-64, 8 bytes LE>
//
// The body is what each profile's rdb.c writes for the encoding a real
// server would hold the value in. The mock keeps an encoding only for sets
// (intset) and sorted sets (listpack / skiplist); for lists, hashes and the
// rest of the set encodings it is derived from the current content and the
// encoding limits in CONFIG, as a key written by plain appends would be held.
// A real key whose history converted it (a hash that once grew past
// hash-max-listpack-entries, a list trimmed back below a node, a stream with
// XDEL tombstones) may DUMP to other bytes on a real server; RESTORE reads
// every encoding either way. Reusable by MIGRATE.

import type { CompatibilityProfile } from '../../core/compatibility'
import {
  formatRedisDouble,
  type DoubleFormatProfile,
} from '../../core/double-format'
import type {
  RedisDataValue,
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
  RedisStreamPendingEntry,
  StreamId,
} from '../data-types'
import { convertToIntsetIfPossible, intsetValue } from '../set-encoding'
import {
  streamEstimateEntriesRead,
  type StreamLagRules,
} from '../stream-groups'
import { streamIdKey } from '../stream-ids'
import { zsetEncoding } from '../zset-encoding'
import { crc64 } from './crc64'
import { RdbFormatError } from './format-error'
import { parseDoubleText, RdbReader, RdbWriter } from './io'
import {
  decodeIntset,
  decodeListpack,
  decodeZiplist,
  decodeZipmap,
  encodeIntset,
  encodeListpack,
  encodeZiplist,
  listpackEntry,
  listpackFromEntries,
  ziplistInsertOverhead,
  type PackedElement,
} from './packed'

export { RdbFormatError }

/** RDB object type bytes (rdb.h). */
export const RDB_TYPE = Object.freeze({
  STRING: 0,
  LIST: 1,
  SET: 2,
  ZSET: 3,
  HASH: 4,
  ZSET_2: 5,
  HASH_ZIPMAP: 9,
  LIST_ZIPLIST: 10,
  SET_INTSET: 11,
  ZSET_ZIPLIST: 12,
  HASH_ZIPLIST: 13,
  LIST_QUICKLIST: 14,
  STREAM_LISTPACKS: 15,
  HASH_LISTPACK: 16,
  ZSET_LISTPACK: 17,
  LIST_QUICKLIST_2: 18,
  STREAM_LISTPACKS_2: 19,
  SET_LISTPACK: 20,
  STREAM_LISTPACKS_3: 21,
  /** Valkey 9.0: a hashtable hash whose fields carry expiry times. */
  VALKEY_HASH_2: 22,
  /** Redis 7.4+: a hashtable hash with field TTLs. */
  HASH_METADATA: 24,
  /** Redis 7.4+: a listpack hash with field TTLs. */
  HASH_LISTPACK_EX: 25,
})

/** How one compatibility profile writes and reads payloads. */
export type RdbFormat = {
  /** RDB_VERSION, the payload footer's version. */
  readonly version: number
  /** RDB 10 (Redis 7.0): listpacks replace ziplists (types 16-19). */
  readonly listpacks: boolean
  /** Redis / Valkey 7.2: listpack lists and sets, stream type 21. */
  readonly listpackSets: boolean
  /** A list element over the node size limit gets a PLAIN quicklist node. */
  readonly plainLargeListElements: boolean
  /** Hash field TTLs: Redis 7.4's types 24 / 25, Valkey 9.0's type 22. */
  readonly hashFieldExpiry: 'redis' | 'valkey' | null
  /**
   * Valkey 9.0 `rdbIsVersionAccepted()`: versions 1-11 and its own 80 only;
   * every other server accepts any version up to its own.
   */
  readonly foreignVersionRange: boolean
  /** How scores print into a ziplist / listpack (`d2string()`). */
  readonly doubles: DoubleFormatProfile
  /** Estimates a pre-7.0 group's entries_read the way XINFO does. */
  readonly streamLag: StreamLagRules
}

/** The payload format of `profile` (see the `rdb.version-*` gates). */
export function rdbFormatFor(profile: CompatibilityProfile): RdbFormat {
  const v80 = profile.has('rdb.version-80')
  const v12 = profile.has('rdb.version-12')
  const v11 = profile.has('rdb.version-11')
  const v10 = profile.has('rdb.version-10')
  return {
    version: v80 ? 80 : v12 ? 12 : v11 ? 11 : v10 ? 10 : 9,
    listpacks: v10,
    listpackSets: v11,
    plainLargeListElements: profile.has('list.large-element-plain-node'),
    hashFieldExpiry: v80 ? 'valkey' : v12 ? 'redis' : null,
    foreignVersionRange: v80,
    doubles: profile,
    streamLag: {
      tombstoneCheck: profile.has('stream.group-lag-tombstone-check'),
      readFirstIdCheck: profile.has('stream.group-lag-read-first-id-check'),
      shortcuts: profile.has('stream.group-lag-shortcuts'),
    },
  }
}

/** The CONFIG limits that decide a value's encoding. */
export type EncodingLimits = {
  readonly hashMaxListpackEntries: number
  readonly hashMaxListpackValue: number
  readonly listMaxListpackSize: number
  readonly setMaxIntsetEntries: number
  readonly setMaxListpackEntries: number
  readonly setMaxListpackValue: number
  readonly zsetMaxListpackEntries: number
  readonly zsetMaxListpackValue: number
  readonly streamNodeMaxBytes: number
  readonly streamNodeMaxEntries: number
}

export const DEFAULT_ENCODING_LIMITS: EncodingLimits = Object.freeze({
  hashMaxListpackEntries: 512,
  hashMaxListpackValue: 64,
  listMaxListpackSize: -2,
  setMaxIntsetEntries: 512,
  setMaxListpackEntries: 128,
  setMaxListpackValue: 64,
  zsetMaxListpackEntries: 128,
  zsetMaxListpackValue: 64,
  streamNodeMaxBytes: 4096,
  streamNodeMaxEntries: 100,
})

// ---------------------------------------------------------------------------
// Payload framing
// ---------------------------------------------------------------------------

/** DUMP's reply for `value`: type, body, version and checksum. */
export function createDumpPayload(
  value: RedisDataValue,
  format: RdbFormat,
  limits: EncodingLimits = DEFAULT_ENCODING_LIMITS,
): Buffer {
  const body = serializeValue(value, format, limits)
  const version = Buffer.alloc(2)
  version.writeUInt16LE(format.version)
  const signed = Buffer.concat([body, version])
  return Buffer.concat([signed, crc64(signed)])
}

/**
 * `verifyDumpPayload()`: the type byte and body of `payload` when its footer
 * carries a version `format` accepts and a matching checksum, else null
 * (`DUMP payload version or checksum are wrong`).
 */
export function verifyDumpPayload(
  payload: Buffer,
  format: RdbFormat,
): Buffer | null {
  if (payload.length < 10) return null
  const footer = payload.length - 10
  const version = payload.readUInt16LE(footer)
  const accepted = format.foreignVersionRange
    ? (version >= 1 && version <= 11) || version === format.version
    : version <= format.version
  if (!accepted) return null
  const expected = crc64(payload.subarray(0, payload.length - 8))
  if (!expected.equals(payload.subarray(payload.length - 8))) return null
  return payload.subarray(0, footer)
}

/**
 * RESTORE's `rdbLoadObjectType()` + `rdbLoadObject()` of a verified body.
 * Throws {@link RdbFormatError} for anything a real server would refuse
 * (and, unlike a real server, for a truncated body, which real servers turn
 * into a key holding whatever parsed). Bytes after a complete value are
 * ignored, as real servers do.
 */
export function deserializeValue(
  body: Buffer,
  format: RdbFormat,
  limits: EncodingLimits = DEFAULT_ENCODING_LIMITS,
): RedisDataValue {
  const reader = new RdbReader(body)
  const type = reader.byte()
  if (!typeKnown(type, format)) throw new RdbFormatError()
  try {
    return loadObject(type, reader, limits, format)
  } catch (error) {
    if (error instanceof RdbFormatError) throw error
    // Buffer reads past the end, BigInt conversions and the like.
    throw new RdbFormatError()
  }
}

function typeKnown(type: number, format: RdbFormat): boolean {
  if ((type >= 0 && type <= 5) || (type >= 9 && type <= 15)) return true
  if (type >= 16 && type <= 19) return format.listpacks
  if (type === 20 || type === 21) return format.listpackSets
  if (type === RDB_TYPE.VALKEY_HASH_2)
    return format.hashFieldExpiry === 'valkey'
  if (type === RDB_TYPE.HASH_METADATA || type === RDB_TYPE.HASH_LISTPACK_EX) {
    return format.hashFieldExpiry === 'redis'
  }
  return false
}

// ---------------------------------------------------------------------------
// Serialize
// ---------------------------------------------------------------------------

/** The type byte and `rdbSaveObject()` body of `value`. */
export function serializeValue(
  value: RedisDataValue,
  format: RdbFormat,
  limits: EncodingLimits = DEFAULT_ENCODING_LIMITS,
): Buffer {
  const w = new RdbWriter()
  switch (value.type) {
    case 'string':
      w.byte(RDB_TYPE.STRING).string(value.value)
      break
    case 'list':
      writeList(w, value, format, limits)
      break
    case 'set':
      writeSet(w, value, format, limits)
      break
    case 'zset':
      writeZset(w, value, format)
      break
    case 'hash':
      writeHash(w, value, format, limits)
      break
    case 'stream':
      writeStream(w, value, format, limits)
      break
  }
  return w.toBuffer()
}

const OPTIMIZATION_LEVEL = [4096, 8192, 16384, 32768, 65536]
const SIZE_SAFETY_LIMIT = 8192
const SIZE_ESTIMATE_OVERHEAD = 8

/** `quicklistNodeExceedsLimit()` for `list-max-listpack-size` `fill`. */
function nodeExceedsLimit(fill: number, size: number, count: number): boolean {
  if (fill < 0) {
    return size > OPTIMIZATION_LEVEL[Math.min(-fill - 1, 4)]
  }
  return size > SIZE_SAFETY_LIMIT || count > fill
}

/**
 * The quicklist nodes RPUSH builds for `values`: an element joins the tail
 * node while `_quicklistNodeAllowInsert()` lets it (the node's bytes, plus
 * the element, plus 6.2's ziplist / 7.0's fixed overhead estimate, within
 * the fill limit). From 7.2 a list starts as one plain listpack and turns
 * into a quicklist (that listpack becoming the first node) only when the
 * next push would exceed the limit (`listTypeTryConvertListpack()`).
 */
type QuicklistNode = { plain: boolean; values: Buffer[] }

function quicklistNodes(
  values: readonly Buffer[],
  format: RdbFormat,
  fill: number,
): QuicklistNode[] {
  const nodes: QuicklistNode[] = []
  let node: QuicklistNode | null = null
  let nodeBytes = 0
  let prevEntry = 0
  let listpackPhase = format.listpackSets
  const emptyBytes = format.listpacks ? 7 : 11

  for (const value of values) {
    const len = value.length
    let join = false
    if (node && listpackPhase) {
      join = !nodeExceedsLimit(fill, nodeBytes + len, node.values.length + 1)
      // Converted: the listpack is now the tail node, which the push tries
      // with the quicklist estimate below.
      if (!join) listpackPhase = false
    } else if (listpackPhase && nodeExceedsLimit(fill, emptyBytes + len, 1)) {
      listpackPhase = false
    }
    const large =
      format.plainLargeListElements &&
      (fill < 0
        ? len > OPTIMIZATION_LEVEL[Math.min(-fill - 1, 4)]
        : len > SIZE_SAFETY_LIMIT)
    if (large) {
      nodes.push({ plain: true, values: [value] })
      node = null
      continue
    }
    if (node && !node.plain && !join && !listpackPhase) {
      const estimate = format.listpacks
        ? nodeBytes + len + SIZE_ESTIMATE_OVERHEAD
        : nodeBytes + len + ziplistInsertOverhead(len)
      join = !nodeExceedsLimit(fill, estimate, node.values.length + 1)
    }
    if (!node || !join) {
      node = { plain: false, values: [] }
      nodes.push(node)
      nodeBytes = emptyBytes
      prevEntry = 0
    }
    node.values.push(value)
    if (format.listpacks) {
      nodeBytes += listpackEntry(value).length
    } else {
      const entry = ziplistEntryLength(value, prevEntry)
      nodeBytes += entry
      prevEntry = entry
    }
  }
  return nodes
}

function ziplistEntryLength(value: Buffer, prevEntry: number): number {
  // prevlen + encoding + data, as encodeZiplist() lays the entry out.
  return encodeZiplist([value]).length - 11 + (prevEntry < 254 ? 0 : 4)
}

function writeList(
  w: RdbWriter,
  list: RedisListData,
  format: RdbFormat,
  limits: EncodingLimits,
): void {
  const nodes = quicklistNodes(list.values, format, limits.listMaxListpackSize)
  if (!format.listpacks) {
    w.byte(RDB_TYPE.LIST_QUICKLIST).len(nodes.length)
    for (const node of nodes) w.string(encodeZiplist(node.values))
    return
  }
  w.byte(RDB_TYPE.LIST_QUICKLIST_2).len(nodes.length)
  for (const node of nodes) {
    if (node.plain) {
      w.len(1).string(node.values[0]) // QUICKLIST_NODE_CONTAINER_PLAIN
    } else {
      w.len(2).string(encodeListpack(node.values)) // ..._PACKED
    }
  }
}

function writeSet(
  w: RdbWriter,
  set: RedisSetData,
  format: RdbFormat,
  limits: EncodingLimits,
): void {
  const members = Array.from(set.members.values())
  if (set.intset) {
    const values = members.map(member => intsetValue(member) ?? 0n)
    values.sort((a, b) => (a < b ? -1 : a > b ? 1 : 0))
    w.byte(RDB_TYPE.SET_INTSET).string(encodeIntset(values))
    return
  }
  if (
    format.listpackSets &&
    members.length <= limits.setMaxListpackEntries &&
    members.every(member => member.length <= limits.setMaxListpackValue)
  ) {
    w.byte(RDB_TYPE.SET_LISTPACK).string(encodeListpack(members))
    return
  }
  w.byte(RDB_TYPE.SET).len(members.length)
  for (const member of members) w.string(member)
}

const LLONG_HALF = 2 ** 62

/**
 * A score as `zzlInsertAt()` stores it: 7.0+ appends an integral score
 * within ±2^62 (`double2ll()`) as an integer, anything else as `d2string()`
 * text; 6.2's ziplist stores the text, integers only within ±(2^52 - 1).
 */
function scoreElement(score: number, format: RdbFormat): PackedElement {
  if (format.listpacks) {
    if (Number.isInteger(score) && Math.abs(score) <= LLONG_HALF) {
      return BigInt(score)
    }
    return Buffer.from(formatRedisDouble(score, format.doubles))
  }
  if (score === 0) return Buffer.from(Object.is(score, -0) ? '-0' : '0')
  if (Number.isInteger(score) && Math.abs(score) < 2 ** 52) {
    return Buffer.from(BigInt(score).toString())
  }
  return Buffer.from(formatRedisDouble(score, format.doubles))
}

function sortedMembers(zset: RedisSortedSetData): RedisSortedSetMember[] {
  return Array.from(zset.members.values()).sort(
    (a, b) =>
      (a.score < b.score ? -1 : a.score > b.score ? 1 : 0) ||
      Buffer.compare(a.member, b.member),
  )
}

function writeZset(
  w: RdbWriter,
  zset: RedisSortedSetData,
  format: RdbFormat,
): void {
  const members = sortedMembers(zset)
  if (zsetEncoding(zset) === 'listpack') {
    const elements: PackedElement[] = []
    for (const { member, score } of members) {
      elements.push(member, scoreElement(score, format))
    }
    if (format.listpacks) {
      w.byte(RDB_TYPE.ZSET_LISTPACK).string(encodeListpack(elements))
    } else {
      const texts = elements.map(e =>
        typeof e === 'bigint' ? Buffer.from(e.toString()) : e,
      )
      w.byte(RDB_TYPE.ZSET_ZIPLIST).string(encodeZiplist(texts))
    }
    return
  }
  // ZSET_2 walks the skiplist from the tail: highest score first.
  w.byte(RDB_TYPE.ZSET_2).len(members.length)
  for (let i = members.length - 1; i >= 0; i--) {
    w.string(members[i].member).binaryDouble(members[i].score)
  }
}

function writeHash(
  w: RdbWriter,
  hash: RedisHashData,
  format: RdbFormat,
  limits: EncodingLimits,
): void {
  const fields = Array.from(hash.fields.values())
  const fitsListpack =
    fields.length <= limits.hashMaxListpackEntries &&
    fields.every(
      f =>
        f.field.length <= limits.hashMaxListpackValue &&
        f.value.length <= limits.hashMaxListpackValue,
    )
  const withTtl = fields.some(f => f.expiresAt !== undefined)

  if (withTtl && format.hashFieldExpiry === 'redis') {
    const minExpire = Math.min(
      ...fields.flatMap(f => (f.expiresAt === undefined ? [] : [f.expiresAt])),
    )
    if (fitsListpack) {
      // A listpackEx keeps fields ordered by expiry, those without one last.
      const timed = fields
        .filter(f => f.expiresAt !== undefined)
        .sort((a, b) => (a.expiresAt as number) - (b.expiresAt as number))
      const untimed = fields.filter(f => f.expiresAt === undefined)
      const elements: PackedElement[] = []
      for (const f of [...timed, ...untimed]) {
        elements.push(f.field, f.value, BigInt(f.expiresAt ?? 0))
      }
      w.byte(RDB_TYPE.HASH_LISTPACK_EX).millis(minExpire)
      w.string(encodeListpack(elements))
      return
    }
    w.byte(RDB_TYPE.HASH_METADATA).millis(minExpire).len(fields.length)
    for (const f of fields) {
      w.len(f.expiresAt === undefined ? 0 : f.expiresAt - minExpire + 1)
      w.string(f.field).string(f.value)
    }
    return
  }

  if (withTtl && format.hashFieldExpiry === 'valkey') {
    w.byte(RDB_TYPE.VALKEY_HASH_2).len(fields.length)
    for (const f of fields) {
      w.string(f.field)
        .string(f.value)
        .millis(f.expiresAt ?? -1)
    }
    return
  }

  if (fitsListpack) {
    const elements = fields.flatMap(f => [f.field, f.value])
    if (format.listpacks) {
      w.byte(RDB_TYPE.HASH_LISTPACK).string(encodeListpack(elements))
    } else {
      w.byte(RDB_TYPE.HASH_ZIPLIST).string(encodeZiplist(elements))
    }
    return
  }
  w.byte(RDB_TYPE.HASH).len(fields.length)
  for (const f of fields) w.string(f.field).string(f.value)
}

const STREAM_ITEM_FLAG_DELETED = 1
const STREAM_ITEM_FLAG_SAMEFIELDS = 2
const STREAM_LISTPACK_MAX_SIZE = 1 << 30

function streamIdBytes(id: StreamId): Buffer {
  const out = Buffer.alloc(16)
  out.writeBigUInt64BE(id.ms, 0)
  out.writeBigUInt64BE(id.seq, 8)
  return out
}

function readStreamId(bytes: Buffer): StreamId {
  if (bytes.length !== 16) throw new RdbFormatError()
  return { ms: bytes.readBigUInt64BE(0), seq: bytes.readBigUInt64BE(8) }
}

function compareBytes(a: Buffer, b: Buffer): number {
  return Buffer.compare(a, b)
}

function compareIds(a: StreamId, b: StreamId): number {
  if (a.ms !== b.ms) return a.ms < b.ms ? -1 : 1
  if (a.seq !== b.seq) return a.seq < b.seq ? -1 : 1
  return 0
}

type StreamNode = {
  master: StreamId
  masterFields: Buffer[]
  count: number
  entries: Buffer[]
  bytes: number
}

/** The listpack nodes `streamAppendItem()` builds for `entries`. */
function streamNodes(
  entries: readonly RedisStreamEntry[],
  limits: EncodingLimits,
): StreamNode[] {
  let maxBytes = limits.streamNodeMaxBytes
  if (maxBytes === 0 || maxBytes > STREAM_LISTPACK_MAX_SIZE) {
    maxBytes = STREAM_LISTPACK_MAX_SIZE
  }
  const nodes: StreamNode[] = []
  let node: StreamNode | null = null
  for (const entry of entries) {
    const pairs = entry.fields.length / 2
    const totelelen = entry.fields.reduce((sum, f) => sum + f.length, 0)
    if (
      node &&
      (node.bytes + totelelen >= maxBytes ||
        (limits.streamNodeMaxEntries > 0 &&
          node.count >= limits.streamNodeMaxEntries))
    ) {
      node = null
    }
    if (!node) {
      const masterFields = entry.fields.filter((_, i) => i % 2 === 0)
      const head = [
        listpackEntry(1n),
        listpackEntry(0n),
        listpackEntry(BigInt(pairs)),
        ...masterFields.map(f => listpackEntry(f)),
        listpackEntry(0n),
      ]
      node = {
        master: entry.id,
        masterFields,
        count: 0,
        entries: head,
        bytes: 0,
      }
      nodes.push(node)
    }
    const same =
      node.masterFields.length === pairs &&
      node.masterFields.every((f, i) => f.equals(entry.fields[i * 2]))
    const parts: PackedElement[] = [
      BigInt(same ? STREAM_ITEM_FLAG_SAMEFIELDS : 0),
      entry.id.ms - node.master.ms,
      BigInt.asIntN(64, entry.id.seq - node.master.seq),
    ]
    if (!same) parts.push(BigInt(pairs))
    for (let i = 0; i < pairs; i++) {
      if (!same) parts.push(entry.fields[i * 2])
      parts.push(entry.fields[i * 2 + 1])
    }
    parts.push(BigInt(same ? pairs + 3 : pairs * 2 + 4))
    node.entries.push(...parts.map(listpackEntry))
    node.count += 1
    node.entries[0] = listpackEntry(BigInt(node.count))
    node.bytes =
      7 + node.entries.reduce((sum, encoded) => sum + encoded.length, 0)
  }
  return nodes
}

function writeStream(
  w: RdbWriter,
  stream: RedisStreamData,
  format: RdbFormat,
  limits: EncodingLimits,
): void {
  const version = format.listpackSets ? 3 : format.listpacks ? 2 : 1
  w.byte(
    version === 3
      ? RDB_TYPE.STREAM_LISTPACKS_3
      : version === 2
        ? RDB_TYPE.STREAM_LISTPACKS_2
        : RDB_TYPE.STREAM_LISTPACKS,
  )
  const nodes = streamNodes(stream.entries, limits)
  w.len(nodes.length)
  for (const node of nodes) {
    w.string(streamIdBytes(node.master))
    w.string(listpackFromEntries(node.entries))
  }
  w.len(stream.entries.length)
  w.len(stream.lastId.ms).len(stream.lastId.seq)
  if (version >= 2) {
    const first = stream.entries[0]?.id ?? { ms: 0n, seq: 0n }
    w.len(first.ms).len(first.seq)
    w.len(stream.maxDeletedEntryId.ms).len(stream.maxDeletedEntryId.seq)
    w.len(stream.entriesAdded)
  }

  const groups = Array.from(stream.groups.values()).sort((a, b) =>
    compareBytes(a.name, b.name),
  )
  w.len(groups.length)
  for (const group of groups) {
    w.string(group.name)
    w.len(group.lastDeliveredId.ms).len(group.lastDeliveredId.seq)
    if (version >= 2) w.len(group.entriesRead ?? -1n)

    const pending = Array.from(group.pending.values()).sort((a, b) =>
      compareIds(a.id, b.id),
    )
    w.len(pending.length)
    for (const p of pending) {
      w.raw(streamIdBytes(p.id)).millis(p.deliveredAt).len(p.deliveryCount)
    }

    const consumers = Array.from(group.consumers.entries()).sort((a, b) =>
      compareBytes(a[1].name, b[1].name),
    )
    w.len(consumers.length)
    for (const [consumerId, consumer] of consumers) {
      w.string(consumer.name).millis(consumer.seenAt)
      if (version >= 3) w.millis(consumer.activeAt ?? -1)
      const owned = pending.filter(p => p.consumerId === consumerId)
      w.len(owned.length)
      for (const p of owned) w.raw(streamIdBytes(p.id))
    }
  }
}

// ---------------------------------------------------------------------------
// Deserialize
// ---------------------------------------------------------------------------

function elementBytes(element: PackedElement): Buffer {
  return typeof element === 'bigint' ? Buffer.from(element.toString()) : element
}

function loadObject(
  type: number,
  r: RdbReader,
  limits: EncodingLimits,
  format: RdbFormat,
): RedisDataValue {
  switch (type) {
    case RDB_TYPE.STRING:
      return { type: 'string', value: r.string() }

    case RDB_TYPE.LIST: {
      const n = r.len()
      const values: Buffer[] = []
      for (let i = 0; i < n; i++) values.push(r.string())
      return buildList(values)
    }
    case RDB_TYPE.LIST_ZIPLIST:
      return buildList(decodeZiplist(r.string()).map(elementBytes))
    case RDB_TYPE.LIST_QUICKLIST:
    case RDB_TYPE.LIST_QUICKLIST_2: {
      const n = r.len()
      const values: Buffer[] = []
      for (let i = 0; i < n; i++) {
        if (type === RDB_TYPE.LIST_QUICKLIST) {
          values.push(...decodeZiplist(r.string()).map(elementBytes))
          continue
        }
        const container = r.len()
        if (container === 1) {
          values.push(r.string()) // QUICKLIST_NODE_CONTAINER_PLAIN
        } else if (container === 2) {
          values.push(...decodeListpack(r.string()).map(elementBytes))
        } else {
          throw new RdbFormatError()
        }
      }
      return buildList(values)
    }

    case RDB_TYPE.SET: {
      const n = r.len()
      const members: Buffer[] = []
      for (let i = 0; i < n; i++) members.push(r.string())
      const set = buildSet(members)
      convertToIntsetIfPossible(set, {
        maxIntsetEntries: limits.setMaxIntsetEntries,
        listpack: true,
      })
      return set
    }
    case RDB_TYPE.SET_INTSET: {
      const values = decodeIntset(r.string())
      const set = buildSet(values.map(v => Buffer.from(v.toString())))
      set.intset = values.length <= limits.setMaxIntsetEntries
      return set
    }
    case RDB_TYPE.SET_LISTPACK:
      return buildSet(decodeListpack(r.string()).map(elementBytes))

    case RDB_TYPE.ZSET:
    case RDB_TYPE.ZSET_2: {
      const n = r.len()
      const pairs: RedisSortedSetMember[] = []
      for (let i = 0; i < n; i++) {
        const member = r.string()
        const score =
          type === RDB_TYPE.ZSET_2 ? r.binaryDouble() : r.textDouble()
        pairs.push({ member, score })
      }
      return buildZset(pairs, limits, true)
    }
    case RDB_TYPE.ZSET_ZIPLIST:
    case RDB_TYPE.ZSET_LISTPACK: {
      const blob = r.string()
      const elements =
        type === RDB_TYPE.ZSET_ZIPLIST
          ? decodeZiplist(blob)
          : decodeListpack(blob)
      if (elements.length % 2 !== 0) throw new RdbFormatError()
      const pairs: RedisSortedSetMember[] = []
      for (let i = 0; i < elements.length; i += 2) {
        const score = elements[i + 1]
        pairs.push({
          member: elementBytes(elements[i]),
          score:
            typeof score === 'bigint' ? Number(score) : parseDoubleText(score),
        })
      }
      return buildZset(pairs, limits, false)
    }

    case RDB_TYPE.HASH: {
      const n = r.len()
      const fields: RedisHashField[] = []
      for (let i = 0; i < n; i++) {
        fields.push({ field: r.string(), value: r.string() })
      }
      return buildHash(fields)
    }
    case RDB_TYPE.HASH_ZIPMAP:
      return buildHash(pairsOf(decodeZipmap(r.string())))
    case RDB_TYPE.HASH_ZIPLIST:
      return buildHash(pairsOf(decodeZiplist(r.string()).map(elementBytes)))
    case RDB_TYPE.HASH_LISTPACK:
      return buildHash(pairsOf(decodeListpack(r.string()).map(elementBytes)))
    case RDB_TYPE.HASH_LISTPACK_EX: {
      r.millis() // the minimum expiry, recomputed from the fields
      const elements = decodeListpack(r.string())
      if (elements.length % 3 !== 0) throw new RdbFormatError()
      const fields: RedisHashField[] = []
      for (let i = 0; i < elements.length; i += 3) {
        const ttl = elements[i + 2]
        if (typeof ttl !== 'bigint' || ttl < 0n) throw new RdbFormatError()
        fields.push({
          field: elementBytes(elements[i]),
          value: elementBytes(elements[i + 1]),
          expiresAt: ttl === 0n ? undefined : Number(ttl),
        })
      }
      return buildHash(fields)
    }
    case RDB_TYPE.HASH_METADATA: {
      const minExpire = r.millis()
      const n = r.len()
      const fields: RedisHashField[] = []
      for (let i = 0; i < n; i++) {
        const ttl = r.bigLen()
        const field = r.string()
        const value = r.string()
        fields.push({
          field,
          value,
          expiresAt: ttl === 0n ? undefined : Number(minExpire + ttl - 1n),
        })
      }
      return buildHash(fields)
    }
    case RDB_TYPE.VALKEY_HASH_2: {
      const n = r.len()
      const fields: RedisHashField[] = []
      for (let i = 0; i < n; i++) {
        const field = r.string()
        const value = r.string()
        const expiry = r.millis()
        if (expiry < -1n) throw new RdbFormatError()
        fields.push({
          field,
          value,
          expiresAt: expiry === -1n ? undefined : Number(expiry),
        })
      }
      return buildHash(fields)
    }

    case RDB_TYPE.STREAM_LISTPACKS:
    case RDB_TYPE.STREAM_LISTPACKS_2:
    case RDB_TYPE.STREAM_LISTPACKS_3:
      return loadStream(
        r,
        format,
        type === RDB_TYPE.STREAM_LISTPACKS_3
          ? 3
          : type === RDB_TYPE.STREAM_LISTPACKS_2
            ? 2
            : 1,
      )

    default:
      throw new RdbFormatError()
  }
}

function pairsOf(elements: readonly Buffer[]): RedisHashField[] {
  if (elements.length % 2 !== 0) throw new RdbFormatError()
  const fields: RedisHashField[] = []
  for (let i = 0; i < elements.length; i += 2) {
    fields.push({ field: elements[i], value: elements[i + 1] })
  }
  return fields
}

function buildList(values: Buffer[]): RedisListData {
  if (values.length === 0) throw new RdbFormatError()
  return { type: 'list', values }
}

function buildSet(members: readonly Buffer[]): RedisSetData {
  if (members.length === 0) throw new RdbFormatError()
  const map = new Map<string, Buffer>()
  for (const member of members) {
    const hex = member.toString('hex')
    if (map.has(hex)) throw new RdbFormatError()
    map.set(hex, member)
  }
  return { type: 'set', members: map, intset: false }
}

/**
 * A sorted set as `rdbLoadObject()` leaves it: a loaded skiplist becomes a
 * listpack when it fits both limits; a loaded listpack stays one unless it
 * holds more entries than `zset-max-listpack-entries`.
 */
function buildZset(
  pairs: readonly RedisSortedSetMember[],
  limits: EncodingLimits,
  fromSkiplist: boolean,
): RedisSortedSetData {
  if (pairs.length === 0) throw new RdbFormatError()
  const members = new Map<string, RedisSortedSetMember>()
  let longest = 0
  for (const { member, score } of pairs) {
    if (Number.isNaN(score)) throw new RdbFormatError()
    const hex = member.toString('hex')
    if (members.has(hex)) throw new RdbFormatError()
    members.set(hex, { member, score })
    longest = Math.max(longest, member.length)
  }
  const tooMany = members.size > limits.zsetMaxListpackEntries
  const tooLong = fromSkiplist && longest > limits.zsetMaxListpackValue
  return {
    type: 'zset',
    members,
    encoding: tooMany || tooLong ? 'skiplist' : 'listpack',
  }
}

function buildHash(fields: readonly RedisHashField[]): RedisHashData {
  if (fields.length === 0) throw new RdbFormatError()
  const map = new Map<string, RedisHashField>()
  for (const field of fields) {
    const hex = field.field.toString('hex')
    if (map.has(hex)) throw new RdbFormatError()
    map.set(hex, field)
  }
  return { type: 'hash', fields: map }
}

function asInt(element: PackedElement | undefined): bigint {
  if (typeof element !== 'bigint') throw new RdbFormatError()
  return element
}

function loadStream(
  r: RdbReader,
  format: RdbFormat,
  version: 1 | 2 | 3,
): RedisStreamData {
  const entries: RedisStreamEntry[] = []
  const nodeCount = r.len()
  for (let n = 0; n < nodeCount; n++) {
    const master = readStreamId(r.string())
    const lp = decodeListpack(r.string())
    if (lp.length === 0) throw new RdbFormatError()
    const masterFieldCount = Number(asInt(lp[2]))
    const masterFields = lp.slice(3, 3 + masterFieldCount).map(elementBytes)
    if (masterFields.length !== masterFieldCount) throw new RdbFormatError()
    let i = 3 + masterFieldCount
    if (asInt(lp[i]) !== 0n) throw new RdbFormatError()
    i += 1
    while (i < lp.length) {
      const flags = Number(asInt(lp[i]))
      const id: StreamId = {
        ms: BigInt.asUintN(64, master.ms + asInt(lp[i + 1])),
        seq: BigInt.asUintN(64, master.seq + asInt(lp[i + 2])),
      }
      i += 3
      const fields: Buffer[] = []
      if (flags & STREAM_ITEM_FLAG_SAMEFIELDS) {
        for (const field of masterFields) {
          if (i >= lp.length) throw new RdbFormatError()
          fields.push(field, elementBytes(lp[i++]))
        }
      } else {
        const pairs = Number(asInt(lp[i++]))
        for (let p = 0; p < pairs; p++) {
          if (i + 1 >= lp.length) throw new RdbFormatError()
          fields.push(elementBytes(lp[i]), elementBytes(lp[i + 1]))
          i += 2
        }
      }
      asInt(lp[i++]) // lp-count
      if (!(flags & STREAM_ITEM_FLAG_DELETED)) entries.push({ id, fields })
    }
  }

  const length = r.len()
  if (length !== entries.length) throw new RdbFormatError()
  const lastId: StreamId = { ms: r.bigLen(), seq: r.bigLen() }
  let maxDeletedEntryId: StreamId = { ms: 0n, seq: 0n }
  let entriesAdded = BigInt(length)
  if (version >= 2) {
    r.bigLen() // first_id, recomputed from the entries
    r.bigLen()
    maxDeletedEntryId = { ms: r.bigLen(), seq: r.bigLen() }
    entriesAdded = r.bigLen()
  }

  const stream: RedisStreamData = {
    type: 'stream',
    entries,
    lastId,
    entriesAdded,
    maxDeletedEntryId,
    groups: new Map(),
  }

  const groupCount = r.len()
  for (let g = 0; g < groupCount; g++) {
    const name = r.string()
    const lastDeliveredId: StreamId = { ms: r.bigLen(), seq: r.bigLen() }
    let entriesRead: bigint | null
    if (version >= 2) {
      const raw = BigInt.asIntN(64, r.bigLen())
      entriesRead = raw < 0n ? null : raw
    } else {
      // Pre-7.0 payloads carry no counter: `rdbLoadObject()` estimates it.
      entriesRead = streamEstimateEntriesRead(
        stream,
        lastDeliveredId,
        format.streamLag,
      )
    }
    const pending = new Map<string, RedisStreamPendingEntry>()
    const pelCount = r.len()
    for (let p = 0; p < pelCount; p++) {
      const id = readStreamId(Buffer.from(r.bytes(16)))
      const deliveredAt = Number(r.millis())
      const deliveryCount = r.len()
      pending.set(streamIdKey(id), {
        id,
        consumerId: '',
        deliveredAt,
        deliveryCount,
      })
    }
    const consumers = new Map<string, RedisStreamConsumer>()
    const consumerCount = r.len()
    for (let c = 0; c < consumerCount; c++) {
      const consumerName = r.string()
      const consumerId = consumerName.toString('hex')
      const seenAt = Number(r.millis())
      const activeAt = version >= 3 ? Number(r.millis()) : seenAt
      consumers.set(consumerId, {
        name: consumerName,
        seenAt,
        activeAt: activeAt < 0 ? null : activeAt,
      })
      const owned = r.len()
      for (let p = 0; p < owned; p++) {
        const id = readStreamId(Buffer.from(r.bytes(16)))
        const entry = pending.get(streamIdKey(id))
        if (!entry) throw new RdbFormatError()
        entry.consumerId = consumerId
      }
    }
    for (const entry of pending.values()) {
      if (entry.consumerId === '') throw new RdbFormatError()
    }
    const group: RedisStreamConsumerGroup = {
      name,
      lastDeliveredId,
      entriesRead,
      consumers,
      pending,
    }
    stream.groups.set(name.toString('hex'), group)
  }
  return stream
}
