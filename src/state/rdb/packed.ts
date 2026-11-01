// The packed blobs RDB embeds: listpack (Redis 7.0+ / Valkey), ziplist
// (Redis 6.2 and older payloads), intset, and zipmap (read only, from
// pre-2.6 payloads real servers still load). Encoders follow listpack.c /
// ziplist.c / intset.c for a structure built by appends, which is what DUMP
// of a freshly written key shows.

import { RdbFormatError } from './format-error'

/**
 * `string2ll()` (and listpack's `lpStringToInt64()`): the integer `value`
 * spells, or null. No sign but '-', no leading zeros, no "-0", int64 range,
 * at most `maxLength` bytes.
 */
export function stringToInt64(
  value: Uint8Array,
  maxLength = 20,
): bigint | null {
  const length = value.length
  if (length === 0 || length > maxLength) return null
  let i = 0
  if (value[0] === 0x2d) {
    i = 1
    if (length === 1) return null
  }
  if (length === 1 && value[0] === 0x30) return 0n
  if (value[i] < 0x31 || value[i] > 0x39) return null
  for (let j = i + 1; j < length; j++) {
    if (value[j] < 0x30 || value[j] > 0x39) return null
  }
  const parsed = BigInt(Buffer.from(value).toString('latin1'))
  if (parsed < -(2n ** 63n) || parsed > 2n ** 63n - 1n) return null
  return parsed
}

/** An element of a packed structure: bytes, or an integer it stores as one. */
export type PackedElement = Buffer | bigint

function intBytes(value: bigint, size: number): Buffer {
  const out = Buffer.alloc(size)
  let v = BigInt.asUintN(size * 8, value)
  for (let i = 0; i < size; i++) {
    out[i] = Number(v & 0xffn)
    v >>= 8n
  }
  return out
}

function readIntLE(buf: Buffer, offset: number, size: number): bigint {
  if (offset + size > buf.length) throw new RdbFormatError()
  let v = 0n
  for (let i = size - 1; i >= 0; i--) {
    v = (v << 8n) | BigInt(buf[offset + i])
  }
  return BigInt.asIntN(size * 8, v)
}

// ---------------------------------------------------------------------------
// listpack
// ---------------------------------------------------------------------------

const LP_HEADER_SIZE = 6
const LP_EOF = 0xff

function lpEncodeBacklen(length: number): Buffer {
  if (length <= 127) return Buffer.from([length])
  if (length < 16383) return Buffer.from([length >> 7, (length & 127) | 128])
  if (length < 2097151) {
    return Buffer.from([
      length >> 14,
      ((length >> 7) & 127) | 128,
      (length & 127) | 128,
    ])
  }
  if (length < 268435455) {
    return Buffer.from([
      length >> 21,
      ((length >> 14) & 127) | 128,
      ((length >> 7) & 127) | 128,
      (length & 127) | 128,
    ])
  }
  return Buffer.from([
    Math.floor(length / 2 ** 28),
    ((length >> 21) & 127) | 128,
    ((length >> 14) & 127) | 128,
    ((length >> 7) & 127) | 128,
    (length & 127) | 128,
  ])
}

function lpEncodeInteger(v: bigint): Buffer {
  if (v >= 0n && v <= 127n) return Buffer.from([Number(v)])
  if (v >= -4096n && v <= 4095n) {
    const u = Number(v < 0n ? (1n << 13n) + v : v)
    return Buffer.from([(u >> 8) | 0xc0, u & 0xff])
  }
  if (v >= -32768n && v <= 32767n) {
    return Buffer.concat([Buffer.from([0xf1]), intBytes(v, 2)])
  }
  if (v >= -8388608n && v <= 8388607n) {
    return Buffer.concat([Buffer.from([0xf2]), intBytes(v, 3)])
  }
  if (v >= -2147483648n && v <= 2147483647n) {
    return Buffer.concat([Buffer.from([0xf3]), intBytes(v, 4)])
  }
  return Buffer.concat([Buffer.from([0xf4]), intBytes(v, 8)])
}

function lpEncodeString(s: Buffer): Buffer {
  const len = s.length
  if (len < 64) return Buffer.concat([Buffer.from([0x80 | len]), s])
  if (len < 4096) {
    return Buffer.concat([Buffer.from([0xe0 | (len >> 8), len & 0xff]), s])
  }
  const header = Buffer.alloc(5)
  header[0] = 0xf0
  header.writeUInt32LE(len, 1)
  return Buffer.concat([header, s])
}

/** One listpack entry (encoding, data and backlen) as `lpAppend` writes it. */
export function listpackEntry(element: PackedElement): Buffer {
  let encoded: Buffer
  if (typeof element === 'bigint') {
    encoded = lpEncodeInteger(element)
  } else {
    const asInt = stringToInt64(element)
    encoded = asInt === null ? lpEncodeString(element) : lpEncodeInteger(asInt)
  }
  return Buffer.concat([encoded, lpEncodeBacklen(encoded.length)])
}

/** A listpack holding `elements` in order. */
export function encodeListpack(elements: readonly PackedElement[]): Buffer {
  return listpackFromEntries(elements.map(listpackEntry))
}

/** A listpack around entries already encoded with {@link listpackEntry}. */
export function listpackFromEntries(entries: readonly Buffer[]): Buffer {
  const body = Buffer.concat(entries as Buffer[])
  const total = LP_HEADER_SIZE + body.length + 1
  const header = Buffer.alloc(LP_HEADER_SIZE)
  header.writeUInt32LE(total, 0)
  header.writeUInt16LE(Math.min(entries.length, 65535), 4)
  return Buffer.concat([header, body, Buffer.from([LP_EOF])])
}

/** The decoded elements of a listpack: strings as bytes, integers as bigint. */
export function decodeListpack(lp: Buffer): PackedElement[] {
  if (lp.length < LP_HEADER_SIZE + 1) throw new RdbFormatError()
  if (lp.readUInt32LE(0) !== lp.length) throw new RdbFormatError()
  const declared = lp.readUInt16LE(4)
  const out: PackedElement[] = []
  let p = LP_HEADER_SIZE
  for (;;) {
    if (p >= lp.length) throw new RdbFormatError()
    const b = lp[p]
    if (b === LP_EOF) break
    let encLen: number
    let element: PackedElement
    const need = (n: number): void => {
      if (p + n > lp.length) throw new RdbFormatError()
    }
    if ((b & 0x80) === 0) {
      element = BigInt(b)
      encLen = 1
    } else if ((b & 0xc0) === 0x80) {
      const len = b & 0x3f
      need(1 + len)
      element = Buffer.from(lp.subarray(p + 1, p + 1 + len))
      encLen = 1 + len
    } else if ((b & 0xe0) === 0xc0) {
      need(2)
      let v = ((b & 0x1f) << 8) | lp[p + 1]
      if (v >= 1 << 12) v -= 1 << 13
      element = BigInt(v)
      encLen = 2
    } else if ((b & 0xf0) === 0xe0) {
      need(2)
      const len = ((b & 0x0f) << 8) | lp[p + 1]
      need(2 + len)
      element = Buffer.from(lp.subarray(p + 2, p + 2 + len))
      encLen = 2 + len
    } else if (b === 0xf0) {
      need(5)
      const len = lp.readUInt32LE(p + 1)
      need(5 + len)
      element = Buffer.from(lp.subarray(p + 5, p + 5 + len))
      encLen = 5 + len
    } else if (b >= 0xf1 && b <= 0xf4) {
      const size = { 0xf1: 2, 0xf2: 3, 0xf3: 4, 0xf4: 8 }[b] as number
      element = readIntLE(lp, p + 1, size)
      encLen = 1 + size
    } else {
      throw new RdbFormatError()
    }
    p += encLen + lpEncodeBacklen(encLen).length
    out.push(element)
  }
  if (p !== lp.length - 1) throw new RdbFormatError()
  if (declared !== 65535 && declared !== out.length) throw new RdbFormatError()
  return out
}

// ---------------------------------------------------------------------------
// ziplist
// ---------------------------------------------------------------------------

const ZIPLIST_HEADER_SIZE = 10
const ZIP_BIG_PREVLEN = 254

function zipEncodingAndData(element: Buffer): Buffer {
  // zipTryEncoding(): strings shorter than 32 bytes that string2ll accepts.
  const v = element.length < 32 ? stringToInt64(element) : null
  if (v !== null) {
    if (v >= 0n && v <= 12n) return Buffer.from([0xf1 + Number(v)])
    if (v >= -128n && v <= 127n) {
      return Buffer.concat([Buffer.from([0xfe]), intBytes(v, 1)])
    }
    if (v >= -32768n && v <= 32767n) {
      return Buffer.concat([Buffer.from([0xc0]), intBytes(v, 2)])
    }
    if (v >= -8388608n && v <= 8388607n) {
      return Buffer.concat([Buffer.from([0xf0]), intBytes(v, 3)])
    }
    if (v >= -2147483648n && v <= 2147483647n) {
      return Buffer.concat([Buffer.from([0xd0]), intBytes(v, 4)])
    }
    return Buffer.concat([Buffer.from([0xe0]), intBytes(v, 8)])
  }
  const len = element.length
  if (len <= 0x3f) return Buffer.concat([Buffer.from([len]), element])
  if (len <= 0x3fff) {
    return Buffer.concat([
      Buffer.from([0x40 | (len >> 8), len & 0xff]),
      element,
    ])
  }
  const header = Buffer.alloc(5)
  header[0] = 0x80
  header.writeUInt32BE(len, 1)
  return Buffer.concat([header, element])
}

/** A ziplist holding `elements` in order, as appends build it. */
export function encodeZiplist(elements: readonly Buffer[]): Buffer {
  const entries: Buffer[] = []
  let prevLen = 0
  let tail = ZIPLIST_HEADER_SIZE
  let offset = ZIPLIST_HEADER_SIZE
  for (const element of elements) {
    let prev: Buffer
    if (prevLen < ZIP_BIG_PREVLEN) {
      prev = Buffer.from([prevLen])
    } else {
      prev = Buffer.alloc(5)
      prev[0] = ZIP_BIG_PREVLEN
      prev.writeUInt32LE(prevLen, 1)
    }
    const entry = Buffer.concat([prev, zipEncodingAndData(element)])
    tail = offset
    offset += entry.length
    prevLen = entry.length
    entries.push(entry)
  }
  const header = Buffer.alloc(ZIPLIST_HEADER_SIZE)
  header.writeUInt32LE(offset + 1, 0)
  header.writeUInt32LE(tail, 4)
  header.writeUInt16LE(Math.min(entries.length, 65535), 8)
  return Buffer.concat([header, ...entries, Buffer.from([0xff])])
}

/** The bytes the next element would add to a ziplist (6.2's estimate). */
export function ziplistInsertOverhead(length: number): number {
  return (length < 254 ? 1 : 5) + (length < 64 ? 1 : length < 16384 ? 2 : 5)
}

/** The decoded elements of a ziplist. */
export function decodeZiplist(zl: Buffer): PackedElement[] {
  if (zl.length < ZIPLIST_HEADER_SIZE + 1) throw new RdbFormatError()
  if (zl.readUInt32LE(0) !== zl.length) throw new RdbFormatError()
  const declared = zl.readUInt16LE(8)
  const out: PackedElement[] = []
  let p = ZIPLIST_HEADER_SIZE
  const need = (n: number): void => {
    if (p + n > zl.length) throw new RdbFormatError()
  }
  for (;;) {
    need(1)
    if (zl[p] === 0xff) break
    p += zl[p] === ZIP_BIG_PREVLEN ? 5 : 1
    need(1)
    const enc = zl[p]
    const top = enc & 0xc0
    if (top === 0x00) {
      const len = enc & 0x3f
      need(1 + len)
      out.push(Buffer.from(zl.subarray(p + 1, p + 1 + len)))
      p += 1 + len
    } else if (top === 0x40) {
      need(2)
      const len = ((enc & 0x3f) << 8) | zl[p + 1]
      need(2 + len)
      out.push(Buffer.from(zl.subarray(p + 2, p + 2 + len)))
      p += 2 + len
    } else if (top === 0x80) {
      need(5)
      const len = zl.readUInt32BE(p + 1)
      need(5 + len)
      out.push(Buffer.from(zl.subarray(p + 5, p + 5 + len)))
      p += 5 + len
    } else if (enc >= 0xf1 && enc <= 0xfd) {
      out.push(BigInt((enc & 0x0f) - 1))
      p += 1
    } else {
      const size = ({ 0xfe: 1, 0xc0: 2, 0xf0: 3, 0xd0: 4, 0xe0: 8 } as const)[
        enc as 0xfe
      ] as number | undefined
      if (size === undefined) throw new RdbFormatError()
      out.push(readIntLE(zl, p + 1, size))
      p += 1 + size
    }
  }
  if (p !== zl.length - 1) throw new RdbFormatError()
  if (declared !== 65535 && declared !== out.length) throw new RdbFormatError()
  return out
}

// ---------------------------------------------------------------------------
// intset
// ---------------------------------------------------------------------------

/** An intset of `values` (sorted ascending, no duplicates). */
export function encodeIntset(values: readonly bigint[]): Buffer {
  let width = 2
  for (const v of values) {
    if (v < -2147483648n || v > 2147483647n) {
      width = 8
      break
    }
    if (v < -32768n || v > 32767n) width = 4
  }
  const header = Buffer.alloc(8)
  header.writeUInt32LE(width, 0)
  header.writeUInt32LE(values.length, 4)
  return Buffer.concat([header, ...values.map(v => intBytes(v, width))])
}

/** The members of an intset blob. */
export function decodeIntset(blob: Buffer): bigint[] {
  if (blob.length < 8) throw new RdbFormatError()
  const width = blob.readUInt32LE(0)
  const count = blob.readUInt32LE(4)
  if (width !== 2 && width !== 4 && width !== 8) throw new RdbFormatError()
  if (blob.length !== 8 + width * count) throw new RdbFormatError()
  const out: bigint[] = []
  for (let i = 0; i < count; i++) {
    out.push(readIntLE(blob, 8 + i * width, width))
  }
  return out
}

// ---------------------------------------------------------------------------
// zipmap (RDB_TYPE_HASH_ZIPMAP, read only)
// ---------------------------------------------------------------------------

/** The field / value pairs of a zipmap blob, flattened. */
export function decodeZipmap(zm: Buffer): Buffer[] {
  const out: Buffer[] = []
  let p = 1 // zmlen
  const need = (n: number): void => {
    if (p + n > zm.length) throw new RdbFormatError()
  }
  const readLen = (): number => {
    need(1)
    const b = zm[p]
    if (b < 254) {
      p += 1
      return b
    }
    if (b === 254) {
      need(5)
      const len = zm.readUInt32LE(p + 1)
      p += 5
      return len
    }
    throw new RdbFormatError()
  }
  for (;;) {
    need(1)
    if (zm[p] === 0xff) break
    const keyLen = readLen()
    need(keyLen)
    out.push(Buffer.from(zm.subarray(p, p + keyLen)))
    p += keyLen
    const valueLen = readLen()
    need(1)
    const free = zm[p]
    p += 1
    need(valueLen + free)
    out.push(Buffer.from(zm.subarray(p, p + valueLen)))
    p += valueLen + free
  }
  return out
}
