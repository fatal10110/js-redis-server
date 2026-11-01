// RDB primitives (rdb.c): lengths, strings (integer-encoded or LZF), times
// and doubles, written and read the way rdbSave* / rdbLoad* do.

import { RdbFormatError } from './format-error'
import { lzfCompress, lzfDecompress } from './lzf'
import { stringToInt64 } from './packed'

export { RdbFormatError }

const RDB_6BITLEN = 0
const RDB_14BITLEN = 1
const RDB_32BITLEN = 0x80
const RDB_64BITLEN = 0x81
const RDB_ENCVAL = 3
const RDB_ENC_INT8 = 0
const RDB_ENC_INT16 = 1
const RDB_ENC_INT32 = 2
const RDB_ENC_LZF = 3

export class RdbWriter {
  private readonly chunks: Buffer[] = []

  constructor(private readonly compression = true) {}

  byte(value: number): this {
    this.chunks.push(Buffer.from([value & 0xff]))
    return this
  }

  raw(data: Buffer): this {
    this.chunks.push(data)
    return this
  }

  /** `rdbSaveLen()`. */
  len(value: number | bigint): this {
    // A negative long long (a stream group's unknown entries_read) is saved
    // as its uint64 bit pattern.
    const v = BigInt.asUintN(64, BigInt(value))
    if (v < 1n << 6n) return this.byte((RDB_6BITLEN << 6) | Number(v))
    if (v < 1n << 14n) {
      const n = Number(v)
      return this.raw(Buffer.from([(RDB_14BITLEN << 6) | (n >> 8), n & 0xff]))
    }
    if (v <= 0xffffffffn) {
      const out = Buffer.alloc(5)
      out[0] = RDB_32BITLEN
      out.writeUInt32BE(Number(v), 1)
      return this.raw(out)
    }
    const out = Buffer.alloc(9)
    out[0] = RDB_64BITLEN
    out.writeBigUInt64BE(BigInt.asUintN(64, v), 1)
    return this.raw(out)
  }

  /** `rdbSaveRawString()`: integer encoding, else LZF, else verbatim. */
  string(s: Buffer): this {
    if (s.length <= 11) {
      const value = stringToInt64(s)
      if (value !== null) {
        if (value >= -128n && value <= 127n) {
          return this.raw(
            Buffer.from([0xc0 | RDB_ENC_INT8, Number(value) & 0xff]),
          )
        }
        if (value >= -32768n && value <= 32767n) {
          const out = Buffer.alloc(3)
          out[0] = 0xc0 | RDB_ENC_INT16
          out.writeInt16LE(Number(value), 1)
          return this.raw(out)
        }
        if (value >= -2147483648n && value <= 2147483647n) {
          const out = Buffer.alloc(5)
          out[0] = 0xc0 | RDB_ENC_INT32
          out.writeInt32LE(Number(value), 1)
          return this.raw(out)
        }
      }
    }
    if (this.compression && s.length > 20) {
      const compressed = lzfCompress(s, s.length - 4)
      if (compressed !== null) {
        this.byte(0xc0 | RDB_ENC_LZF)
        this.len(compressed.length)
        this.len(s.length)
        return this.raw(compressed)
      }
    }
    this.len(s.length)
    return this.raw(s)
  }

  /** `rdbSaveMillisecondTime()`: int64 little-endian. */
  millis(value: number | bigint): this {
    const out = Buffer.alloc(8)
    out.writeBigInt64LE(BigInt(value))
    return this.raw(out)
  }

  /** `rdbSaveBinaryDoubleValue()`. */
  binaryDouble(value: number): this {
    const out = Buffer.alloc(8)
    out.writeDoubleLE(value)
    return this.raw(out)
  }

  toBuffer(): Buffer {
    return Buffer.concat(this.chunks)
  }
}

export class RdbReader {
  private pos = 0

  constructor(private readonly data: Buffer) {}

  get offset(): number {
    return this.pos
  }

  bytes(n: number): Buffer {
    if (n < 0 || this.pos + n > this.data.length) throw new RdbFormatError()
    const out = this.data.subarray(this.pos, this.pos + n)
    this.pos += n
    return out
  }

  byte(): number {
    return this.bytes(1)[0]
  }

  /** `rdbLoadLenByRef()`: a length, or an encoded-string marker. */
  private lenOrEncoding(): { value: bigint; encoded: boolean } {
    const first = this.byte()
    const type = (first & 0xc0) >> 6
    if (type === RDB_ENCVAL)
      return { value: BigInt(first & 0x3f), encoded: true }
    if (type === RDB_6BITLEN)
      return { value: BigInt(first & 0x3f), encoded: false }
    if (type === RDB_14BITLEN) {
      return {
        value: BigInt(((first & 0x3f) << 8) | this.byte()),
        encoded: false,
      }
    }
    if (first === RDB_32BITLEN) {
      return { value: BigInt(this.bytes(4).readUInt32BE(0)), encoded: false }
    }
    if (first === RDB_64BITLEN) {
      return { value: this.bytes(8).readBigUInt64BE(0), encoded: false }
    }
    throw new RdbFormatError()
  }

  /** A plain length as a bigint (stream ids and counters). */
  bigLen(): bigint {
    const { value, encoded } = this.lenOrEncoding()
    if (encoded) throw new RdbFormatError()
    return value
  }

  /** A plain length that must fit the remaining input's scale. */
  len(): number {
    const value = this.bigLen()
    if (value > BigInt(Number.MAX_SAFE_INTEGER)) throw new RdbFormatError()
    return Number(value)
  }

  /** `rdbGenericLoadStringObject()`. */
  string(): Buffer {
    const { value, encoded } = this.lenOrEncoding()
    if (!encoded) {
      if (value > BigInt(this.data.length)) throw new RdbFormatError()
      return Buffer.from(this.bytes(Number(value)))
    }
    switch (Number(value)) {
      case RDB_ENC_INT8:
        return Buffer.from(String(this.bytes(1).readInt8(0)))
      case RDB_ENC_INT16:
        return Buffer.from(String(this.bytes(2).readInt16LE(0)))
      case RDB_ENC_INT32:
        return Buffer.from(String(this.bytes(4).readInt32LE(0)))
      case RDB_ENC_LZF: {
        const clen = this.len()
        const len = this.len()
        // A sanity cap: no real payload expands beyond 2^32 bytes.
        if (len > 0xffffffff) throw new RdbFormatError()
        const out = lzfDecompress(this.bytes(clen), len)
        if (out === null) throw new RdbFormatError()
        return out
      }
      default:
        throw new RdbFormatError()
    }
  }

  millis(): bigint {
    return this.bytes(8).readBigInt64LE(0)
  }

  binaryDouble(): number {
    return this.bytes(8).readDoubleLE(0)
  }

  /** `rdbLoadDoubleValue()`: the RDB_TYPE_ZSET text form. */
  textDouble(): number {
    const len = this.byte()
    if (len === 255) return -Infinity
    if (len === 254) return Infinity
    if (len === 253) return NaN
    return parseDoubleText(this.bytes(len))
  }
}

/** `strtod()` of a stored score: decimal text, `inf`, `-inf`, `nan`. */
export function parseDoubleText(text: Buffer): number {
  const s = text.toString('latin1').trim().toLowerCase()
  const unsigned = s.replace(/^[+-]/, '')
  const negative = s.startsWith('-')
  if (unsigned === 'inf' || unsigned === 'infinity') {
    return negative ? -Infinity : Infinity
  }
  if (unsigned === 'nan') return NaN
  if (s.length === 0 || !/^[+-]?(\d+\.?\d*|\.\d+)(e[+-]?\d+)?$/.test(s)) {
    throw new RdbFormatError()
  }
  return Number(s)
}
