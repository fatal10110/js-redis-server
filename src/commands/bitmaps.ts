import { defineCommand } from '../core/command-definition'
import type { RedisExecutionContext } from '../core/redis-context'
import {
  isIntegerToken,
  t,
  type CommandSchema,
  type ParseContext,
} from '../core/command-schema'
import {
  BitOffsetError,
  BitOpNotSingleKeyError,
  BitPosBitError,
  BitValueError,
  BitfieldOverflowTypeError,
  BitfieldRoGetOnlyError,
  BitfieldTypeError,
  ExpectedIntegerError,
  RedisSyntaxError,
  WrongNumberOfArgumentsError,
} from '../core/redis-error'
import { RedisResult } from '../core/redis-result'
import { RedisValue } from '../core/redis-value'
import { ensureStringOrMissing, INT64_MAX, INT64_MIN, integer } from './helpers'

// The highest addressable bit. Redis caps a bit offset's *start byte* at
// proto-max-bulk-len (512MB) — i.e. the offset itself must be < 2^32 bits.
// A field may still spill one type-width past this, growing the buffer.
const MAX_BIT_OFFSET = 0x1_0000_0000

const EMPTY = Buffer.alloc(0)

// --- shared bit helpers ----------------------------------------------------

function getBit(buf: Buffer, bitIndex: number): number {
  const byteIndex = bitIndex >>> 3
  if (byteIndex >= buf.length) {
    return 0
  }
  return (buf[byteIndex]! >> (7 - (bitIndex & 7))) & 1
}

function byteAt(buf: Buffer, index: number): number {
  return index < buf.length ? buf[index]! : 0
}

function parseBitOffset(token: Buffer | undefined): number {
  if (!token) {
    throw new BitOffsetError()
  }
  const raw = token.toString()
  if (!isIntegerToken(raw)) {
    throw new BitOffsetError()
  }
  const value = Number(raw)
  if (!Number.isSafeInteger(value) || value < 0 || value >= MAX_BIT_OFFSET) {
    throw new BitOffsetError()
  }
  return value
}

function parseRangeIndex(token: Buffer): number {
  const raw = token.toString()
  if (!isIntegerToken(raw)) {
    throw new ExpectedIntegerError()
  }
  const value = Number(raw)
  if (!Number.isSafeInteger(value)) {
    throw new ExpectedIntegerError()
  }
  return value
}

// Normalize a [start, end] range (Redis semantics, shared by BITCOUNT/BITPOS).
// `size` is the number of addressable units (bytes for BYTE mode, bits for BIT
// mode). Returns null when the range is empty.
function resolveRange(
  start: number,
  end: number,
  size: number,
): [first: number, last: number] | null {
  if (start < 0 && end < 0 && start > end) {
    return null
  }
  if (start < 0) start = size + start
  if (end < 0) end = size + end
  if (start < 0) start = 0
  if (end < 0) end = 0
  if (end >= size) end = size - 1
  if (start > end) {
    return null
  }
  return [start, end]
}

// Count set bits within the inclusive bit range [firstBit, lastBit].
// ponytail: naive per-bit scan, O(bits). A byte-wise popcount table would be
// faster for multi-MB bitmaps, but this mock isn't sized for those.
function countBits(buf: Buffer, firstBit: number, lastBit: number): number {
  let count = 0
  for (let i = firstBit; i <= lastBit; i++) {
    count += getBit(buf, i)
  }
  return count
}

// --- SETBIT / GETBIT -------------------------------------------------------

type SetBitArgs = { key: Buffer; offset: number; value: number }

export const setbitCommand = defineCommand({
  name: 'setbit',
  schema: t.custom(
    (input, index, ctx): { value: SetBitArgs; nextIndex: number } => {
      if (input.length - index !== 3) {
        throw new WrongNumberOfArgumentsError(ctx.commandName)
      }
      const offset = parseBitOffset(input[index + 1])
      const valueRaw = input[index + 2]!.toString()
      if (valueRaw !== '0' && valueRaw !== '1') {
        throw new BitValueError()
      }
      return {
        value: { key: input[index]!, offset, value: Number(valueRaw) },
        nextIndex: index + 3,
      }
    },
  ),
  flags: ['write', 'denyoom'],
  keys: args => [args.key],
  execute: (args, ctx) => {
    const existing = ensureStringOrMissing(ctx.db, args.key)
    const byteIndex = args.offset >>> 3
    const bitMask = 1 << (7 - (args.offset & 7))

    const current = existing ?? EMPTY
    const requiredSize = byteIndex + 1
    const target =
      requiredSize > current.length ? Buffer.alloc(requiredSize) : current
    if (target !== current) {
      current.copy(target, 0)
    }

    const oldBit = (target[byteIndex]! & bitMask) === 0 ? 0 : 1
    if (args.value === 1) {
      target[byteIndex]! |= bitMask
    } else {
      target[byteIndex]! &= ~bitMask
    }

    ctx.db.setString(args.key, target, { keepTtl: true })
    return integer(oldBit)
  },
})

type GetBitArgs = { key: Buffer; offset: number }

export const getbitCommand = defineCommand({
  name: 'getbit',
  schema: t.custom(
    (input, index, ctx): { value: GetBitArgs; nextIndex: number } => {
      if (input.length - index !== 2) {
        throw new WrongNumberOfArgumentsError(ctx.commandName)
      }
      return {
        value: { key: input[index]!, offset: parseBitOffset(input[index + 1]) },
        nextIndex: index + 2,
      }
    },
  ),
  flags: ['readonly', 'fast'],
  keys: args => [args.key],
  execute: (args, ctx) => {
    const existing = ensureStringOrMissing(ctx.db, args.key)
    if (!existing) {
      return integer(0)
    }
    return integer(getBit(existing, args.offset))
  },
})

// --- BITCOUNT --------------------------------------------------------------

type BitRange = { start: number; end: number; bit: boolean }
type BitCountArgs = { key: Buffer; range?: BitRange }

export const bitcountCommand = defineCommand({
  name: 'bitcount',
  schema: t.custom(
    (input, index, ctx): { value: BitCountArgs; nextIndex: number } => {
      const key = input[index]
      if (!key) {
        throw new WrongNumberOfArgumentsError(ctx.commandName)
      }
      const extra = input.length - index - 1
      if (extra === 0) {
        return { value: { key }, nextIndex: input.length }
      }
      if (extra !== 2 && extra !== 3) {
        throw new RedisSyntaxError()
      }
      const start = parseRangeIndex(input[index + 1]!)
      const end = parseRangeIndex(input[index + 2]!)
      const bit = parseRangeUnit(input[index + 3], extra === 3, ctx)
      return {
        value: { key, range: { start, end, bit } },
        nextIndex: input.length,
      }
    },
  ),
  flags: ['readonly'],
  keys: args => [args.key],
  execute: (args, ctx) => {
    const existing = ensureStringOrMissing(ctx.db, args.key)
    if (!existing || existing.length === 0) {
      return integer(0)
    }
    if (!args.range) {
      return integer(countBits(existing, 0, existing.length * 8 - 1))
    }

    const { start, end, bit } = args.range
    const size = bit ? existing.length * 8 : existing.length
    const resolved = resolveRange(start, end, size)
    if (!resolved) {
      return integer(0)
    }
    const [first, last] = resolved
    const firstBit = bit ? first : first * 8
    const lastBit = bit ? last : last * 8 + 7
    return integer(countBits(existing, firstBit, lastBit))
  },
})

// --- BITPOS ----------------------------------------------------------------

type BitPosArgs = {
  key: Buffer
  bit: number
  start?: number
  end?: number
  endGiven: boolean
  bitMode: boolean
}

export const bitposCommand = defineCommand({
  name: 'bitpos',
  schema: t.custom(
    (input, index, ctx): { value: BitPosArgs; nextIndex: number } => {
      const key = input[index]
      const bitToken = input[index + 1]
      if (!key || !bitToken) {
        throw new WrongNumberOfArgumentsError(ctx.commandName)
      }
      const bitRaw = bitToken.toString()
      if (bitRaw !== '0' && bitRaw !== '1') {
        throw new BitPosBitError()
      }
      const extra = input.length - index - 2
      if (extra > 3) {
        throw new RedisSyntaxError()
      }
      const args: BitPosArgs = {
        key,
        bit: Number(bitRaw),
        endGiven: extra >= 2,
        bitMode: false,
      }
      if (extra >= 1) {
        args.start = parseRangeIndex(input[index + 2]!)
      }
      if (extra >= 2) {
        args.end = parseRangeIndex(input[index + 3]!)
      }
      args.bitMode = parseRangeUnit(input[index + 4], extra === 3, ctx)
      return { value: args, nextIndex: input.length }
    },
  ),
  flags: ['readonly'],
  keys: args => [args.key],
  execute: (args, ctx) => {
    const existing = ensureStringOrMissing(ctx.db, args.key)
    if (!existing || existing.length === 0) {
      // Missing/empty key reads as all-zero: a clear bit is at position 0, a
      // set bit is never found.
      return integer(args.bit === 0 ? 0 : -1)
    }

    const totalBits = existing.length * 8
    let firstBit: number
    let lastBit: number
    if (args.start === undefined) {
      firstBit = 0
      lastBit = totalBits - 1
    } else {
      const size = args.bitMode ? totalBits : existing.length
      const end = args.end ?? -1
      const resolved = resolveRange(args.start, end, size)
      if (!resolved) {
        return integer(-1)
      }
      const [first, last] = resolved
      firstBit = args.bitMode ? first : first * 8
      lastBit = args.bitMode ? last : last * 8 + 7
    }

    for (let i = firstBit; i <= lastBit; i++) {
      if (getBit(existing, i) === args.bit) {
        return integer(i)
      }
    }
    // Not found. When looking for a clear bit without an explicit end, the
    // string is treated as zero-padded on the right, so the answer is the
    // first bit past the searched range.
    if (args.bit === 0 && !args.endGiven) {
      return integer(lastBit + 1)
    }
    return integer(-1)
  },
})

// Parse the optional BYTE|BIT modifier shared by BITCOUNT/BITPOS. Returns true
// for BIT mode, false for BYTE mode. The modifier is gated to Redis 7.0+; on
// older profiles an unrecognized trailing token is a plain syntax error.
function parseRangeUnit(
  token: Buffer | undefined,
  present: boolean,
  ctx: ParseContext,
): boolean {
  if (!present) {
    return false
  }
  if (!ctx.profile.has('bit.byte-bit-range')) {
    throw new RedisSyntaxError()
  }
  const unit = token!.toString().toUpperCase()
  if (unit === 'BYTE') return false
  if (unit === 'BIT') return true
  throw new RedisSyntaxError()
}

// --- BITOP -----------------------------------------------------------------

type BitOp = 'AND' | 'OR' | 'XOR' | 'NOT'
type BitOpArgs = { op: BitOp; destKey: Buffer; sourceKeys: Buffer[] }

export const bitopCommand = defineCommand({
  name: 'bitop',
  schema: t.custom(
    (input, index, ctx): { value: BitOpArgs; nextIndex: number } => {
      const opToken = input[index]
      const destKey = input[index + 1]
      const sourceKeys = input.slice(index + 2)
      if (!opToken || !destKey || sourceKeys.length === 0) {
        throw new WrongNumberOfArgumentsError(ctx.commandName)
      }
      const op = opToken.toString().toUpperCase()
      if (op !== 'AND' && op !== 'OR' && op !== 'XOR' && op !== 'NOT') {
        throw new RedisSyntaxError()
      }
      if (op === 'NOT' && sourceKeys.length !== 1) {
        throw new BitOpNotSingleKeyError()
      }
      return {
        value: { op, destKey, sourceKeys },
        nextIndex: input.length,
      }
    },
  ),
  flags: ['write', 'denyoom'],
  keys: args => [args.destKey, ...args.sourceKeys],
  execute: (args, ctx) => {
    const sources = args.sourceKeys.map(
      key => ensureStringOrMissing(ctx.db, key) ?? EMPTY,
    )
    const maxLen = sources.reduce((max, buf) => Math.max(max, buf.length), 0)

    if (maxLen === 0) {
      ctx.db.delete(args.destKey)
      return integer(0)
    }

    const result = Buffer.alloc(maxLen)
    if (args.op === 'NOT') {
      const src = sources[0]!
      for (let i = 0; i < maxLen; i++) {
        result[i] = ~byteAt(src, i) & 0xff
      }
    } else {
      for (let i = 0; i < maxLen; i++) {
        let acc = byteAt(sources[0]!, i)
        for (let j = 1; j < sources.length; j++) {
          const value = byteAt(sources[j]!, i)
          if (args.op === 'AND') acc &= value
          else if (args.op === 'OR') acc |= value
          else acc ^= value
        }
        result[i] = acc
      }
    }

    ctx.db.setString(args.destKey, result)
    return integer(maxLen)
  },
})

// --- BITFIELD / BITFIELD_RO ------------------------------------------------

type FieldType = { signed: boolean; bits: number }
type Overflow = 'WRAP' | 'SAT' | 'FAIL'
type BitFieldOp =
  | { kind: 'GET'; type: FieldType; offset: number }
  | {
      kind: 'SET'
      type: FieldType
      offset: number
      operand: bigint
      overflow: Overflow
    }
  | {
      kind: 'INCRBY'
      type: FieldType
      offset: number
      operand: bigint
      overflow: Overflow
    }
type BitFieldArgs = { key: Buffer; ops: BitFieldOp[] }

function parseFieldType(token: Buffer | undefined): FieldType {
  if (!token) {
    throw new RedisSyntaxError()
  }
  const match = /^([iu])(\d+)$/.exec(token.toString())
  if (!match) {
    throw new BitfieldTypeError()
  }
  const signed = match[1] === 'i'
  const bits = Number(match[2])
  const maxBits = signed ? 64 : 63
  if (bits < 1 || bits > maxBits) {
    throw new BitfieldTypeError()
  }
  return { signed, bits }
}

function parseFieldOffset(token: Buffer | undefined, bits: number): number {
  if (!token) {
    throw new RedisSyntaxError()
  }
  let raw = token.toString()
  const useWidth = raw.startsWith('#')
  if (useWidth) {
    raw = raw.slice(1)
  }
  if (!isIntegerToken(raw)) {
    throw new BitOffsetError()
  }
  const n = Number(raw)
  if (!Number.isSafeInteger(n) || n < 0) {
    throw new BitOffsetError()
  }
  const offset = useWidth ? n * bits : n
  if (offset >= MAX_BIT_OFFSET) {
    throw new BitOffsetError()
  }
  return offset
}

function parseFieldValue(token: Buffer | undefined): bigint {
  if (!token) {
    throw new RedisSyntaxError()
  }
  const raw = token.toString()
  if (!isIntegerToken(raw)) {
    throw new ExpectedIntegerError()
  }
  const value = BigInt(raw)
  if (value < INT64_MIN || value > INT64_MAX) {
    throw new ExpectedIntegerError()
  }
  return value
}

function parseBitFieldOps(
  input: readonly Buffer[],
  index: number,
  ctx: ParseContext,
  readonly: boolean,
): { value: BitFieldArgs; nextIndex: number } {
  const key = input[index]
  if (!key) {
    throw new WrongNumberOfArgumentsError(ctx.commandName)
  }
  const ops: BitFieldOp[] = []
  let overflow: Overflow = 'WRAP'
  let cursor = index + 1

  while (cursor < input.length) {
    const sub = input[cursor]!.toString().toUpperCase()

    if (sub === 'OVERFLOW') {
      const modeToken = input[cursor + 1]
      if (!modeToken) {
        throw new RedisSyntaxError()
      }
      const mode = modeToken.toString().toUpperCase()
      if (mode !== 'WRAP' && mode !== 'SAT' && mode !== 'FAIL') {
        throw new BitfieldOverflowTypeError()
      }
      overflow = mode
      cursor += 2
      continue
    }

    if (sub === 'GET') {
      const type = parseFieldType(input[cursor + 1])
      const offset = parseFieldOffset(input[cursor + 2], type.bits)
      ops.push({ kind: 'GET', type, offset })
      cursor += 3
      continue
    }

    if (sub === 'SET' || sub === 'INCRBY') {
      if (readonly) {
        throw new BitfieldRoGetOnlyError()
      }
      const type = parseFieldType(input[cursor + 1])
      const offset = parseFieldOffset(input[cursor + 2], type.bits)
      const operand = parseFieldValue(input[cursor + 3])
      ops.push({ kind: sub, type, offset, operand, overflow })
      cursor += 4
      continue
    }

    if (readonly) {
      throw new BitfieldRoGetOnlyError()
    }
    throw new RedisSyntaxError()
  }

  return { value: { key, ops }, nextIndex: input.length }
}

function readField(buf: Buffer, offset: number, type: FieldType): bigint {
  let value = 0n
  for (let i = 0; i < type.bits; i++) {
    value = (value << 1n) | BigInt(getBit(buf, offset + i))
  }
  if (type.signed && value & (1n << BigInt(type.bits - 1))) {
    value -= 1n << BigInt(type.bits)
  }
  return value
}

function writeField(
  buf: Buffer,
  offset: number,
  type: FieldType,
  value: bigint,
): void {
  const stored = value & ((1n << BigInt(type.bits)) - 1n)
  for (let i = 0; i < type.bits; i++) {
    const bit = Number((stored >> BigInt(type.bits - 1 - i)) & 1n)
    const bitIndex = offset + i
    const byteIndex = bitIndex >>> 3
    const mask = 1 << (7 - (bitIndex & 7))
    if (bit) {
      buf[byteIndex]! |= mask
    } else {
      buf[byteIndex]! &= ~mask
    }
  }
}

// Apply the overflow policy. Returns the value to store (and reply with), or
// null when an overflow occurs under FAIL.
function applyOverflow(
  value: bigint,
  type: FieldType,
  mode: Overflow,
): bigint | null {
  const min = type.signed ? -(1n << BigInt(type.bits - 1)) : 0n
  const max = type.signed
    ? (1n << BigInt(type.bits - 1)) - 1n
    : (1n << BigInt(type.bits)) - 1n

  if (value >= min && value <= max) {
    return value
  }
  if (mode === 'FAIL') {
    return null
  }
  if (mode === 'SAT') {
    return value > max ? max : min
  }
  // WRAP: modular reduction into [min, max].
  const range = 1n << BigInt(type.bits)
  return ((((value - min) % range) + range) % range) + min
}

function executeBitField(
  args: BitFieldArgs,
  ctx: RedisExecutionContext,
): RedisResult {
  const existing = ensureStringOrMissing(ctx.db, args.key)
  const hasWrite = args.ops.some(op => op.kind !== 'GET')

  let buf = existing ? Buffer.from(existing) : Buffer.alloc(0)
  // Pre-grow once to fit every write op — Redis grows the key even for an op
  // that ultimately FAILs its overflow check.
  let maxBytes = buf.length
  for (const op of args.ops) {
    if (op.kind === 'GET') continue
    const need = Math.ceil((op.offset + op.type.bits) / 8)
    if (need > maxBytes) maxBytes = need
  }
  if (maxBytes > buf.length) {
    const grown = Buffer.alloc(maxBytes)
    buf.copy(grown, 0)
    buf = grown
  }

  const results: RedisValue[] = []
  for (const op of args.ops) {
    if (op.kind === 'GET') {
      results.push(RedisValue.integer(readField(buf, op.offset, op.type)))
      continue
    }
    if (op.kind === 'SET') {
      const old = readField(buf, op.offset, op.type)
      const next = applyOverflow(op.operand, op.type, op.overflow)
      if (next === null) {
        results.push(RedisValue.null())
        continue
      }
      writeField(buf, op.offset, op.type, next)
      results.push(RedisValue.integer(old))
    } else {
      const current = readField(buf, op.offset, op.type)
      const next = applyOverflow(current + op.operand, op.type, op.overflow)
      if (next === null) {
        results.push(RedisValue.null())
        continue
      }
      writeField(buf, op.offset, op.type, next)
      results.push(RedisValue.integer(next))
    }
  }

  if (hasWrite) {
    ctx.db.setString(args.key, buf, { keepTtl: true })
  }
  return RedisResult.create(RedisValue.array(results))
}

function bitFieldSchema(readonly: boolean): CommandSchema<BitFieldArgs> {
  return t.custom((input, index, ctx) =>
    parseBitFieldOps(input, index, ctx, readonly),
  )
}

export const bitfieldCommand = defineCommand({
  name: 'bitfield',
  schema: bitFieldSchema(false),
  flags: ['write', 'denyoom'],
  keys: args => [args.key],
  execute: executeBitField,
})

export const bitfieldRoCommand = defineCommand({
  name: 'bitfield_ro',
  since: { redis: '6.2.0', valkey: '7.2.0' },
  schema: bitFieldSchema(true),
  flags: ['readonly', 'fast'],
  keys: args => [args.key],
  execute: executeBitField,
})

export const bitmapsCommands = [
  setbitCommand,
  getbitCommand,
  bitcountCommand,
  bitposCommand,
  bitopCommand,
  bitfieldCommand,
  bitfieldRoCommand,
]
