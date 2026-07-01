import { createHash } from 'node:crypto'
import { defineCommand } from '../core/command-definition'
import { t } from '../core/command-schema'
import { InvalidHllError } from '../core/redis-error'
import { ensureStringOrMissing, integer, simpleString } from './helpers'
import type { RedisDatabase } from '../state'

// Dense HyperLogLog registers: 14-bit index -> 16384 registers, one byte
// each. Not byte-compatible with real Redis's sparse/dense HLL encoding —
// PFCOUNT is approximate cardinality in both implementations, so exact wire
// format parity isn't required (no DUMP/RESTORE support for this type yet).
const HLL_BITS = 14
const HLL_REGISTERS = 1 << HLL_BITS
const HLL_INDEX_SHIFT = BigInt(64 - HLL_BITS)
const HLL_REST_BITS = 64 - HLL_BITS

function hash64(value: Buffer): bigint {
  return createHash('sha1').update(value).digest().readBigUInt64BE(0)
}

// Returns true when adding `value` increased a register (i.e. the HLL was
// altered).
function hllAdd(registers: Buffer, value: Buffer): boolean {
  const h = hash64(value)
  const index = Number(h >> HLL_INDEX_SHIFT)
  const rest = h & ((1n << HLL_INDEX_SHIFT) - 1n)

  let rank = 1
  for (let i = HLL_REST_BITS - 1; i >= 0; i--) {
    if ((rest >> BigInt(i)) & 1n) break
    rank++
  }

  if (registers[index]! < rank) {
    registers[index] = rank
    return true
  }
  return false
}

// Standard HyperLogLog cardinality estimator with small/large range
// corrections (linear counting below 2.5m, large-range correction near 2^32).
function hllCount(registers: Buffer): number {
  const m = HLL_REGISTERS
  let sum = 0
  let zeros = 0
  for (let i = 0; i < m; i++) {
    sum += Math.pow(2, -registers[i]!)
    if (registers[i] === 0) zeros++
  }

  const alpha = 0.7213 / (1 + 1.079 / m)
  let estimate = (alpha * m * m) / sum

  if (estimate <= 2.5 * m && zeros > 0) {
    estimate = m * Math.log(m / zeros)
  } else if (estimate > (1 / 30) * Math.pow(2, 32)) {
    estimate = -Math.pow(2, 32) * Math.log(1 - estimate / Math.pow(2, 32))
  }

  return Math.round(estimate)
}

function mergeInto(dest: Buffer, source: Buffer): void {
  for (let i = 0; i < HLL_REGISTERS; i++) {
    if (source[i]! > dest[i]!) {
      dest[i] = source[i]!
    }
  }
}

// Fetches a key as HLL registers. Returns null for a missing key (treated as
// an empty/all-zero HLL by callers). Throws WRONGTYPE for a non-string key,
// or the HLL-specific WRONGTYPE for a string that isn't a valid HLL buffer.
function getHllOrThrow(db: RedisDatabase, key: Buffer): Buffer | null {
  const existing = ensureStringOrMissing(db, key)
  if (!existing) {
    return null
  }
  if (existing.length !== HLL_REGISTERS) {
    throw new InvalidHllError()
  }
  return existing
}

// --- PFADD -------------------------------------------------------------

export const pfaddCommand = defineCommand({
  name: 'pfadd',
  schema: t.object({ key: t.key(), elements: t.variadic(t.bulk()) }),
  flags: ['write', 'denyoom', 'fast'],
  keys: args => [args.key],
  execute: (args, ctx) => {
    const existing = getHllOrThrow(ctx.db, args.key)
    const created = existing === null
    const registers = existing
      ? Buffer.from(existing)
      : Buffer.alloc(HLL_REGISTERS)

    let altered = created
    for (const element of args.elements) {
      if (hllAdd(registers, element)) {
        altered = true
      }
    }

    if (altered) {
      ctx.db.setString(args.key, registers, { keepTtl: true })
    }
    return integer(altered ? 1 : 0)
  },
})

// --- PFCOUNT -------------------------------------------------------------

export const pfcountCommand = defineCommand({
  name: 'pfcount',
  schema: t.object({ keys: t.variadic(t.key(), { min: 1 }) }),
  flags: ['readonly'],
  keys: args => args.keys,
  execute: (args, ctx) => {
    if (args.keys.length === 1) {
      const registers = getHllOrThrow(ctx.db, args.keys[0]!)
      return integer(registers ? hllCount(registers) : 0)
    }

    const merged = Buffer.alloc(HLL_REGISTERS)
    for (const key of args.keys) {
      const registers = getHllOrThrow(ctx.db, key)
      if (registers) {
        mergeInto(merged, registers)
      }
    }
    return integer(hllCount(merged))
  },
})

// --- PFMERGE -------------------------------------------------------------

export const pfmergeCommand = defineCommand({
  name: 'pfmerge',
  schema: t.object({
    destKey: t.key(),
    sourceKeys: t.variadic(t.key()),
  }),
  flags: ['write', 'denyoom'],
  keys: args => [args.destKey, ...args.sourceKeys],
  execute: (args, ctx) => {
    const destExisting = getHllOrThrow(ctx.db, args.destKey)
    const merged = destExisting
      ? Buffer.from(destExisting)
      : Buffer.alloc(HLL_REGISTERS)

    for (const key of args.sourceKeys) {
      const registers = getHllOrThrow(ctx.db, key)
      if (registers) {
        mergeInto(merged, registers)
      }
    }

    ctx.db.setString(args.destKey, merged, { keepTtl: true })
    return simpleString('OK')
  },
})

export const hyperloglogCommands = [
  pfaddCommand,
  pfcountCommand,
  pfmergeCommand,
]
