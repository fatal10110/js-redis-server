// CRC-64/Jones as Redis computes it (`crc64.c`): reflected, polynomial
// 0xad93d23594c935a9, initial value 0, no final xor. DUMP payloads end with
// this checksum of everything before it. The check value of "123456789" is
// 0xe9c6d914c4b8d9ca. The table holds each 64-bit entry as two 32-bit halves
// so the hot loop stays in plain numbers.

const REFLECTED_POLY = 0x95ac9329ac4bc9b5n // bit-reversed 0xad93d23594c935a9

const TABLE_LO = new Uint32Array(256)
const TABLE_HI = new Uint32Array(256)

for (let index = 0; index < 256; index++) {
  let crc = BigInt(index)
  for (let bit = 0; bit < 8; bit++) {
    crc = crc & 1n ? (crc >> 1n) ^ REFLECTED_POLY : crc >> 1n
  }
  TABLE_LO[index] = Number(crc & 0xffffffffn)
  TABLE_HI[index] = Number(crc >> 32n)
}

/** The CRC-64 of `data`, as the 8 little-endian bytes DUMP appends. */
export function crc64(data: Uint8Array): Buffer {
  let lo = 0
  let hi = 0
  for (let i = 0; i < data.length; i++) {
    const index = (lo ^ data[i]) & 0xff
    lo = ((lo >>> 8) | (hi << 24)) ^ TABLE_LO[index]
    hi = (hi >>> 8) ^ TABLE_HI[index]
  }
  const out = Buffer.alloc(8)
  out.writeUInt32LE(lo >>> 0, 0)
  out.writeUInt32LE(hi >>> 0, 4)
  return out
}
