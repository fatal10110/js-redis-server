// LZF as Redis vendors it (`lzf_c.c` / `lzf_d.c`, liblzf 3.6 with HLOG 16,
// VERY_FAST 1, ULTRA_FAST 0). RDB strings longer than 20 bytes are stored
// compressed when that saves at least 4 bytes, so DUMP is byte-identical to a
// real server only with the very same match choices: this is a line-by-line
// port of `lzf_compress`, hash function included.

const HLOG = 16
const HSIZE = 1 << HLOG
const MAX_LIT = 1 << 5
const MAX_OFF = 1 << 13
const MAX_REF = (1 << 8) + (1 << 3)

function idx(hval: number): number {
  // VERY_FAST: ((h >> (3*8 - HLOG)) - h*5) & (HSIZE - 1), unsigned 32-bit.
  return ((hval >>> (24 - HLOG)) - Math.imul(hval, 5)) & (HSIZE - 1)
}

/**
 * `lzf_compress(in, in_len, out, out_len)`: the compressed bytes, or null when
 * they would not fit in `outLen` bytes (the C function's 0).
 */
export function lzfCompress(input: Uint8Array, outLen: number): Buffer | null {
  const inLen = input.length
  if (inLen === 0 || outLen === 0) return null

  // The C buffer is out_len + 1 bytes (rdbSaveLzfStringObject), and the final
  // run bookkeeping may touch one byte past out_end.
  const out = Buffer.alloc(outLen + 2)
  // Positions are stored + 1 so 0 means an empty slot; position 0 can never
  // be a reference anyway (`ref > in_data`).
  const htab = new Int32Array(HSIZE)
  let ip = 0
  let op = 1 // start run
  let lit = 0
  const at = (i: number): number => input[i] ?? 0

  let hval = ((at(0) << 8) | at(1)) >>> 0
  while (ip < inLen - 2) {
    hval = ((hval << 8) | at(ip + 2)) >>> 0
    const slot = idx(hval)
    const ref = htab[slot] - 1
    htab[slot] = ip + 1

    let off: number
    if (
      ref > 0 &&
      (off = ip - ref - 1) < MAX_OFF &&
      input[ref + 2] === input[ip + 2] &&
      input[ref] === input[ip] &&
      input[ref + 1] === input[ip + 1]
    ) {
      let len = 2
      let maxlen = inLen - ip - len
      if (maxlen > MAX_REF) maxlen = MAX_REF

      if (op + 3 + 1 >= outLen) {
        if (op - (lit === 0 ? 1 : 0) + 3 + 1 >= outLen) return null
      }

      out[op - lit - 1] = (lit - 1) & 0xff // stop run
      if (lit === 0) op -= 1 // undo run if length is zero

      extend: {
        if (maxlen > 16) {
          for (let k = 0; k < 16; k++) {
            len++
            if (input[ref + len] !== input[ip + len]) break extend
          }
        }
        do {
          len++
        } while (len < maxlen && input[ref + len] === input[ip + len])
      }

      len -= 2 // len is now #octets - 1
      ip++

      if (len < 7) {
        out[op++] = (off >> 8) + (len << 5)
      } else {
        out[op++] = (off >> 8) + (7 << 5)
        out[op++] = len - 7
      }
      out[op++] = off & 0xff

      lit = 0
      op++ // start run

      ip += len + 1
      if (ip >= inLen - 2) break

      ip -= 2
      hval = ((at(ip) << 8) | at(ip + 1)) >>> 0
      hval = ((hval << 8) | at(ip + 2)) >>> 0
      htab[idx(hval)] = ip + 1
      ip++
      hval = ((hval << 8) | at(ip + 2)) >>> 0
      htab[idx(hval)] = ip + 1
      ip++
    } else {
      // one more literal byte we must copy
      if (op >= outLen) return null
      lit++
      out[op++] = input[ip++]
      if (lit === MAX_LIT) {
        out[op - lit - 1] = lit - 1 // stop run
        lit = 0
        op++ // start run
      }
    }
  }

  if (op + 3 > outLen) return null // at most 3 bytes can be missing here

  while (ip < inLen) {
    lit++
    out[op++] = input[ip++]
    if (lit === MAX_LIT) {
      out[op - lit - 1] = lit - 1
      lit = 0
      op++
    }
  }

  out[op - lit - 1] = (lit - 1) & 0xff // end run
  if (lit === 0) op -= 1 // undo run if length is zero

  return out.subarray(0, op)
}

/**
 * `lzf_decompress`: the `outLen` bytes `input` expands to, or null when the
 * data is corrupt or does not expand to exactly `outLen` bytes.
 */
export function lzfDecompress(
  input: Uint8Array,
  outLen: number,
): Buffer | null {
  const out = Buffer.alloc(outLen)
  let ip = 0
  let op = 0
  while (ip < input.length) {
    const ctrl = input[ip++]
    if (ctrl < 1 << 5) {
      const len = ctrl + 1
      if (op + len > outLen || ip + len > input.length) return null
      out.set(input.subarray(ip, ip + len), op)
      op += len
      ip += len
    } else {
      let len = ctrl >> 5
      if (len === 7) {
        if (ip >= input.length) return null
        len += input[ip++]
      }
      if (ip >= input.length) return null
      const ref = op - ((ctrl & 0x1f) << 8) - input[ip++] - 1
      len += 2
      if (op + len > outLen || ref < 0) return null
      for (let k = 0; k < len; k++) out[op++] = out[ref + k]
    }
  }
  return op === outLen ? out : null
}
