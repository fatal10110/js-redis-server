// Browser stand-in for `node:crypto`, aliased in vite.config.ts. The library
// source the demo bundles uses exactly one thing from it — synchronous
// `createHash('sha1')` (Lua script SHA1s in src/state/script-cache.ts, and the
// HyperLogLog element hash in src/commands/hyperloglog.ts). Both call sites are
// synchronous, so SubtleCrypto (async-only) cannot replace it, and the full
// `crypto-browserify` polyfill drags `elliptic` and friends into the Pages
// bundle for no reason (#393). This is a small pure-JS SHA-1 instead.

// `Buffer` here is the global, not an `import from 'buffer'`: in the bundle
// that is the same polyfill vite-plugin-node-polyfills injects into ../../src,
// so the digest has the BigInt readers hyperloglog needs (the `buffer@5` a
// bare 'buffer' import can resolve to does not).

type Input = string | Uint8Array

interface Hash {
  update(data: Input): Hash
  digest(): Buffer
  digest(encoding: 'hex'): string
}

export function createHash(algorithm: string): Hash {
  if (algorithm.toLowerCase() !== 'sha1') {
    throw new Error(`browser demo crypto shim: unsupported hash ${algorithm}`)
  }
  const chunks: Uint8Array[] = []
  const hash: Hash = {
    update(data: Input) {
      // Same default as Node: strings hash as their UTF-8 bytes.
      chunks.push(typeof data === 'string' ? Buffer.from(data, 'utf8') : data)
      return hash
    },
    digest(encoding?: 'hex') {
      const out = Buffer.from(sha1(Buffer.concat(chunks)))
      return (encoding === 'hex' ? out.toString('hex') : out) as never
    },
  }
  return hash
}

export default { createHash }

/** SHA-1 (FIPS 180-4) of `message`, as 20 bytes. */
export function sha1(message: Uint8Array): Uint8Array {
  // Pad: 0x80, zeros to 56 mod 64, then the 64-bit big-endian bit length.
  const padded = new Uint8Array((((message.length + 8) >> 6) + 1) << 6)
  padded.set(message)
  padded[message.length] = 0x80
  const view = new DataView(padded.buffer)
  const bits = message.length * 8
  view.setUint32(padded.length - 8, Math.floor(bits / 0x100000000))
  view.setUint32(padded.length - 4, bits >>> 0)

  let h0 = 0x67452301
  let h1 = 0xefcdab89
  let h2 = 0x98badcfe
  let h3 = 0x10325476
  let h4 = 0xc3d2e1f0
  const w = new Uint32Array(80)

  for (let offset = 0; offset < padded.length; offset += 64) {
    for (let i = 0; i < 16; i++) {
      w[i] = view.getUint32(offset + i * 4)
    }
    for (let i = 16; i < 80; i++) {
      const x = w[i - 3] ^ w[i - 8] ^ w[i - 14] ^ w[i - 16]
      w[i] = (x << 1) | (x >>> 31)
    }

    let a = h0
    let b = h1
    let c = h2
    let d = h3
    let e = h4
    for (let i = 0; i < 80; i++) {
      let f: number
      let k: number
      if (i < 20) {
        f = (b & c) | (~b & d)
        k = 0x5a827999
      } else if (i < 40) {
        f = b ^ c ^ d
        k = 0x6ed9eba1
      } else if (i < 60) {
        f = (b & c) | (b & d) | (c & d)
        k = 0x8f1bbcdc
      } else {
        f = b ^ c ^ d
        k = 0xca62c1d6
      }
      const t = (((a << 5) | (a >>> 27)) + f + e + k + w[i]) >>> 0
      e = d
      d = c
      c = (b << 30) | (b >>> 2)
      b = a
      a = t
    }

    h0 = (h0 + a) >>> 0
    h1 = (h1 + b) >>> 0
    h2 = (h2 + c) >>> 0
    h3 = (h3 + d) >>> 0
    h4 = (h4 + e) >>> 0
  }

  const out = new Uint8Array(20)
  const outView = new DataView(out.buffer)
  ;[h0, h1, h2, h3, h4].forEach((h, i) => outView.setUint32(i * 4, h))
  return out
}
