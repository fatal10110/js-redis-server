/**
 * Benchmark for the RESP2 request decoder (#505): one multibulk request of N
 * small elements, fed in socket-sized chunks, at doubling N. With incremental
 * parsing the time per element stays flat as N grows; the old decoder
 * re-parsed the incomplete request from its first byte on every chunk, so its
 * time per element grew with N (quadratic overall).
 *
 *   node --import tsx scripts/bench-resp2-decoder.ts [chunkBytes]
 */
import { Resp2CommandDecoder } from '../src/core/transports/resp2/decoder'

const chunkBytes = Number(process.argv[2] ?? 16 * 1024)

function request(elements: number): Buffer {
  const parts: Buffer[] = [Buffer.from(`*${elements + 1}\r\n$5\r\nRPUSH\r\n`)]
  const element = Buffer.from('$5\r\nvalue\r\n')
  for (let i = 0; i < elements; i++) {
    parts.push(element)
  }
  return Buffer.concat(parts)
}

function decodeMs(bytes: Buffer): number {
  const decoder = new Resp2CommandDecoder({
    maxBulkLength: () => 512n * 1024n * 1024n,
  })
  const started = process.hrtime.bigint()
  let frames = 0
  for (let offset = 0; offset < bytes.length; offset += chunkBytes) {
    decoder.push(bytes.subarray(offset, offset + chunkBytes))
    while (decoder.next()) {
      frames += 1
    }
  }
  const elapsed = Number(process.hrtime.bigint() - started) / 1e6
  if (frames !== 1) {
    throw new Error(`expected one frame, decoded ${frames}`)
  }
  return elapsed
}

console.log(`chunk size ${chunkBytes} bytes`)
console.log('elements   total ms   ns/element')
for (const elements of [50_000, 100_000, 200_000, 400_000, 800_000]) {
  const bytes = request(elements)
  // Best of three, to keep a GC pause or a busy neighbour out of the figure.
  const ms = Math.min(decodeMs(bytes), decodeMs(bytes), decodeMs(bytes))
  console.log(
    `${String(elements).padStart(8)} ${ms.toFixed(1).padStart(10)} ${((ms * 1e6) / elements).toFixed(0).padStart(12)}`,
  )
}
