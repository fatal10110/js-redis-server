/**
 * Regenerates `tests/fixtures/redis-double-format.json`: the text real Redis
 * and Valkey servers print for a double, per version (#451).
 *
 * Point it at *private* servers — one per version — e.g.
 *
 *   docker run -d --rm --name dbl-62 -p 47451:6379 redis:6.2.14
 *   docker run -d --rm --name dbl-70 -p 47452:6379 redis:7.0.15
 *   ...
 *   node --import tsx scripts/capture-double-format-fixture.ts \
 *     47451 47452 47453 47454 47455 47456 47457
 *
 * Each value is stored with ZADD and read back with ZSCORE (a RESP2 bulk
 * string built by `addReplyDouble`). The script only touches one random key
 * per server and deletes it afterwards.
 *
 * Servers print one of exactly two spellings — `%.17g` (Redis 6.2 / 7.0) or
 * `d2string()` / `fpconv_dtoa` (Redis 7.2+, every Valkey) — so the fixture
 * stores those two columns once and records which one each server matched.
 * The script fails if a server matches neither.
 *
 * The fixture keeps the first `KEEP` generated values plus every value where a
 * naive formatter would be wrong: an fpconv (Grisu2) digit string that is not
 * JavaScript's shortest round-trip, or a `%.17g` round-half-even tie that
 * `toPrecision(17)` rounds the other way.
 */
import { randomBytes } from 'node:crypto'
import { writeFileSync } from 'node:fs'
import { resolve } from 'node:path'
import Redis from 'ioredis'

const ports = process.argv.slice(2).map(Number)
if (ports.length === 0 || ports.some(port => !Number.isInteger(port))) {
  console.error('usage: capture-double-format-fixture.ts <port> [port...]')
  process.exit(1)
}

const TOTAL = 30000
const KEEP = 1200
const OUTPUT = resolve(__dirname, '../tests/fixtures/redis-double-format.json')

// The edge-weighted generator from #451 (deterministic, seed 414).
function generateValues(count: number): number[] {
  let seed = 414
  const rand = () => {
    seed = (seed * 1103515245 + 12345) & 0x7fffffff
    return seed / 0x7fffffff
  }
  const values: number[] = [
    0.1,
    0.3,
    1 / 3,
    2.5,
    100,
    1e-5,
    0.0000123,
    0.000123,
    0.0001234567,
    1e-7,
    1e-6,
    1e15,
    1e16,
    1e17,
    1e20,
    1e21,
    5e18,
    Number('1.2345678901234567e22'),
    2 ** 52,
    2 ** 52 - 0.5,
    2 ** 53,
    2 ** 53 + 2,
    2 ** 62,
    -(2 ** 62),
    2 ** 62 + 1024,
    2 ** 63,
    Number('1234567890123456789'),
    1234567890123456.25,
    1234567890123456.75,
    5e-324,
    2.2250738585072014e-308,
    Number.MAX_VALUE,
    -Number.MAX_VALUE,
    4.8911660955712037e-5,
  ]
  const buf = new DataView(new ArrayBuffer(8))
  while (values.length < count) {
    const kind = values.length % 6
    let v: number
    if (kind === 0) {
      buf.setUint32(0, Math.floor(rand() * 2 ** 32))
      buf.setUint32(4, Math.floor(rand() * 2 ** 32))
      v = buf.getFloat64(0)
    } else if (kind === 1) {
      v = Math.round(10 ** (15 + rand() * 8))
    } else if (kind === 2) {
      const d = Math.floor(rand() * 999) + 1
      v = Number(`${d}e${Math.floor(rand() * 40) - 20}`)
    } else if (kind === 3) {
      const digits = Math.floor(rand() * 17) + 1
      const m = Math.floor(rand() * 10 ** Math.min(digits, 15)) + 1
      v = Number(`${m}e-${Math.floor(rand() * 20)}`)
    } else if (kind === 4) {
      v = (rand() - 0.5) * 10 ** Math.floor(rand() * 30 - 10)
    } else {
      v = 2 ** 62 + (Math.floor(rand() * 64) - 32) * 1024
    }
    if (!Number.isFinite(v) || v === 0) {
      continue
    }
    if (rand() < 0.3) {
      v = -v
    }
    values.push(v)
  }
  return values
}

async function capture(port: number, inputs: readonly string[]) {
  const client = new Redis({ port, lazyConnect: true })
  await client.connect()
  const info = await client.info('server')
  const valkey = /^valkey_version:(\S+)/m.exec(info)?.[1]
  const redis = /^redis_version:(\S+)/m.exec(info)?.[1]
  const server = valkey ? `valkey-${valkey}` : `redis-${redis}`
  const key = `dblfix:${randomBytes(8).toString('hex')}`
  const replies: string[] = []
  const BATCH = 1000
  try {
    for (let start = 0; start < inputs.length; start += BATCH) {
      const slice = inputs.slice(start, start + BATCH)
      const pipeline = client.pipeline()
      slice.forEach((input, i) => pipeline.zadd(key, input, `m${start + i}`))
      slice.forEach((_, i) => pipeline.zscore(key, `m${start + i}`))
      const results = (await pipeline.exec()) ?? []
      for (const [err, reply] of results.slice(slice.length)) {
        if (err) throw err
        replies.push(String(reply))
      }
    }
  } finally {
    await client.del(key)
    client.disconnect()
  }
  return { server, replies }
}

// `%.17g` tie check: the digits `toPrecision(17)` picks (round half up).
function toPrecisionDigits(v: number): string {
  return Math.abs(v)
    .toExponential(16)
    .replace(/e.*$/, '')
    .replace('.', '')
    .replace(/0+$/, '')
}

function significantDigits(text: string): string {
  return text
    .replace(/^-/, '')
    .replace(/e.*$/, '')
    .replace('.', '')
    .replace(/^0+/, '')
    .replace(/0+$/, '')
}

function shortestDigits(v: number): string {
  return Math.abs(v)
    .toExponential()
    .replace(/e.*$/, '')
    .replace('.', '')
    .replace(/0+$/, '')
}

async function main(): Promise<void> {
  const values = generateValues(TOTAL)
  const inputs = values.map(v => String(v))
  const captured = []
  for (const port of ports) {
    captured.push(await capture(port, inputs))
  }

  const g17Server = captured.find(c => c.replies[0] === '0.10000000000000001')
  const fpconvServer = captured.find(c => c.replies[0] === '0.1')
  if (!g17Server || !fpconvServer) {
    throw new Error('need at least one %.17g server and one fpconv server')
  }

  const servers: Record<string, 'g17' | 'fpconv'> = {}
  for (const { server, replies } of captured) {
    const style = replies.every((r, i) => r === g17Server.replies[i])
      ? 'g17'
      : replies.every((r, i) => r === fpconvServer.replies[i])
        ? 'fpconv'
        : undefined
    if (!style) {
      throw new Error(`${server} matches neither %.17g nor fpconv`)
    }
    servers[server] = style
  }

  const cases: [string, string, string][] = []
  values.forEach((v, i) => {
    const g17 = g17Server.replies[i]
    const fpconv = fpconvServer.replies[i]
    // Integers within ±2^62 print every digit (`ll2string`), so their digits
    // differ from the shortest round-trip by design — not a Grisu2 case.
    const integerPath = Number.isInteger(v) && Math.abs(v) <= 2 ** 62
    const interesting =
      (!integerPath && significantDigits(fpconv) !== shortestDigits(v)) ||
      significantDigits(g17) !== toPrecisionDigits(v)
    if (i < KEEP || interesting) {
      cases.push([inputs[i], g17, fpconv])
    }
  })

  writeFileSync(
    OUTPUT,
    JSON.stringify(
      {
        description:
          'ZADD <input> then ZSCORE, captured from real servers by scripts/capture-double-format-fixture.ts. Columns: [input, %.17g reply, d2string/fpconv reply].',
        servers,
        cases,
      },
      null,
      0,
    ).replace(/\],\[/g, '],\n['),
  )
  console.log(
    `wrote ${cases.length} cases (${TOTAL} probed) for ${Object.keys(servers).join(', ')}`,
  )
}

main().catch(err => {
  console.error(err)
  process.exitCode = 1
})
