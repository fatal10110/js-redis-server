import assert from 'node:assert'
import { after, before, describe, test } from 'node:test'

import {
  acceptsVersion,
  ALL_PROFILES,
  caseAvailable,
  DUMP_CASES,
  RDB_VERSION,
  realPayload,
} from '../dump-payload-cases'
import { RawRedisConnection } from '../raw-tcp/raw-connection'
import { TestRunner } from '../test-config'
import { activeProfile, commandFrame, randomKey } from '../utils'

/**
 * DUMP / RESTORE payloads by profile (#202). DUMP of each case must equal,
 * byte for byte, what the real server of the active profile answers
 * (captured in tests/fixtures/dump-payloads.json); RESTORE takes the payload
 * of every other profile whose RDB version the active one accepts and
 * rebuilds the same value, and refuses the rest with `DUMP payload version or
 * checksum are wrong`. Checked against redis-server 6.2.14, 7.0.15, 7.2.4,
 * 7.4.4, 8.0.0 and valkey-server 8.0.0, 9.0.0.
 */
const testRunner = new TestRunner()
const profile = activeProfile
const hashFieldTtl =
  profile === 'redis-7.4' || profile === 'redis-8.0' || profile === 'valkey-9.0'

const PAYLOAD_ERROR = '-ERR DUMP payload version or checksum are wrong\r\n'

type Plain = string | number | null | Plain[]

function plain(value: unknown): Plain {
  if (Buffer.isBuffer(value)) return value.toString('latin1')
  if (Array.isArray(value)) return value.map(plain)
  if (value === null || value === undefined) return null
  if (typeof value === 'number' || typeof value === 'string') return value
  return String(value)
}

/**
 * What a value restored from `source` reads back as, given the same value
 * built natively. A Redis 6.2 payload differs on newer servers in two ways
 * (the same on every 7.0+ server and the mock): its ziplist keeps a -0 score
 * as the text "-0", which a 7.0+ listpack would have stored as the integer
 * 0; and its consumer groups carry no entries_read, which loading estimates
 * (`streamEstimateDistanceFromFirstEverEntry()`), where XGROUP CREATE leaves
 * it unknown.
 */
function afterRestore(name: string, source: string, native: Plain): Plain {
  if (source !== 'redis-6.2' || profile === 'redis-6.2') return native
  const copy = JSON.parse(JSON.stringify(native)) as Plain[]
  if (name === 'zset') {
    const flat = copy[1] as Plain[]
    flat[flat.indexOf('c') + 1] = '-0'
  }
  if (name === 'stream_groups') {
    for (const group of copy[3] as Plain[][]) {
      const groupName = group[group.indexOf('name') + 1]
      group[group.indexOf('entries-read') + 1] = groupName === 'g1' ? 0 : 2
    }
  }
  return copy
}

describe(`DUMP / RESTORE payloads (${testRunner.getBackendName()}, ${profile})`, () => {
  let conn: RawRedisConnection

  before(async () => {
    const port = await testRunner.setupRawStandalone()
    conn = await RawRedisConnection.connect('127.0.0.1', port)
  })

  after(async () => {
    conn?.close()
    await testRunner.cleanup()
  })

  async function call(...args: (string | Buffer)[]): Promise<Plain> {
    conn.write(commandFrame(...args))
    return plain(await conn.readFrame())
  }

  async function raw(...args: (string | Buffer)[]): Promise<Buffer> {
    conn.write(commandFrame(...args))
    return conn.readRawFrame()
  }

  async function build(name: string): Promise<string> {
    const key = `dump:${randomKey()}:${name}`
    for (const command of DUMP_CASES[name].setup(key)) {
      const reply = await raw(...command)
      assert.ok(!reply.toString().startsWith('-'), reply.toString())
    }
    return key
  }

  /** What a client can read back of `key`, in a comparable form. */
  async function snapshot(key: string): Promise<Plain> {
    const type = await call('TYPE', key)
    switch (type) {
      case 'string':
        return [type, await call('GET', key)]
      case 'list':
        return [type, await call('LRANGE', key, '0', '-1')]
      case 'set':
        return [type, await call('SORT', key, 'ALPHA')]
      case 'zset':
        return [type, await call('ZRANGE', key, '0', '-1', 'WITHSCORES')]
      case 'hash': {
        const flat = (await call('HGETALL', key)) as string[]
        const pairs: string[][] = []
        for (let i = 0; i < flat.length; i += 2) {
          pairs.push([flat[i], flat[i + 1]])
        }
        pairs.sort((a, b) => (a[0] < b[0] ? -1 : a[0] > b[0] ? 1 : 0))
        if (!hashFieldTtl) return [type, pairs]
        // HPEXPIRETIME is not in the mock yet (#651): compare the remaining
        // times, with a margin for the time between the two reads.
        const fields = pairs.map(pair => pair[0])
        const ttls = (await call(
          'HPTTL',
          key,
          'FIELDS',
          String(fields.length),
          ...fields,
        )) as number[]
        return [
          type,
          pairs,
          ttls.map(ms => (ms < 0 ? ms : Math.round(ms / 1e6))),
        ]
      }
      case 'stream':
        return [
          type,
          await call('XLEN', key),
          await call('XRANGE', key, '-', '+'),
          await call('XINFO', 'GROUPS', key),
        ]
      default:
        return [type]
    }
  }

  for (const name of Object.keys(DUMP_CASES)) {
    const own = realPayload(name, profile)
    const deterministic = DUMP_CASES[name].deterministic !== false
    if (caseAvailable(name, profile) && own && deterministic) {
      test(`DUMP ${name} is the real payload`, async () => {
        const key = await build(name)
        const dumped = await call('DUMP', key)
        assert.strictEqual(
          Buffer.from(dumped as string, 'latin1').toString('hex'),
          own.toString('hex'),
        )
      })
    }

    for (const source of ALL_PROFILES) {
      const payload = realPayload(name, source)
      if (!payload) continue
      const version = RDB_VERSION[source]
      if (!acceptsVersion(profile, version)) {
        test(`RESTORE ${name} from ${source} (RDB ${version}) is refused`, async () => {
          const key = `dump:${randomKey()}:${name}`
          assert.strictEqual(
            (await raw('RESTORE', key, '0', payload)).toString(),
            PAYLOAD_ERROR,
          )
          assert.strictEqual(await call('EXISTS', key), 1 - 1)
        })
        continue
      }
      test(`RESTORE ${name} from ${source} (RDB ${version}) rebuilds the value`, async () => {
        const key = `dump:${randomKey()}:${name}`
        assert.strictEqual(
          (await raw('RESTORE', key, '0', payload)).toString(),
          '+OK\r\n',
        )
        if (!caseAvailable(name, profile)) return
        const original = await build(name)
        assert.deepStrictEqual(
          await snapshot(key),
          afterRestore(name, source, await snapshot(original)),
        )
      })
    }
  }
})
