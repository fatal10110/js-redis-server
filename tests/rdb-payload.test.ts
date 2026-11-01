import assert from 'node:assert'
import { readFileSync } from 'node:fs'
import { resolve } from 'node:path'
import { describe, test } from 'node:test'

import { resolveCompatibilityProfile } from '../src/core/compatibility'
import type { RedisDataValue } from '../src/state/data-types'
import { crc64 } from '../src/state/rdb/crc64'
import { lzfCompress, lzfDecompress } from '../src/state/rdb/lzf'
import {
  createDumpPayload,
  deserializeValue,
  RdbFormatError,
  rdbFormatFor,
  verifyDumpPayload,
} from '../src/state/rdb/payload'

type Profile =
  | 'redis-6.2'
  | 'redis-7.0'
  | 'redis-7.2'
  | 'redis-7.4'
  | 'redis-8.0'
  | 'valkey-8.0'
  | 'valkey-9.0'

const PROFILES: readonly Profile[] = [
  'redis-6.2',
  'redis-7.0',
  'redis-7.2',
  'redis-7.4',
  'redis-8.0',
  'valkey-8.0',
  'valkey-9.0',
]

// DUMP payloads captured from real servers (#202): see
// tests-integration/dump-payload-cases.ts for the commands behind each case.
const FIXTURES = JSON.parse(
  readFileSync(resolve(__dirname, 'fixtures/dump-payloads.json'), 'utf8'),
) as Record<string, Partial<Record<Profile, string>>>

const format = (profile: Profile) =>
  rdbFormatFor(resolveCompatibilityProfile(profile))

/** A payload around `body` with `version` and a valid checksum. */
function forge(body: string, version: number): Buffer {
  const signed = Buffer.concat([
    Buffer.from(body, 'hex'),
    Buffer.from([version & 0xff, version >> 8]),
  ])
  return Buffer.concat([signed, crc64(signed)])
}

function load(payload: Buffer, profile: Profile): RedisDataValue {
  const body = verifyDumpPayload(payload, format(profile))
  assert.ok(body, 'payload verifies')
  return deserializeValue(body, format(profile))
}

describe('crc64', () => {
  test('matches the CRC-64/Jones check value', () => {
    // 0xe9c6d914c4b8d9ca, little-endian as DUMP stores it.
    assert.strictEqual(
      crc64(Buffer.from('123456789')).toString('hex'),
      'cad9b8c414d9c6e9',
    )
  })
})

describe('lzf', () => {
  test('compresses like liblzf (VERY_FAST, HLOG 16)', () => {
    const input = Buffer.from('x'.repeat(45))
    const out = lzfCompress(input, input.length - 4)
    assert.strictEqual(out?.toString('hex'), '017878e02000017878')
  })

  test('round-trips text and refuses output that does not fit', () => {
    const input = Buffer.from(
      'the quick brown fox jumps over the lazy dog '.repeat(40),
    )
    const out = lzfCompress(input, input.length - 4)
    assert.ok(out)
    assert.deepStrictEqual(lzfDecompress(out, input.length), input)
    const noise = Buffer.from(
      Array.from({ length: 64 }, (_, i) => (i * 97 + 13) & 0xff),
    )
    assert.strictEqual(lzfCompress(noise, noise.length - 4), null)
    assert.strictEqual(lzfDecompress(Buffer.from([0xe0]), 10), null)
  })
})

describe('DUMP payloads of real servers', () => {
  for (const [name, payloads] of Object.entries(FIXTURES)) {
    for (const profile of PROFILES) {
      const hex = payloads[profile]
      if (hex === undefined) continue
      test(`${name} on ${profile} loads and serializes back to the same bytes`, () => {
        const payload = Buffer.from(hex, 'hex')
        const value = load(payload, profile)
        assert.strictEqual(
          createDumpPayload(value, format(profile)).toString('hex'),
          hex,
        )
      })
    }
  }

  test('a real string payload carries each profile version and checksum', () => {
    assert.deepStrictEqual(
      Object.fromEntries(PROFILES.map(p => [p, FIXTURES.string[p]?.slice(6)])),
      {
        'redis-6.2': '090041fa9f7e4dbee97f',
        'redis-7.0': '0a009108ceb219388ace',
        'redis-7.2': '0b00f8d773c1e4a76347',
        'redis-7.4': '0c005a7efa72e3121487',
        'redis-8.0': '0c005a7efa72e3121487',
        'valkey-8.0': '0b00f8d773c1e4a76347',
        'valkey-9.0': '50005e220723876aed3a',
      },
    )
  })
})

describe('verifyDumpPayload', () => {
  const body = '000176'
  test('accepts versions up to the profile own', () => {
    assert.ok(verifyDumpPayload(forge(body, 9), format('redis-7.0')))
    assert.ok(verifyDumpPayload(forge(body, 0), format('redis-6.2')))
    assert.strictEqual(
      verifyDumpPayload(forge(body, 11), format('redis-7.0')),
      null,
    )
    assert.strictEqual(
      verifyDumpPayload(forge(body, 12), format('valkey-8.0')),
      null,
    )
  })

  test('valkey 9.0 accepts 1-11 and 80 only', () => {
    const valkey9 = format('valkey-9.0')
    assert.strictEqual(verifyDumpPayload(forge(body, 0), valkey9), null)
    assert.ok(verifyDumpPayload(forge(body, 1), valkey9))
    assert.ok(verifyDumpPayload(forge(body, 11), valkey9))
    assert.strictEqual(verifyDumpPayload(forge(body, 12), valkey9), null)
    assert.strictEqual(verifyDumpPayload(forge(body, 79), valkey9), null)
    assert.ok(verifyDumpPayload(forge(body, 80), valkey9))
    assert.strictEqual(verifyDumpPayload(forge(body, 81), valkey9), null)
  })

  test('rejects a bad checksum and payloads under 10 bytes', () => {
    const good = forge(body, 9)
    const bad = Buffer.from(good)
    bad[bad.length - 1] ^= 1
    assert.strictEqual(verifyDumpPayload(bad, format('redis-8.0')), null)
    assert.strictEqual(
      verifyDumpPayload(Buffer.alloc(9), format('redis-8.0')),
      null,
    )
  })
})

describe('deserializeValue', () => {
  test('loads encodings no profile writes any more', () => {
    // RDB_TYPE_LIST: length, then plain strings.
    assert.deepStrictEqual(load(forge('01020161c007', 9), 'redis-8.0'), {
      type: 'list',
      values: [Buffer.from('a'), Buffer.from('7')],
    })
    // RDB_TYPE_LIST_ZIPLIST: [a, 1].
    const list = load(
      forge('0a10100000000d000000020000016103f2ff', 9),
      'redis-6.2',
    )
    assert.deepStrictEqual(list, {
      type: 'list',
      values: [Buffer.from('a'), Buffer.from('1')],
    })
    // RDB_TYPE_ZSET: scores as text, with the inf / nan length markers.
    const zset = load(forge('0302016103322e350162fe', 9), 'redis-8.0')
    assert.strictEqual(zset.type, 'zset')
    assert.deepStrictEqual(
      Array.from(zset.members.values()).map(m => [
        m.member.toString(),
        m.score,
      ]),
      [
        ['a', 2.5],
        ['b', Infinity],
      ],
    )
    // RDB_TYPE_HASH_ZIPMAP: zmlen 1, "f" -> "v" with no free bytes.
    const hash = load(forge('0907010166010076ff', 9), 'redis-8.0')
    assert.strictEqual(hash.type, 'hash')
    assert.deepStrictEqual(
      Array.from(hash.fields.values()).map(f => [
        f.field.toString(),
        f.value.toString(),
      ]),
      [['f', 'v']],
    )
  })

  test('refuses unknown types, empty values and truncated bodies', () => {
    const cases: Array<[string, Profile]> = [
      ['7e', 'redis-8.0'], // unknown type
      ['00', 'redis-8.0'], // type byte only
      ['0005ab', 'redis-8.0'], // truncated string
      ['0100', 'redis-8.0'], // an empty list
      ['14', 'redis-7.0'], // a listpack set before 7.2
      ['19', 'redis-7.2'], // a field-TTL hash before 7.4
      ['16', 'redis-8.0'], // valkey's field-TTL hash on redis
    ]
    for (const [body, profile] of cases) {
      assert.throws(
        () => deserializeValue(Buffer.from(body, 'hex'), format(profile)),
        RdbFormatError,
        `${body} on ${profile}`,
      )
    }
  })

  test('ignores bytes after a complete value, like real servers', () => {
    assert.deepStrictEqual(
      deserializeValue(Buffer.from('000176ffff', 'hex'), format('redis-8.0')),
      { type: 'string', value: Buffer.from('v') },
    )
  })
})
