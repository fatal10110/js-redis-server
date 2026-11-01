import { readFileSync } from 'node:fs'
import { resolve } from 'node:path'
import type { ProfileName } from './utils'

/**
 * DUMP payloads captured from real servers (redis-server 6.2.14, 7.0.15,
 * 7.2.4, 7.4.4, 8.0.0, valkey-server 8.0.0, 9.0.0; #202), keyed by case and
 * profile, and the commands that build each case's key. A case's payload is
 * missing for a profile where its bytes are not deterministic (a hashtable
 * set on 6.2 / 7.0) or the case cannot be built (hash field TTLs).
 */
const PAYLOADS = JSON.parse(
  readFileSync(
    resolve(__dirname, '../tests/fixtures/dump-payloads.json'),
    'utf8',
  ),
) as Record<string, Partial<Record<ProfileName, string>>>

export const ALL_PROFILES: readonly ProfileName[] = [
  'redis-6.2',
  'redis-7.0',
  'redis-7.2',
  'redis-7.4',
  'redis-8.0',
  'valkey-8.0',
  'valkey-9.0',
]

/** RDB_VERSION, the version each profile writes in the payload footer. */
export const RDB_VERSION: Record<ProfileName, number> = {
  'redis-6.2': 9,
  'redis-7.0': 10,
  'redis-7.2': 11,
  'redis-7.4': 12,
  'redis-8.0': 12,
  'valkey-8.0': 11,
  'valkey-9.0': 80,
}

/** Whether a server of `profile` RESTOREs a payload of RDB `version`. */
export function acceptsVersion(profile: ProfileName, version: number): boolean {
  if (profile === 'valkey-9.0') {
    return (version >= 1 && version <= 11) || version === 80
  }
  return version <= RDB_VERSION[profile]
}

const HASH_FIELD_TTL: readonly ProfileName[] = [
  'redis-7.4',
  'redis-8.0',
  'valkey-9.0',
]

type Command = string[]

function numbered(fill: string, count: number): string[] {
  return Array.from(
    { length: count },
    (_, i) => `${String(i).padStart(3, '0')}${fill.repeat(100)}`,
  )
}

export type DumpCase = {
  /** The commands that build the key (`k` is its name). */
  setup: (k: string) => Command[]
  /** Profiles that can build it; every profile when absent. */
  profiles?: readonly ProfileName[]
  /**
   * False when a real server's bytes depend on its hash seed (a hashtable of
   * several entries iterates in a per-process order), so only RESTORE of the
   * captured payload is checked.
   */
  deterministic?: false
}

export const DUMP_CASES: Record<string, DumpCase> = {
  string: { setup: k => [['SET', k, 'v']] },
  string_int: { setup: k => [['SET', k, '123456789']] },
  string_negative: { setup: k => [['SET', k, '-5']] },
  string_int64: { setup: k => [['SET', k, '9223372036854775807']] },
  string_lzf: { setup: k => [['SET', k, 'x'.repeat(45)]] },
  string_lzf_text: { setup: k => [['SET', k, 'abcdefghij'.repeat(30)]] },
  string_raw: {
    setup: k => [['SET', k, '0123456789abcdefghijklmnopqrstuvwxyz']],
  },
  string_empty: { setup: k => [['SET', k, '']] },
  list: {
    setup: k => [['RPUSH', k, 'a', 'b', '1', '-300', '70000', '5000000000']],
  },
  list_nodes: { setup: k => [['RPUSH', k, ...numbered('q', 200)]] },
  list_pushes: { setup: k => numbered('p', 200).map(v => ['RPUSH', k, v]) },
  list_big_element: { setup: k => [['RPUSH', k, 'a', 'z'.repeat(9000), 'b']] },
  set_intset: {
    setup: k => [['SADD', k, '1', '2', '3', '-70000', '5000000000']],
  },
  set_listpack: { setup: k => [['SADD', k, 'a', 'b', '7', 'c']] },
  hash: {
    setup: k => [
      ['HSET', k, 'f', 'v', 'g', '2', 'n', '-300', 'big', '99999999999'],
    ],
  },
  hash_hashtable_value: { setup: k => [['HSET', k, 'f', 'v'.repeat(65)]] },
  zset: {
    setup: k => [
      ['ZADD', k, '1', 'a', '2.5', 'b', '-0', 'c', '0.1', 'd', '1e20', 'e'],
      ['ZADD', k, 'inf', 'f', '-inf', 'g', '5000000000', 'h'],
    ],
  },
  zset_skiplist: {
    setup: k => [
      [
        'ZADD',
        k,
        ...Array.from({ length: 130 }, (_, i) => [
          `${i}.5`,
          `m${String(i).padStart(3, '0')}`,
        ]).flat(),
      ],
    ],
  },
  zset_long_member: {
    setup: k => [['ZADD', k, '1', 'a', '2', 'm'.repeat(65)]],
  },
  stream: {
    setup: k => [
      ['XADD', k, '1-1', 'f', 'v'],
      ['XADD', k, '1-2', 'f', 'w'],
      ['XADD', k, '2-0', 'a', 'b', 'c', 'd'],
    ],
  },
  stream_nodes: {
    setup: k =>
      Array.from({ length: 250 }, (_, i) => [
        'XADD',
        k,
        `${i + 1}-1`,
        'f',
        `v${i}`,
      ]),
  },
  stream_bytes: {
    setup: k =>
      Array.from({ length: 20 }, (_, i) => [
        'XADD',
        k,
        `${i + 1}-0`,
        'field',
        'y'.repeat(300),
      ]),
  },
  stream_groups: {
    setup: k => [
      ['XADD', k, '1-1', 'f', 'v'],
      ['XADD', k, '1-2', 'f', 'w'],
      ['XGROUP', 'CREATE', k, 'g2', '$'],
      ['XGROUP', 'CREATE', k, 'g1', '0'],
    ],
  },
  stream_empty: {
    setup: k => [
      ['XADD', k, '1-1', 'f', 'v'],
      ['XDEL', k, '1-1'],
    ],
  },
  hash_ttl: {
    profiles: HASH_FIELD_TTL,
    setup: k => [
      ['HSET', k, 'f', 'v', 'g', 'w', 'h', 'x'],
      ['HPEXPIREAT', k, '9999999999999', 'FIELDS', '1', 'g'],
      ['HPEXPIREAT', k, '8888888888888', 'FIELDS', '1', 'h'],
    ],
  },
  hash_ttl_hashtable: {
    profiles: HASH_FIELD_TTL,
    deterministic: false,
    setup: k => [
      ['HSET', k, 'f', 'v'.repeat(65), 'g', 'w'],
      ['HPEXPIREAT', k, '9999999999999', 'FIELDS', '1', 'g'],
    ],
  },
}

/** The real payload of `name` on `profile`, if one was captured. */
export function realPayload(
  name: string,
  profile: ProfileName,
): Buffer | undefined {
  const hex = PAYLOADS[name]?.[profile]
  return hex === undefined ? undefined : Buffer.from(hex, 'hex')
}

export function caseAvailable(name: string, profile: ProfileName): boolean {
  const profiles = DUMP_CASES[name].profiles
  return profiles === undefined || profiles.includes(profile)
}
