import { describe, test } from 'node:test'
import assert from 'node:assert'
import { readFileSync } from 'node:fs'
import { resolve } from 'node:path'

import {
  formatGeoCoordinate,
  formatRedisDouble,
} from '../../src/core/double-format'
import { resolveCompatibilityProfile } from '../../src/core/compatibility'
import type { RedisFlavor } from '../../src/core/compatibility/profile'
import { RedisValue, encodeRedisValue } from '../../src/internal'

// Captured from real servers by scripts/capture-double-format-fixture.ts:
// every server printed exactly one of the two columns for every value.
type Fixture = {
  servers: Record<string, 'g17' | 'fpconv'>
  cases: [input: string, g17: string, fpconv: string][]
}
const fixture = JSON.parse(
  readFileSync(
    resolve(__dirname, '../fixtures/redis-double-format.json'),
    'utf8',
  ),
) as Fixture

function profileFor(server: string) {
  const [flavor, version] = server.split('-') as [RedisFlavor, string]
  return resolveCompatibilityProfile({ flavor, version })
}

describe('formatRedisDouble against captured real-server output (#451)', () => {
  for (const [server, style] of Object.entries(fixture.servers)) {
    test(`${server} (${style === 'g17' ? '%.17g' : 'd2string / fpconv_dtoa'})`, () => {
      const profile = profileFor(server)
      const column = style === 'g17' ? 1 : 2
      const mismatches: string[] = []
      for (const row of fixture.cases) {
        const actual = formatRedisDouble(Number(row[0]), profile)
        if (actual !== row[column]) {
          mismatches.push(`${row[0]}: expected ${row[column]}, got ${actual}`)
        }
      }
      assert.deepStrictEqual(
        mismatches.slice(0, 20),
        [],
        `${mismatches.length} / ${fixture.cases.length} mismatches`,
      )
    })
  }

  test('the fixture covers every compatibility preset version', () => {
    for (const server of [
      'redis-6.2',
      'redis-7.0',
      'redis-7.2',
      'redis-7.4',
      'redis-8.0',
      'valkey-8.0',
      'valkey-9.0',
    ]) {
      assert.ok(
        Object.keys(fixture.servers).some(s => s.startsWith(`${server}.`)),
        `no capture for ${server}`,
      )
    }
  })
})

describe('formatRedisDouble special values', () => {
  const old = resolveCompatibilityProfile('redis-6.2')
  const modern = resolveCompatibilityProfile('redis-8.0')

  for (const profile of [old, modern]) {
    test(`nan / inf / -inf / -0 on ${profile.flavor}-${profile.version}`, () => {
      assert.strictEqual(formatRedisDouble(NaN, profile), 'nan')
      assert.strictEqual(formatRedisDouble(Infinity, profile), 'inf')
      assert.strictEqual(formatRedisDouble(-Infinity, profile), '-inf')
      assert.strictEqual(formatRedisDouble(-0, profile), '-0')
      assert.strictEqual(formatRedisDouble(0, profile), '0')
    })
  }

  test('%.17g rounds a tie half to even, unlike toPrecision', () => {
    // 1234567890123456.25 is exact: the 18th digit is a bare 5.
    assert.strictEqual(
      formatRedisDouble(1234567890123456.25, old),
      '1234567890123456.2',
    )
    assert.strictEqual(
      formatRedisDouble(1234567890123456.75, old),
      '1234567890123456.8',
    )
  })

  test('%.17g switches to a two-digit exponent below 1e-4 and from 1e17', () => {
    assert.strictEqual(formatRedisDouble(0.0001, old), '0.0001')
    assert.strictEqual(
      formatRedisDouble(0.00001, old),
      '1.0000000000000001e-05',
    )
    assert.strictEqual(formatRedisDouble(1e16, old), '10000000000000000')
    assert.strictEqual(formatRedisDouble(1e17, old), '1e+17')
    assert.strictEqual(
      formatRedisDouble(-1e300, old),
      '-1.0000000000000001e+300',
    )
  })

  test('defaults to the default profile (redis-8.0) spelling', () => {
    assert.strictEqual(formatRedisDouble(0.1), '0.1')
    assert.strictEqual(formatRedisDouble(2 ** 62), '4611686018427387904')
  })
})

describe('RESP encoder double text follows the profile', () => {
  const old = resolveCompatibilityProfile('redis-7.0')
  const modern = resolveCompatibilityProfile('redis-7.2')

  test('RESP2 bulk string', () => {
    assert.deepStrictEqual(
      encodeRedisValue(RedisValue.double(0.1), { version: 2, profile: old }),
      Buffer.from('$19\r\n0.10000000000000001\r\n'),
    )
    assert.deepStrictEqual(
      encodeRedisValue(RedisValue.double(0.1), { version: 2, profile: modern }),
      Buffer.from('$3\r\n0.1\r\n'),
    )
  })

  test('RESP3 double', () => {
    assert.deepStrictEqual(
      encodeRedisValue(RedisValue.double(1e-5), { version: 3, profile: old }),
      Buffer.from(',1.0000000000000001e-05\r\n'),
    )
    assert.deepStrictEqual(
      encodeRedisValue(RedisValue.double(1e-5), {
        version: 3,
        profile: modern,
      }),
      Buffer.from(',0.00001\r\n'),
    )
  })

  test('an explicit text wins over the profile spelling', () => {
    assert.deepStrictEqual(
      encodeRedisValue(RedisValue.double(0.1, '0.1000'), {
        version: 3,
        profile: old,
      }),
      Buffer.from(',0.1000\r\n'),
    )
  })
})

describe('formatGeoCoordinate (#451)', () => {
  // Values read back with GEOPOS from real redis 7.4.4 / 8.0.0 / valkey 9.0.0.
  const human = resolveCompatibilityProfile('redis-7.4')
  const valkey = resolveCompatibilityProfile('valkey-9.0')
  const modern = resolveCompatibilityProfile('redis-8.0')

  test('%.17Lf trimmed before Redis 8.0 and on every Valkey', () => {
    for (const profile of [human, valkey]) {
      assert.strictEqual(
        formatGeoCoordinate(13.361389338970184, profile),
        '13.36138933897018433',
      )
      assert.strictEqual(
        formatGeoCoordinate(4.9427062607109546e-5, profile),
        '0.00004942706260711',
      )
      assert.strictEqual(formatGeoCoordinate(-0, profile), '0')
      assert.strictEqual(formatGeoCoordinate(-12.5, profile), '-12.5')
    }
  })

  test('d2string on Redis 8.0', () => {
    assert.strictEqual(
      formatGeoCoordinate(13.361389338970184, modern),
      '13.361389338970184',
    )
    assert.strictEqual(
      formatGeoCoordinate(4.9427062607109546e-5, modern),
      '4.9427062607109546e-5',
    )
  })
})
