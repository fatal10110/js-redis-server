import { describe, test } from 'node:test'
import assert from 'node:assert'

import { resolveCompatibilityProfile } from '../../src/core/compatibility/profile'
import {
  parseScoreRangeToken,
  parseSortScoreToken,
  parseString2dToken,
} from '../../src/core/double-token'

/**
 * The three double converters against real servers (#534, #513). Rows were
 * captured with ZADD/ZSCORE, ZRANGEBYSCORE/ZCOUNT and SORT on redis-server
 * 6.2.14, 7.0.15, 7.2.4, 7.4.4, 8.0.0 and valkey-server 8.0.0, 9.0.0, each
 * SORT row in a fresh server (stale errno, see src/core/double-token.ts).
 */
const legacy = resolveCompatibilityProfile('redis-7.4')
const redis8 = resolveCompatibilityProfile('redis-8.0')
const valkey8 = resolveCompatibilityProfile('valkey-8.0')
const valkey9 = resolveCompatibilityProfile('valkey-9.0')
const valkey81 = resolveCompatibilityProfile({
  flavor: 'valkey',
  version: '8.1.0',
})
const all = [legacy, redis8, valkey8, valkey9]

describe('parseString2dToken', () => {
  test('decimal, infinity spellings and subnormals are accepted everywhere', () => {
    for (const profile of all) {
      for (const [raw, value] of [
        ['1', 1],
        ['.5', 0.5],
        ['5.', 5],
        ['1e5', 1e5],
        ['-0', -0],
        ['inf', Infinity],
        ['+INF', Infinity],
        ['-Infinity', -Infinity],
        ['infinity', Infinity],
        ['1e-310', 1e-310],
      ] as const) {
        assert.strictEqual(parseString2dToken(raw, profile), value, raw)
      }
    }
  })

  test('spaces, garbage, NaN and range errors are refused everywhere', () => {
    for (const profile of all) {
      for (const raw of [
        '',
        ' 1',
        '\t1',
        '1 ',
        'nan',
        '1e400',
        '-1e400',
        '1e-400',
        '0x',
        '0b11',
        '1_0',
        '0x1p9999',
        '0x1p-2000',
        '1\0',
        '.',
        '-',
        '1e',
        'infinit',
      ]) {
        assert.strictEqual(parseString2dToken(raw, profile), undefined, raw)
      }
    }
  })

  test('hex floats are refused only from Valkey 8.1', () => {
    const rows = [
      ['0x10', 16],
      ['0x1p3', 8],
      ['0X1P-2', 0.25],
      ['0x1.8', 1.5],
      ['-0x10', -16],
      ['+0x10', 16],
      ['0x1e5', 0x1e5],
      ['0x1p-1074', 5e-324],
    ] as const
    for (const profile of [legacy, redis8, valkey8]) {
      for (const [raw, value] of rows) {
        assert.strictEqual(parseString2dToken(raw, profile), value, raw)
      }
    }
    for (const profile of [valkey81, valkey9]) {
      for (const [raw] of rows) {
        assert.strictEqual(parseString2dToken(raw, profile), undefined, raw)
      }
    }
  })

  test('takes a Buffer as latin1 bytes', () => {
    assert.strictEqual(parseString2dToken(Buffer.from('2.5'), redis8), 2.5)
    assert.strictEqual(
      parseString2dToken(Buffer.from([0x31, 0xff]), redis8),
      undefined,
    )
  })
})

describe('parseScoreRangeToken', () => {
  test('same on every profile', () => {
    for (const profile of all) {
      for (const [raw, value] of [
        ['', 0],
        ['1', 1],
        ['1e400', Infinity],
        ['-1e400', -Infinity],
        ['1e-400', 0],
        ['inf', Infinity],
        ['INFINITY', Infinity],
        ['-infinity', -Infinity],
        ['+INF', Infinity],
        ['5\0', 5],
        ['5\0junk', 5],
      ] as const) {
        assert.strictEqual(parseScoreRangeToken(raw, profile), value, raw)
      }
      for (const raw of ['1 ', '5\t', 'nan', '0x', '"', '\t', ' ', 'abc']) {
        assert.strictEqual(parseScoreRangeToken(raw, profile), undefined, raw)
      }
    }
  })

  test('hex floats and leading whitespace only before Redis 8.0 / Valkey 8.1', () => {
    const rows = [
      ['0x1', 1],
      ['0x1p3', 8],
      ['0x20', 32],
      [' 1', 1],
      [' 0x10', 16],
      ['\t1', 1],
    ] as const
    for (const profile of [legacy, valkey8]) {
      for (const [raw, value] of rows) {
        assert.strictEqual(parseScoreRangeToken(raw, profile), value, raw)
      }
    }
    for (const profile of [redis8, valkey81, valkey9]) {
      for (const [raw] of rows) {
        assert.strictEqual(parseScoreRangeToken(raw, profile), undefined, raw)
      }
    }
  })
})

describe('parseSortScoreToken', () => {
  test('same on every profile', () => {
    for (const profile of all) {
      for (const [raw, value] of [
        ['inf', Infinity],
        ['-inf', -Infinity],
        ['+inf', Infinity],
        ['INF', Infinity],
        ['Infinity', Infinity],
        ['infinity', Infinity],
        ['+3', 3],
        ['.5', 0.5],
        ['5.', 5],
        ['1e5', 1e5],
      ] as const) {
        assert.strictEqual(parseSortScoreToken(raw, profile), value, raw)
      }
      for (const raw of [
        '1 ',
        'nan',
        'NaN',
        '0b11',
        '1_0',
        '0x',
        '1e',
        '0x1p',
      ]) {
        assert.strictEqual(parseSortScoreToken(raw, profile), undefined, raw)
      }
    }
  })

  test('hex floats and leading whitespace only before Redis 8.0 / Valkey 8.1', () => {
    const rows = [
      ['0x10', 16],
      ['0x1p3', 8],
      ['-0x10', -16],
      ['0x1.8', 1.5],
      [' 1', 1],
    ] as const
    for (const profile of [legacy, valkey8]) {
      for (const [raw, value] of rows) {
        assert.strictEqual(parseSortScoreToken(raw, profile), value, raw)
      }
    }
    for (const profile of [redis8, valkey9]) {
      for (const [raw] of rows) {
        assert.strictEqual(parseSortScoreToken(raw, profile), undefined, raw)
      }
    }
  })

  test('an empty element is 0, except on Valkey 8.1+', () => {
    for (const profile of [legacy, redis8, valkey8]) {
      assert.strictEqual(parseSortScoreToken('', profile), 0)
      assert.strictEqual(parseSortScoreToken('\0x', profile), 0)
    }
    assert.strictEqual(parseSortScoreToken('', valkey9), undefined)
    assert.strictEqual(parseSortScoreToken('', valkey81), undefined)
    assert.strictEqual(parseSortScoreToken(' ', legacy), undefined)
  })

  test('ERANGE: refused by strtod and valkey_strtod, ignored by Redis 8.0', () => {
    for (const raw of ['1e400', '-1e400', '1e-400', '1e-310']) {
      assert.strictEqual(parseSortScoreToken(raw, legacy), undefined, raw)
      assert.strictEqual(parseSortScoreToken(raw, valkey8), undefined, raw)
    }
    assert.strictEqual(parseSortScoreToken('1e400', redis8), Infinity)
    assert.strictEqual(parseSortScoreToken('-1e400', redis8), -Infinity)
    assert.strictEqual(parseSortScoreToken('1e-400', redis8), 0)
    assert.strictEqual(parseSortScoreToken('1e-310', redis8), 1e-310)
    for (const raw of ['1e400', '-1e400', '1e-400']) {
      assert.strictEqual(parseSortScoreToken(raw, valkey9), undefined, raw)
    }
    assert.strictEqual(parseSortScoreToken('1e-310', valkey9), 1e-310)
    // glibc only flags an inexact subnormal: 2^-1074 written in hex is exact.
    assert.strictEqual(parseSortScoreToken('0x1p-1074', legacy), 5e-324)
    assert.strictEqual(parseSortScoreToken('0x1.8p-1074', legacy), undefined)
    assert.strictEqual(parseSortScoreToken('0x1p-1022', legacy), 2 ** -1022)
    assert.strictEqual(
      parseSortScoreToken('2.2250738585072011e-308', legacy),
      undefined,
    )
  })
})
