import { describe, test } from 'node:test'
import assert from 'node:assert'

import { parseLongDoubleToken } from '../../src/core/long-double-token'

/**
 * `parseLongDoubleToken` against real Redis' `string2ld()` (strtold). Every
 * accept/refuse row below was captured with `INCRBYFLOAT` and `HINCRBYFLOAT`
 * on redis 6.2, 7.0, 7.2, 7.4 and 8.0 and valkey 7.2, 8.0 and 9.0 (amd64
 * images); all eight answered identically (#234).
 */
describe('parseLongDoubleToken', () => {
  test('parses C99 hex floats like strtold', () => {
    const cases: [string, number][] = [
      ['0x10', 16],
      ['0X10', 16],
      ['-0x10', -16],
      ['+0x10', 16],
      ['0x10p0', 16],
      ['0x1.8p3', 12],
      ['0x1.8P+3', 12],
      ['0x1P3', 8],
      ['0x1p+3', 8],
      ['0x1p-0', 1],
      ['0x1p-2', 0.25],
      ['0x.8', 0.5],
      ['0x1.p1', 2],
      ['0xAbC.dEf', 2748.870849609375],
      // `e` is a hex digit, not an exponent marker.
      ['0x1e5', 485],
      ['0x1.8e', 1.5546875],
      ['0x1p-1074', 5e-324],
      ['0x1.fffffffffffffp1023', Number.MAX_VALUE],
    ]
    for (const [token, expected] of cases) {
      assert.strictEqual(parseLongDoubleToken(token), expected, token)
    }
  })

  test('rounds a hex mantissa wider than a double to nearest, ties to even', () => {
    // 1 + 2^-53 is exactly halfway between 1 and the next double: even wins.
    assert.strictEqual(parseLongDoubleToken('0x1.00000000000008p0'), 1)
    // Just above halfway rounds up.
    assert.strictEqual(
      parseLongDoubleToken('0x1.00000000000008000001p0'),
      1 + 2 ** -52,
    )
    // 1 + 3 * 2^-53 is halfway with an odd lower neighbour: rounds up.
    assert.strictEqual(
      parseLongDoubleToken('0x1.00000000000018p0'),
      1 + 2 * 2 ** -52,
    )
    // Subnormal range: half the smallest subnormal ties to zero, a hair more
    // rounds up to it.
    assert.strictEqual(parseLongDoubleToken('0x1p-1075'), 0)
    assert.strictEqual(parseLongDoubleToken('0x1.8p-1075'), 5e-324)
    assert.strictEqual(parseLongDoubleToken('0x1.8p-1074'), 1e-323)
  })

  test('keeps plain decimals, a leading zero is still decimal', () => {
    const cases: [string, number][] = [
      ['1', 1],
      ['010', 10],
      ['1.', 1],
      ['.5', 0.5],
      ['-.5e1', -5],
      ['1e5', 100000],
      ['1e+5', 100000],
      ['0e-99999', 0],
      ['1' + '.' + '0'.repeat(5117), 1],
    ]
    for (const [token, expected] of cases) {
      assert.strictEqual(parseLongDoubleToken(token), expected, token)
    }
    assert.ok(Object.is(parseLongDoubleToken('-0x0'), -0))
    assert.ok(Object.is(parseLongDoubleToken('0x0p-99999'), 0))
  })

  test('returns infinity for the infinity literals, in any case and sign', () => {
    for (const token of [
      'inf',
      'INF',
      'Inf',
      '+inf',
      'infinity',
      '+infinity',
    ]) {
      assert.strictEqual(parseLongDoubleToken(token), Infinity, token)
    }
    for (const token of ['-inf', '-INFINITY']) {
      assert.strictEqual(parseLongDoubleToken(token), -Infinity, token)
    }
  })

  test('refuses what string2ld refuses', () => {
    const invalid = [
      '',
      '0x',
      '0x.',
      '0x1p',
      '0x1p3.5',
      '0xg',
      '00x10',
      '0x-10',
      '0x+10',
      // Not C syntax, although JavaScript's Number() takes them.
      '0b11',
      '0o7',
      '1_0',
      // Whitespace on either side, and anything after the number.
      ' 0x10',
      '0x10 ',
      ' 1',
      '1 ',
      '1\0abc',
      '3abc',
      '1,5',
      '1e',
      '.',
      // NaN in every strtold spelling.
      'nan',
      '-nan',
      'NaN',
      'nan(1)',
      'infinit',
      'infx',
      // Overflows long double.
      '1e5000',
      '1e99999999999999999999',
      '0x1p16384',
      '0x1p99999',
      // A nonzero value that underflows long double to zero.
      '1e-4952',
      '-1e-99999',
      '1e-99999999999999999999',
      '00000.00001e-4946',
      '0x1p-16446',
      '0x1p-99999',
      '0x1p-99999999999999999999',
      // 5120 bytes: one past string2ld's buffer.
      '1' + '.' + '0'.repeat(5118),
    ]
    for (const token of invalid) {
      assert.strictEqual(
        parseLongDoubleToken(token),
        undefined,
        JSON.stringify(token.length > 40 ? `${token.slice(0, 40)}...` : token),
      )
    }
  })

  test('places the underflow edge at 2^-16446, like an 80-bit long double', () => {
    // 2^-16446 is 1.82259...e-4951, and it rounds (ties to even) to zero.
    assert.strictEqual(parseLongDoubleToken('1.8225e-4951'), undefined)
    assert.strictEqual(parseLongDoubleToken('1.8226e-4951'), 0)
    assert.strictEqual(parseLongDoubleToken('1.9e-4951'), 0)
    assert.strictEqual(parseLongDoubleToken('1e-4950'), 0)
    assert.strictEqual(parseLongDoubleToken('0x1p-16446'), undefined)
    assert.strictEqual(parseLongDoubleToken('0x1.0000001p-16446'), 0)
    assert.strictEqual(parseLongDoubleToken('0x1p-16445'), 0)
    assert.strictEqual(
      parseLongDoubleToken('0x0.0000000000000000000001p-16350'),
      0,
    )
  })

  test('refuses values past the double range, which it cannot hold (#512)', () => {
    // Real Redis accepts these as long doubles; a double cannot represent them.
    for (const token of ['1e400', '0x1p1024', '0x1p16383']) {
      assert.strictEqual(parseLongDoubleToken(token), undefined, token)
    }
  })

  test('reads a Buffer byte for byte', () => {
    assert.strictEqual(parseLongDoubleToken(Buffer.from('0x10')), 16)
    assert.strictEqual(
      parseLongDoubleToken(Buffer.from([0x31, 0x00])),
      undefined,
    )
    // A non-ASCII byte is never part of a number.
    assert.strictEqual(
      parseLongDoubleToken(Buffer.from([0x31, 0xb2])),
      undefined,
    )
  })
})
