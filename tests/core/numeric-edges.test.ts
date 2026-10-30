import { describe, test } from 'node:test'
import assert from 'node:assert'

import { parseBlockingTimeout } from '../../src/commands/blocking'
import {
  keyExpireDeadlineMs,
  stringExpireDeadlineMs,
} from '../../src/commands/helpers'
import { resolveCompatibilityProfile } from '../../src/core/compatibility/profile'
import { parseLongDoubleMagnitude } from '../../src/core/long-double-token'

const redis62 = resolveCompatibilityProfile('redis-6.2')
const redis70 = resolveCompatibilityProfile('redis-7.0')
const redis80 = resolveCompatibilityProfile('redis-8.0')

const message = (fn: () => unknown): string => {
  try {
    fn()
  } catch (error) {
    return (error as Error).message
  }
  return 'no error'
}

describe('parseLongDoubleMagnitude', () => {
  test('takes long doubles beyond the double range as infinities', () => {
    assert.strictEqual(parseLongDoubleMagnitude('1e400'), Infinity)
    assert.strictEqual(parseLongDoubleMagnitude('-1e400'), -Infinity)
    assert.strictEqual(parseLongDoubleMagnitude('0x1p1024'), Infinity)
    assert.strictEqual(parseLongDoubleMagnitude('1.18e4932'), Infinity)
    assert.strictEqual(
      parseLongDoubleMagnitude('0x1.fffffffffffffffep16383'),
      Infinity,
    )
  })

  test('refuses what overflows a long double, and everything string2ld refuses', () => {
    for (const token of [
      '1e4933',
      '1.19e4932',
      '0x1p16384',
      '1e-5000',
      ' 1',
      '1 ',
      '',
      'nan',
      '0b11',
      '0x',
      '1e',
    ]) {
      assert.strictEqual(parseLongDoubleMagnitude(token), undefined, token)
    }
  })

  test('agrees with parseLongDoubleToken inside the double range', () => {
    assert.strictEqual(parseLongDoubleMagnitude('0x1.8p1'), 3)
    assert.strictEqual(parseLongDoubleMagnitude('-inf'), -Infinity)
    assert.strictEqual(parseLongDoubleMagnitude('1e308'), 1e308)
  })
})

describe('parseBlockingTimeout', () => {
  const parse = (token: string, profile = redis80) =>
    parseBlockingTimeout(Buffer.from(token), profile)

  test('returns whole milliseconds, rounded up from 7.0.8', () => {
    assert.strictEqual(parse('1'), 1000)
    assert.strictEqual(parse('0x1.8p1'), 3000)
    assert.strictEqual(parse('0.0001'), 1)
    assert.strictEqual(parse('0.0001', redis70), 1)
    assert.strictEqual(parse('0.0001', redis62), 0)
    assert.strictEqual(parse('-0.0001'), 0)
    assert.strictEqual(parse('0'), 0)
  })

  test('refuses an end time past LLONG_MAX per profile', () => {
    for (const token of ['inf', '1e400', '1e19', '9223372036854775807']) {
      assert.strictEqual(
        message(() => parse(token)),
        'timeout is out of range',
      )
      assert.strictEqual(
        message(() => parse(token, redis62)),
        'timeout is negative',
      )
    }
    assert.strictEqual(
      message(() => parse('-1e400')),
      'timeout is negative',
    )
    assert.strictEqual(
      message(() => parse(' 1')),
      'timeout is not a float or out of range',
    )
    // 6.2 / 7.0: an end time that wraps is already over.
    assert.strictEqual(parse('9223371000000000', redis62), 1)
  })
})

describe('expire deadlines', () => {
  const now = 1_700_000_000_000
  const set = (token: string, unit: 'EX' | 'PX' | 'EXAT' | 'PXAT') =>
    message(() =>
      stringExpireDeadlineMs(Buffer.from(token), unit, 'set', redis80, now),
    )

  test('SET refuses int64 times that overflow', () => {
    const invalid = "invalid expire time in 'set' command"
    assert.strictEqual(set('9223372036854775807', 'EX'), invalid)
    assert.strictEqual(set('9223372036854775', 'EX'), invalid)
    assert.strictEqual(set('9223372036854775807', 'PX'), invalid)
    assert.strictEqual(set('9223372036854776', 'EXAT'), invalid)
    assert.strictEqual(set('0', 'PXAT'), invalid)
    assert.strictEqual(set('9223372036854775', 'EXAT'), 'no error')
    assert.strictEqual(set('9223372036854775807', 'PXAT'), 'no error')
    assert.strictEqual(
      set('9223372036854775808', 'PXAT'),
      'value is not an integer or out of range',
    )
  })

  test('EXPIRE refuses only conversions and additions that overflow', () => {
    const expire = (token: string, seconds: boolean, base: number) =>
      message(() =>
        keyExpireDeadlineMs(
          Buffer.from(token),
          seconds,
          base,
          'expire',
          redis62,
        ),
      )
    const invalid = 'invalid expire time in expire'
    assert.strictEqual(expire('9223372036854775807', true, now), invalid)
    assert.strictEqual(expire('-9223372036854775808', true, now), invalid)
    assert.strictEqual(expire('-9223372036854775', true, now), 'no error')
    assert.strictEqual(expire('9223372036854775807', false, now), invalid)
    assert.strictEqual(expire('9223372036854775807', false, 0), 'no error')
    assert.strictEqual(expire('-9223372036854775808', false, 0), 'no error')
    assert.strictEqual(expire('9223372036854776', true, 0), invalid)
  })
})
