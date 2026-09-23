import { describe, test } from 'node:test'
import assert from 'node:assert'
import {
  asciiLowerCase,
  asciiUpperCase,
  equalsAscii,
} from '../src/core/ascii-case'

// Code points, not literals, so a formatter can never normalize the
// look-alikes into the ASCII letters they are being tested against.
const KELVIN = String.fromCodePoint(0x212a) // lowercases to 'k' in JS
const LONG_S = String.fromCodePoint(0x017f) // uppercases to 'S' in JS
const DOTLESS_I = String.fromCodePoint(0x0131) // uppercases to 'I' in JS
const DOTTED_CAPITAL_I = String.fromCodePoint(0x0130) // lowercases to 'i' + U+0307 in JS

describe('ASCII-only case folding (#382)', () => {
  test('asciiLowerCase folds A-Z and nothing else', () => {
    assert.strictEqual(asciiLowerCase('GeT'), 'get')
    assert.strictEqual(asciiLowerCase('ZADD_@[`{09'), 'zadd_@[`{09')
    assert.strictEqual(asciiLowerCase(`${KELVIN}EYS`), `${KELVIN}eys`)
    assert.strictEqual(asciiLowerCase(DOTTED_CAPITAL_I), DOTTED_CAPITAL_I)
    assert.strictEqual(asciiLowerCase('ÀÉ'), 'ÀÉ')
    assert.strictEqual(asciiLowerCase(''), '')
  })

  test('asciiUpperCase folds a-z and nothing else', () => {
    assert.strictEqual(asciiUpperCase('sTrEaM'), 'STREAM')
    assert.strictEqual(asciiUpperCase('zadd_@[`{09'), 'ZADD_@[`{09')
    assert.strictEqual(asciiUpperCase(`${LONG_S}tream`), `${LONG_S}TREAM`)
    assert.strictEqual(asciiUpperCase(`${DOTLESS_I}nfo`), `${DOTLESS_I}NFO`)
    assert.strictEqual(asciiUpperCase('àé'), 'àé')
  })

  test('the Unicode folds these guard against really do hit ASCII', () => {
    // Pins the premise: if these ever stopped folding onto ASCII, the tests
    // above would still pass but no longer prove anything.
    assert.strictEqual(KELVIN.toLowerCase(), 'k')
    assert.strictEqual(LONG_S.toUpperCase(), 'S')
    assert.strictEqual(DOTLESS_I.toUpperCase(), 'I')
  })

  test('equalsAscii matches case-insensitively on ASCII only', () => {
    assert.strictEqual(equalsAscii(Buffer.from('FilterBy'), 'filterby'), true)
    assert.strictEqual(equalsAscii(Buffer.from('filterby'), 'filterby'), true)
    assert.strictEqual(equalsAscii(Buffer.from(`${KELVIN}eys`), 'keys'), false)
    assert.strictEqual(equalsAscii(Buffer.from('filterb'), 'filterby'), false)
  })
})
