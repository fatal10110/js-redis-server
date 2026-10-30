import assert from 'node:assert/strict'
import { describe, test } from 'node:test'

import { formatIncrByFloatResult } from '../../src/core/double-format'
import {
  addLongDoubles,
  parseLongDouble,
} from '../../src/core/long-double-token'

// `SET k <stored>` then `INCRBYFLOAT k <increment>` on real redis 6.2.14,
// 8.0.0 and valkey 9.0.0 (identical on every profile: x86-64 80-bit
// `long double`), #512. `undefined` stored means a missing key; null expected
// means "increment would produce NaN or Infinity".
function incr(stored: string | undefined, increment: string): string | null {
  const current = parseLongDouble(stored ?? '0')
  const step = parseLongDouble(increment)
  assert.ok(current && step, `${stored} + ${increment} parses`)
  const sum = addLongDoubles(current, step)
  return sum === undefined ? null : formatIncrByFloatResult(sum)
}

const E400 = [
  '1000000000000000000028188068394758651458645343362905203862591069',
  '3539685534008629862039363994848324160522094053927317616200295822',
  '7772592557340238289765933406610177974474345461739178624481166749',
  '7172377894382439159333804747067502624668440135923751360383034373',
  '5485505244955964979021825038280091068414947402456898653040951017',
  '5126580926158275889201834725116433165913626641381763097348063437',
  '32497430221946880',
].join('')

const P1024 = [
  '1797693134862315907729305190789024733617976978942306572734300811',
  '5773267580550096313270847732240753602112011387987139335765878976',
  '8814416622492847430639474124377767893424865485276302219601246094',
  '1194530829520850057688381506823424628814739131105408272371633505',
  '10684586298239947245938479716304835356329624224137216',
].join('')

const TWO_E308 = [
  '1999999999999999999933717593116912913211201996948311344155940959',
  '8984346973767612374420025384377707864089349445010802644630426949',
  '9187921167194841628860392837535918200039206381557326219209014213',
  '3358783067915778778291210871261225367298032372604341731785068897',
  '63247582601711514636284849020905456510092687857156096',
].join('')

const TWO_DBL_MAX = [
  '3595386269724631399918420935482088680967728938886583569706288415',
  '3035325704624879270940373721677417112885758683885104137861741672',
  '6902272110715794556052955234146997955372576789671236326588091188',
  '8698590748945812549613388347692977582455524366671979077056642065',
  '65009503223382416235440319971852753604933726679072768',
].join('')

describe('long double INCRBYFLOAT arithmetic (#512)', () => {
  test('operands and the sum round to a 64-bit significand', () => {
    for (const [stored, increment, expected] of [
      ['0.1', '0.2', '0.3'],
      ['1234.5678', '0', '1234.56779999999999997'],
      ['1.1', '2.2', '3.3'],
      ['9007199254740993', '1', '9007199254740994'],
      ['0.7', '0.1', '0.8'],
      ['0.8', '0.1', '0.9'],
      ['0.9', '0.1', '1'],
      ['100', '1.23456789012345678', '101.23456789012345678'],
      ['5.5', '4.5e-17', '5.50000000000000005'],
      ['3', '-0.1', '2.9'],
      ['10.5', '0.1', '10.6'],
      ['-5', '5', '0'],
      [undefined, '0.12345678901234567890123', '0.12345678901234568'],
      [undefined, '-1e-400', '0'],
      ['0', '1.5e-17', '0.00000000000000002'],
      ['0x1p-16445', '0x1p-16445', '0'],
    ] as const) {
      assert.equal(
        incr(stored, increment),
        expected,
        `${stored} + ${increment}`,
      )
    }
  })

  test('values past the double range are exact integers', () => {
    assert.equal(incr(undefined, '1e400'), E400)
    assert.equal(incr(undefined, '0x1p1024'), P1024)
    assert.equal(incr('1e308', '1e308'), TWO_E308)
    // The stored text re-parses to the same long double.
    assert.equal(incr(TWO_E308, '1'), TWO_E308)
    assert.equal(
      incr('1.7976931348623157e308', '1.7976931348623157e308'),
      TWO_DBL_MAX,
    )
  })

  test('the long double range ends near 1.19e4932', () => {
    const max = incr(undefined, '1.18973149535723176502e+4932')!
    assert.equal(max.length, 4933)
    assert.ok(
      max.startsWith(
        '1189731495357231765021263853030970205169063322294624200440323733891737005522970722616410290336528882',
      ),
    )
    assert.equal(incr(undefined, '-0x1.fffffffffffffffep16383'), `-${max}`)
    assert.equal(incr(max, '1e4932'), null)
    assert.equal(incr('1e4932', '1e4932'), null)
    assert.equal(parseLongDouble('1e4933'), undefined)
    assert.equal(parseLongDouble('0x1p16384'), undefined)
  })

  test('an infinite operand gives the NaN/Infinity error', () => {
    assert.equal(incr(undefined, 'inf'), null)
    assert.equal(incr('-inf', '1'), null)
    assert.equal(parseLongDouble('nan'), undefined)
  })
})
