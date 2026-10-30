import assert from 'node:assert/strict'
import { describe, test } from 'node:test'

import type { RedisSortedSetData } from '../src/state/data-types'
import {
  DEFAULT_ZSET_ENCODING_RULES,
  prepareZsetForAdd,
  storedZsetByInsertion,
  storedZsetConvertedIfFits,
  writeZsetScore,
  zsetEncoding,
  zsetScanOrder,
  type ZsetEncodingRules,
} from '../src/state/zset-encoding'

// The rules of t_zset.c the model mirrors (#512). Each profile's rules:
const REDIS_6_2: ZsetEncodingRules = {
  ...DEFAULT_ZSET_ENCODING_RULES,
  sizeHint: false,
  integerScores: false,
}
const REDIS_7_0: ZsetEncodingRules = {
  ...DEFAULT_ZSET_ENCODING_RULES,
  sizeHint: false,
}
const REDIS_7_2 = DEFAULT_ZSET_ENCODING_RULES
const VALKEY_8_0: ZsetEncodingRules = {
  ...DEFAULT_ZSET_ENCODING_RULES,
  longestMemberHint: true,
}

const LONG = Buffer.alloc(65, 'a')

function zadd(
  zset: RedisSortedSetData,
  rules: ZsetEncodingRules,
  pairs: [number, Buffer | string][],
): void {
  const members = pairs.map(([, member]) => Buffer.from(member))
  prepareZsetForAdd(zset, members, rules)
  pairs.forEach(([score], i) => {
    writeZsetScore(zset, members[i]!, score, rules)
  })
}

function scoreOf(zset: RedisSortedSetData, member: string): number {
  return zset.members.get(Buffer.from(member).toString('hex'))!.score
}

function newZset(): RedisSortedSetData {
  return { type: 'zset', members: new Map() }
}

describe('zset encoding model (#512)', () => {
  test('a small set is a listpack; -0 is stored as 0 from 7.0', () => {
    for (const [rules, expected] of [
      [REDIS_6_2, -0],
      [REDIS_7_0, 0],
      [REDIS_7_2, 0],
    ] as const) {
      const zset = newZset()
      zadd(zset, rules, [[-0, 'a']])
      assert.equal(zsetEncoding(zset), 'listpack')
      assert.ok(Object.is(scoreOf(zset, 'a'), expected))
    }
  })

  test('a 129th member or a 65-byte member converts, for good', () => {
    const zset = newZset()
    zadd(
      zset,
      REDIS_7_0,
      Array.from({ length: 128 }, (_, i): [number, string] => [i, `m${i}`]),
    )
    assert.equal(zsetEncoding(zset), 'listpack')
    zadd(zset, REDIS_7_0, [[1, 'm0']])
    assert.equal(zsetEncoding(zset), 'listpack', 'an update never converts')
    zadd(zset, REDIS_7_0, [[-0, 'extra']])
    assert.equal(zsetEncoding(zset), 'skiplist')
    assert.ok(Object.is(scoreOf(zset, 'extra'), -0))

    zset.members.clear()
    zset.members.set('00', { member: Buffer.from([0]), score: 1 })
    zadd(zset, REDIS_7_0, [[-0, 'n']])
    assert.equal(zsetEncoding(zset), 'skiplist', 'shrinking keeps it')
    assert.ok(Object.is(scoreOf(zset, 'n'), -0))

    const long = newZset()
    zadd(long, REDIS_7_0, [[1, 'a']])
    zadd(long, REDIS_7_0, [[2, LONG]])
    assert.equal(zsetEncoding(long), 'skiplist')
  })

  test('7.2+: more ZADD pairs than the limit make a skiplist up front', () => {
    const pairs = Array.from({ length: 129 }, (): [number, string] => [-0, 'm'])
    const before = newZset()
    zadd(before, REDIS_7_0, pairs)
    assert.equal(zsetEncoding(before), 'listpack')
    assert.ok(Object.is(scoreOf(before, 'm'), 0))

    const after = newZset()
    zadd(after, REDIS_7_2, pairs)
    assert.equal(zsetEncoding(after), 'skiplist')
    assert.ok(Object.is(scoreOf(after, 'm'), -0))

    const existing = newZset()
    zadd(existing, REDIS_7_2, [[1, 'x']])
    zadd(existing, REDIS_7_2, pairs)
    assert.equal(zsetEncoding(existing), 'skiplist')
  })

  test('creation reads the first member (Redis) or the longest (Valkey 8)', () => {
    const redis = newZset()
    zadd(redis, REDIS_7_2, [
      [-0, 'a'],
      [1, LONG],
    ])
    assert.equal(zsetEncoding(redis), 'skiplist')
    assert.ok(Object.is(scoreOf(redis, 'a'), 0))

    const valkey = newZset()
    zadd(valkey, VALKEY_8_0, [
      [-0, 'a'],
      [1, LONG],
    ])
    assert.ok(Object.is(scoreOf(valkey, 'a'), -0))

    const first = newZset()
    zadd(first, REDIS_7_0, [
      [1, LONG],
      [-0, 'a'],
    ])
    assert.ok(Object.is(scoreOf(first, 'a'), -0))
  })

  test('Valkey 8 converts an existing listpack for a long member it skips', () => {
    // ZADD k XX 0.2 <65 bytes>: nothing is added.
    for (const [rules, expected] of [
      [REDIS_7_2, 'listpack'],
      [VALKEY_8_0, 'skiplist'],
    ] as const) {
      const zset = newZset()
      zadd(zset, rules, [[0.1, 'a']])
      prepareZsetForAdd(zset, [LONG], rules)
      assert.equal(zsetEncoding(zset), expected)
    }
  })

  test('STORE results are a listpack again when they fit', () => {
    const fits = storedZsetConvertedIfFits(
      new Map([['61', { member: Buffer.from('a'), score: -0 }]]),
      REDIS_7_0,
    )
    assert.equal(zsetEncoding(fits), 'listpack')
    assert.ok(Object.is(scoreOf(fits, 'a'), 0))

    const long = storedZsetConvertedIfFits(
      new Map([
        ['61', { member: Buffer.from('a'), score: -0 }],
        [LONG.toString('hex'), { member: LONG, score: 1 }],
      ]),
      REDIS_7_0,
    )
    assert.equal(zsetEncoding(long), 'skiplist')
    assert.ok(Object.is(scoreOf(long, 'a'), -0))

    // ZRANGESTORE inserts one by one: `a` is stored as 0 before the long
    // member converts the listpack.
    const ranged = storedZsetByInsertion(
      [
        { member: Buffer.from('a'), score: -0 },
        { member: LONG, score: 1 },
      ],
      REDIS_7_0,
    )
    assert.equal(zsetEncoding(ranged), 'skiplist')
    assert.ok(Object.is(scoreOf(ranged, 'a'), 0))
  })

  test('ZSCAN walks a listpack in (score, member) order', () => {
    const zset = newZset()
    zadd(zset, REDIS_6_2, [
      [3, 'c'],
      [1, 'b'],
      [1, 'a'],
      [2, 'aa'],
      [-0, 'z'],
      [Infinity, 'i'],
      [-Infinity, 'j'],
    ])
    assert.deepEqual(
      zsetScanOrder(zset).map(entry => entry.member.toString()),
      ['j', 'z', 'a', 'b', 'aa', 'c', 'i'],
    )
    zset.encoding = 'skiplist'
    assert.deepEqual(
      zsetScanOrder(zset).map(entry => entry.member.toString()),
      ['c', 'b', 'a', 'aa', 'z', 'i', 'j'],
    )
  })
})
