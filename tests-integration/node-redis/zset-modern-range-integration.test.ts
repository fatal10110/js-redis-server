import { test, describe, before, after } from 'node:test'
import assert from 'node:assert'
import { RedisClusterType } from 'redis'
import { TestRunner } from '../test-config'
import {
  connectToNodeRedisSlotOwner,
  errorWithMessage,
  flushNodeRedisCluster,
  randomKey,
} from '../utils'

const testRunner = new TestRunner()

describe(`Sorted Set Modern Range / ZMSCORE / ZRANDMEMBER (node-redis, ${testRunner.getBackendName()})`, () => {
  let redisClient: RedisClusterType

  before(async () => {
    redisClient = (await testRunner.setupNodeRedisCluster()) as RedisClusterType
    await flushNodeRedisCluster(redisClient)
  })

  after(async () => {
    await testRunner.cleanup()
  })

  async function seedScored(pairs: Array<[number, string]>): Promise<string> {
    const key = `{zmod:${randomKey()}}`
    await redisClient.zAdd(
      key,
      pairs.map(([score, value]) => ({ score, value })),
    )
    return key
  }

  async function seedLex(members: string[]): Promise<string> {
    return seedScored(members.map(m => [0, m] as [number, string]))
  }

  // ---------- ZMSCORE ----------

  test('ZMSCORE returns scores for present members, nil for missing', async () => {
    const key = await seedScored([
      [1, 'a'],
      [2, 'b'],
      [3, 'c'],
    ])
    try {
      assert.deepStrictEqual(await redisClient.zmScore(key, ['a', 'x', 'c']), [
        1,
        null,
        3,
      ])
    } finally {
      await redisClient.del(key)
    }
  })

  test('ZMSCORE on a missing key returns all nil', async () => {
    const key = `{zmod:${randomKey()}}`
    assert.deepStrictEqual(await redisClient.zmScore(key, ['a', 'b']), [
      null,
      null,
    ])
  })

  test('ZMSCORE rejects wrong arity', async () => {
    const key = await seedScored([[1, 'a']])
    try {
      await assert.rejects(
        () => redisClient.sendCommand(key, true, ['ZMSCORE', key]),
        errorWithMessage("ERR wrong number of arguments for 'zmscore' command"),
      )
    } finally {
      await redisClient.del(key)
    }
  })

  test('ZMSCORE on a wrong-type key returns WRONGTYPE', async () => {
    const key = `{zmod:${randomKey()}}`
    await redisClient.set(key, 'notazset')
    try {
      await assert.rejects(
        () => redisClient.zmScore(key, ['a']),
        errorWithMessage(
          'WRONGTYPE Operation against a key holding the wrong kind of value',
        ),
      )
    } finally {
      await redisClient.del(key)
    }
  })

  // ---------- ZRANDMEMBER ----------

  test('ZRANDMEMBER without count returns one existing member', async () => {
    const key = await seedScored([
      [1, 'a'],
      [2, 'b'],
      [3, 'c'],
    ])
    try {
      const member = await redisClient.zRandMember(key)
      assert.ok(['a', 'b', 'c'].includes(member as string))
    } finally {
      await redisClient.del(key)
    }
  })

  test('ZRANDMEMBER with positive count returns distinct members', async () => {
    const key = await seedScored([
      [1, 'a'],
      [2, 'b'],
      [3, 'c'],
      [4, 'd'],
    ])
    try {
      const res = await redisClient.zRandMemberCount(key, 3)
      assert.strictEqual(res.length, 3)
      assert.strictEqual(new Set(res).size, 3) // distinct
      for (const m of res) assert.ok(['a', 'b', 'c', 'd'].includes(m))
    } finally {
      await redisClient.del(key)
    }
  })

  test('ZRANDMEMBER with count larger than cardinality returns all members', async () => {
    const key = await seedScored([
      [1, 'a'],
      [2, 'b'],
    ])
    try {
      const res = await redisClient.zRandMemberCount(key, 10)
      assert.strictEqual(res.length, 2)
      assert.deepStrictEqual([...res].sort(), ['a', 'b'])
    } finally {
      await redisClient.del(key)
    }
  })

  test('ZRANDMEMBER with negative count allows repeats and matches |count| length', async () => {
    const key = await seedScored([
      [1, 'a'],
      [2, 'b'],
    ])
    try {
      const res = await redisClient.zRandMemberCount(key, -8)
      assert.strictEqual(res.length, 8)
      for (const m of res) assert.ok(['a', 'b'].includes(m))
    } finally {
      await redisClient.del(key)
    }
  })

  test('ZRANDMEMBER WITHSCORES returns member/score pairs', async () => {
    const key = await seedScored([
      [1, 'a'],
      [2, 'b'],
      [3, 'c'],
    ])
    try {
      const res = await redisClient.zRandMemberCountWithScores(key, 2)
      assert.strictEqual(res.length, 2)
      const scores: Record<string, number> = { a: 1, b: 2, c: 3 }
      for (const { value, score } of res) {
        assert.strictEqual(score, scores[value])
      }
    } finally {
      await redisClient.del(key)
    }
  })

  test('ZRANDMEMBER with count 0 returns empty array', async () => {
    const key = await seedScored([[1, 'a']])
    try {
      assert.deepStrictEqual(await redisClient.zRandMemberCount(key, 0), [])
    } finally {
      await redisClient.del(key)
    }
  })

  test('ZRANDMEMBER on a missing key returns nil / empty array', async () => {
    const key = `{zmod:${randomKey()}}`
    assert.strictEqual(await redisClient.zRandMember(key), null)
    assert.deepStrictEqual(await redisClient.zRandMemberCount(key, 3), [])
  })

  test('ZRANDMEMBER with non-integer count errors', async () => {
    const key = await seedScored([[1, 'a']])
    try {
      await assert.rejects(
        () => redisClient.sendCommand(key, true, ['ZRANDMEMBER', key, 'x']),
        errorWithMessage('ERR value is not an integer or out of range'),
      )
      // WITHSCORES without a count is parsed as the count token -> not an integer
      await assert.rejects(
        () =>
          redisClient.sendCommand(key, true, [
            'ZRANDMEMBER',
            key,
            'WITHSCORES',
          ]),
        errorWithMessage('ERR value is not an integer or out of range'),
      )
    } finally {
      await redisClient.del(key)
    }
  })

  test('ZRANDMEMBER on a wrong-type key returns WRONGTYPE', async () => {
    const key = `{zmod:${randomKey()}}`
    await redisClient.set(key, 'notazset')
    try {
      await assert.rejects(
        () => redisClient.zRandMember(key),
        errorWithMessage(
          'WRONGTYPE Operation against a key holding the wrong kind of value',
        ),
      )
    } finally {
      await redisClient.del(key)
    }
  })

  // ---------- modern ZRANGE: index form (legacy still works) ----------

  test('ZRANGE legacy index form returns members in order', async () => {
    const key = await seedScored([
      [1, 'a'],
      [2, 'b'],
      [3, 'c'],
      [4, 'd'],
      [5, 'e'],
    ])
    try {
      assert.deepStrictEqual(await redisClient.zRange(key, 0, -1), [
        'a',
        'b',
        'c',
        'd',
        'e',
      ])
      assert.deepStrictEqual(await redisClient.zRange(key, 1, 3), [
        'b',
        'c',
        'd',
      ])
      assert.deepStrictEqual(await redisClient.zRangeWithScores(key, 0, -1), [
        { value: 'a', score: 1 },
        { value: 'b', score: 2 },
        { value: 'c', score: 3 },
        { value: 'd', score: 4 },
        { value: 'e', score: 5 },
      ])
    } finally {
      await redisClient.del(key)
    }
  })

  test('ZRANGE REV reverses the index ordering', async () => {
    const key = await seedScored([
      [1, 'a'],
      [2, 'b'],
      [3, 'c'],
    ])
    try {
      assert.deepStrictEqual(
        await redisClient.zRange(key, 0, -1, { REV: true }),
        ['c', 'b', 'a'],
      )
      assert.deepStrictEqual(
        await redisClient.zRangeWithScores(key, 0, -1, { REV: true }),
        [
          { value: 'c', score: 3 },
          { value: 'b', score: 2 },
          { value: 'a', score: 1 },
        ],
      )
      assert.deepStrictEqual(
        await redisClient.zRange(key, 1, 2, { REV: true }),
        ['b', 'a'],
      )
    } finally {
      await redisClient.del(key)
    }
  })

  // ---------- modern ZRANGE: BYSCORE ----------

  test('ZRANGE BYSCORE filters by score bounds', async () => {
    const key = await seedScored([
      [1, 'a'],
      [2, 'b'],
      [3, 'c'],
      [4, 'd'],
      [5, 'e'],
    ])
    try {
      assert.deepStrictEqual(
        await redisClient.zRange(key, '(1', '4', { BY: 'SCORE' }),
        ['b', 'c', 'd'],
      )
      assert.deepStrictEqual(
        await redisClient.zRangeWithScores(key, '-inf', '+inf', {
          BY: 'SCORE',
        }),
        [
          { value: 'a', score: 1 },
          { value: 'b', score: 2 },
          { value: 'c', score: 3 },
          { value: 'd', score: 4 },
          { value: 'e', score: 5 },
        ],
      )
      assert.deepStrictEqual(
        await redisClient.zRange(key, '-inf', '+inf', {
          BY: 'SCORE',
          LIMIT: { offset: 1, count: 2 },
        }),
        ['b', 'c'],
      )
    } finally {
      await redisClient.del(key)
    }
  })

  test('ZRANGE BYSCORE REV takes bounds as max min and reverses', async () => {
    const key = await seedScored([
      [1, 'a'],
      [2, 'b'],
      [3, 'c'],
      [4, 'd'],
      [5, 'e'],
    ])
    try {
      assert.deepStrictEqual(
        await redisClient.zRange(key, 5, 2, { BY: 'SCORE', REV: true }),
        ['e', 'd', 'c', 'b'],
      )
      assert.deepStrictEqual(
        await redisClient.zRangeWithScores(key, 5, 2, {
          BY: 'SCORE',
          REV: true,
        }),
        [
          { value: 'e', score: 5 },
          { value: 'd', score: 4 },
          { value: 'c', score: 3 },
          { value: 'b', score: 2 },
        ],
      )
      // min given before max with REV -> empty
      assert.deepStrictEqual(
        await redisClient.zRange(key, 2, 5, { BY: 'SCORE', REV: true }),
        [],
      )
    } finally {
      await redisClient.del(key)
    }
  })

  // ---------- modern ZRANGE: BYLEX ----------

  test('ZRANGE BYLEX filters by lex bounds', async () => {
    const key = await seedLex(['a', 'b', 'c', 'd', 'e'])
    try {
      assert.deepStrictEqual(
        await redisClient.zRange(key, '[a', '[c', { BY: 'LEX' }),
        ['a', 'b', 'c'],
      )
      assert.deepStrictEqual(
        await redisClient.zRange(key, '-', '+', {
          BY: 'LEX',
          LIMIT: { offset: 1, count: 2 },
        }),
        ['b', 'c'],
      )
    } finally {
      await redisClient.del(key)
    }
  })

  test('ZRANGE BYLEX REV takes bounds as max min and reverses', async () => {
    const key = await seedLex(['a', 'b', 'c', 'd', 'e'])
    try {
      assert.deepStrictEqual(
        await redisClient.zRange(key, '[c', '[a', { BY: 'LEX', REV: true }),
        ['c', 'b', 'a'],
      )
      assert.deepStrictEqual(
        await redisClient.zRange(key, '+', '-', {
          BY: 'LEX',
          REV: true,
          LIMIT: { offset: 1, count: 2 },
        }),
        ['d', 'c'],
      )
    } finally {
      await redisClient.del(key)
    }
  })

  test('ZRANGE on a missing key returns empty array', async () => {
    const key = `{zmod:${randomKey()}}`
    assert.deepStrictEqual(await redisClient.zRange(key, 0, -1), [])
    assert.deepStrictEqual(
      await redisClient.zRange(key, '-inf', '+inf', { BY: 'SCORE' }),
      [],
    )
    assert.deepStrictEqual(
      await redisClient.zRange(key, '-', '+', { BY: 'LEX' }),
      [],
    )
  })

  // ---------- ZRANGESTORE ----------

  test('ZRANGESTORE stores an index range and overwrites destination', async () => {
    const tag = `{zrangestore:${randomKey()}}`
    const source = `${tag}:source`
    const destination = `${tag}:destination`

    try {
      await redisClient.zAdd(source, [
        { score: 1, value: 'a' },
        { score: 2, value: 'b' },
        { score: 3, value: 'c' },
        { score: 4, value: 'd' },
      ])
      await redisClient.set(destination, 'old-value')

      assert.strictEqual(
        await redisClient.zRangeStore(destination, source, 1, 2),
        2,
      )
      assert.deepStrictEqual(
        await redisClient.zRangeWithScores(destination, 0, -1),
        [
          { value: 'b', score: 2 },
          { value: 'c', score: 3 },
        ],
      )
      assert.deepStrictEqual(await redisClient.zRange(source, 0, -1), [
        'a',
        'b',
        'c',
        'd',
      ])
    } finally {
      await redisClient.del(source)
      await redisClient.del(destination)
    }
  })

  test('ZRANGESTORE supports BYSCORE and BYLEX ranges', async () => {
    const tag = `{zrangestore-ranges:${randomKey()}}`
    const scoreSource = `${tag}:score-source`
    const scoreDestination = `${tag}:score-destination`
    const lexSource = `${tag}:lex-source`
    const lexDestination = `${tag}:lex-destination`

    try {
      await redisClient.zAdd(scoreSource, [
        { score: 1, value: 'a' },
        { score: 2, value: 'b' },
        { score: 3, value: 'c' },
        { score: 4, value: 'd' },
        { score: 5, value: 'e' },
      ])
      assert.strictEqual(
        await redisClient.zRangeStore(scoreDestination, scoreSource, 5, 2, {
          BY: 'SCORE',
          REV: true,
          LIMIT: { offset: 1, count: 2 },
        }),
        2,
      )
      assert.deepStrictEqual(
        await redisClient.zRangeWithScores(scoreDestination, 0, -1),
        [
          { value: 'c', score: 3 },
          { value: 'd', score: 4 },
        ],
      )

      await redisClient.zAdd(lexSource, [
        { score: 0, value: 'a' },
        { score: 0, value: 'b' },
        { score: 0, value: 'c' },
        { score: 0, value: 'd' },
      ])
      assert.strictEqual(
        await redisClient.zRangeStore(lexDestination, lexSource, '[d', '[b', {
          BY: 'LEX',
          REV: true,
          LIMIT: { offset: 0, count: 2 },
        }),
        2,
      )
      assert.deepStrictEqual(await redisClient.zRange(lexDestination, 0, -1), [
        'c',
        'd',
      ])
    } finally {
      await redisClient.del([
        scoreSource,
        scoreDestination,
        lexSource,
        lexDestination,
      ])
    }
  })

  test('ZRANGESTORE deletes destination when source is missing or range is empty', async () => {
    const tag = `{zrangestore-empty:${randomKey()}}`
    const source = `${tag}:source`
    const missingSource = `${tag}:missing`
    const destination = `${tag}:destination`

    try {
      await redisClient.zAdd(source, { score: 1, value: 'a' })
      await redisClient.zAdd(destination, { score: 9, value: 'old' })
      assert.strictEqual(
        await redisClient.zRangeStore(destination, source, 2, 3, {
          BY: 'SCORE',
        }),
        0,
      )
      assert.strictEqual(await redisClient.exists(destination), 0)

      await redisClient.zAdd(destination, { score: 9, value: 'old' })
      assert.strictEqual(
        await redisClient.zRangeStore(destination, missingSource, 0, -1),
        0,
      )
      assert.strictEqual(await redisClient.exists(destination), 0)
    } finally {
      await redisClient.del([source, missingSource, destination])
    }
  })

  test('ZRANGESTORE rejects invalid syntax and wrong-type sources', async () => {
    const tag = `{zrangestore-errors:${randomKey()}}`
    const source = `${tag}:source`
    const destination = `${tag}:destination`
    const stringSource = `${tag}:string-source`
    const send = (args: string[]) =>
      redisClient.sendCommand(destination, false, args)

    try {
      await redisClient.zAdd(source, [
        { score: 1, value: 'a' },
        { score: 2, value: 'b' },
      ])
      await redisClient.set(stringSource, 'not-a-zset')

      await assert.rejects(
        () =>
          send([
            'ZRANGESTORE',
            destination,
            source,
            '0',
            '-1',
            'LIMIT',
            '0',
            '1',
          ]),
        errorWithMessage(
          'ERR syntax error, LIMIT is only supported in combination with either BYSCORE or BYLEX',
        ),
      )
      await assert.rejects(
        () =>
          send(['ZRANGESTORE', destination, source, '0', '-1', 'WITHSCORES']),
        errorWithMessage('ERR syntax error'),
      )
      await assert.rejects(
        () =>
          send([
            'ZRANGESTORE',
            destination,
            source,
            '-',
            '+',
            'BYSCORE',
            'BYLEX',
          ]),
        errorWithMessage('ERR syntax error'),
      )
      await assert.rejects(
        () => send(['ZRANGESTORE', destination, source, '(1', '4']),
        errorWithMessage('ERR value is not an integer or out of range'),
      )
      await assert.rejects(
        () => send(['ZRANGESTORE', destination, source, 'x', '5', 'BYSCORE']),
        errorWithMessage('ERR min or max is not a float'),
      )
      await assert.rejects(
        () => send(['ZRANGESTORE', destination, source, 'a', 'c', 'BYLEX']),
        errorWithMessage('ERR min or max not valid string range item'),
      )
      await assert.rejects(
        () =>
          send([
            'ZRANGESTORE',
            destination,
            source,
            '-inf',
            '+inf',
            'BYSCORE',
            'LIMIT',
            'a',
            'b',
          ]),
        errorWithMessage('ERR value is not an integer or out of range'),
      )
      await assert.rejects(
        () => send(['ZRANGESTORE', destination, source, '0']),
        errorWithMessage(
          "ERR wrong number of arguments for 'zrangestore' command",
        ),
      )
      await assert.rejects(
        () => send(['ZRANGESTORE', destination, stringSource, '0', '-1']),
        errorWithMessage(
          'WRONGTYPE Operation against a key holding the wrong kind of value',
        ),
      )
    } finally {
      await redisClient.del([source, destination, stringSource])
    }
  })

  test('ZRANGESTORE rejects destination and source keys from different slots', async () => {
    const source = `{zrangestore-cross:${randomKey()}}:source`
    const destination = `zrangestore-cross-destination:${randomKey()}`

    try {
      await redisClient.zAdd(source, { score: 1, value: 'a' })
      const directClient = await connectToNodeRedisSlotOwner(
        redisClient,
        source,
      )
      try {
        await assert.rejects(
          () =>
            directClient.sendCommand([
              'ZRANGESTORE',
              destination,
              source,
              '0',
              '-1',
            ]),
          errorWithMessage(
            "CROSSSLOT Keys in request don't hash to the same slot",
          ),
        )
      } finally {
        directClient.destroy()
      }
    } finally {
      await redisClient.del(source)
      await redisClient.del(destination)
    }
  })

  // ---------- modern ZRANGE: error paths ----------

  test('ZRANGE rejects invalid option combinations', async () => {
    const key = await seedScored([
      [1, 'a'],
      [2, 'b'],
      [3, 'c'],
    ])
    const send = (args: string[]) => redisClient.sendCommand(key, true, args)
    try {
      await assert.rejects(
        () => send(['ZRANGE', key, '0', '-1', 'LIMIT', '0', '2']),
        errorWithMessage(
          'ERR syntax error, LIMIT is only supported in combination with either BYSCORE or BYLEX',
        ),
      )
      await assert.rejects(
        () => send(['ZRANGE', key, '-', '+', 'BYLEX', 'WITHSCORES']),
        errorWithMessage(
          'ERR syntax error, WITHSCORES not supported in combination with BYLEX',
        ),
      )
      await assert.rejects(
        () => send(['ZRANGE', key, '-', '+', 'BYSCORE', 'BYLEX']),
        errorWithMessage('ERR syntax error'),
      )
      // index form requires integer start/stop
      await assert.rejects(
        () => send(['ZRANGE', key, '(1', '4']),
        errorWithMessage('ERR value is not an integer or out of range'),
      )
      await assert.rejects(
        () => send(['ZRANGE', key, 'x', '5', 'BYSCORE']),
        errorWithMessage('ERR min or max is not a float'),
      )
      await assert.rejects(
        () => send(['ZRANGE', key, 'a', 'c', 'BYLEX']),
        errorWithMessage('ERR min or max not valid string range item'),
      )
      await assert.rejects(
        () =>
          send(['ZRANGE', key, '-inf', '+inf', 'BYSCORE', 'LIMIT', 'a', 'b']),
        errorWithMessage('ERR value is not an integer or out of range'),
      )
      await assert.rejects(
        () => send(['ZRANGE', key, '0']),
        errorWithMessage("ERR wrong number of arguments for 'zrange' command"),
      )
    } finally {
      await redisClient.del(key)
    }
  })
})
