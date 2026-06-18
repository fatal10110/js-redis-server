import { test, describe, before, after } from 'node:test'
import assert from 'node:assert'
import { Cluster } from 'ioredis'
import { TestRunner } from '../test-config'
import { connectToSlotOwner, errorWithMessage, randomKey } from '../utils'

const testRunner = new TestRunner()

describe(`Sorted Set Modern Range / ZMSCORE / ZRANDMEMBER (${testRunner.getBackendName()})`, () => {
  let redisClient: Cluster | undefined

  before(async () => {
    redisClient = await testRunner.setupIoredisCluster(
      'zset-modern-integration',
    )
  })

  after(async () => {
    await testRunner.cleanup()
  })

  async function seedScored(pairs: Array<[number, string]>): Promise<string> {
    const key = `{zmod:${randomKey()}}`
    const args: (string | number)[] = []
    for (const [score, member] of pairs) {
      args.push(score, member)
    }
    await redisClient?.zadd(key, ...(args as [number, string]))
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
      assert.deepStrictEqual(await redisClient?.zmscore(key, 'a', 'x', 'c'), [
        '1',
        null,
        '3',
      ])
    } finally {
      await redisClient?.del(key)
    }
  })

  test('ZMSCORE on a missing key returns all nil', async () => {
    const key = `{zmod:${randomKey()}}`
    assert.deepStrictEqual(await redisClient?.zmscore(key, 'a', 'b'), [
      null,
      null,
    ])
  })

  test('ZMSCORE rejects wrong arity', async () => {
    const key = await seedScored([[1, 'a']])
    try {
      await assert.rejects(
        () => redisClient?.call('zmscore', key),
        errorWithMessage("ERR wrong number of arguments for 'zmscore' command"),
      )
    } finally {
      await redisClient?.del(key)
    }
  })

  test('ZMSCORE on a wrong-type key returns WRONGTYPE', async () => {
    const key = `{zmod:${randomKey()}}`
    await redisClient?.set(key, 'notazset')
    try {
      await assert.rejects(
        () => redisClient?.zmscore(key, 'a'),
        errorWithMessage(
          'WRONGTYPE Operation against a key holding the wrong kind of value',
        ),
      )
    } finally {
      await redisClient?.del(key)
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
      const member = await redisClient?.zrandmember(key)
      assert.ok(['a', 'b', 'c'].includes(member as string))
    } finally {
      await redisClient?.del(key)
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
      const res = (await redisClient?.zrandmember(key, 3)) as string[]
      assert.strictEqual(res.length, 3)
      assert.strictEqual(new Set(res).size, 3) // distinct
      for (const m of res) assert.ok(['a', 'b', 'c', 'd'].includes(m))
    } finally {
      await redisClient?.del(key)
    }
  })

  test('ZRANDMEMBER with count larger than cardinality returns all members', async () => {
    const key = await seedScored([
      [1, 'a'],
      [2, 'b'],
    ])
    try {
      const res = (await redisClient?.zrandmember(key, 10)) as string[]
      assert.strictEqual(res.length, 2)
      assert.deepStrictEqual([...res].sort(), ['a', 'b'])
    } finally {
      await redisClient?.del(key)
    }
  })

  test('ZRANDMEMBER with negative count allows repeats and matches |count| length', async () => {
    const key = await seedScored([
      [1, 'a'],
      [2, 'b'],
    ])
    try {
      const res = (await redisClient?.zrandmember(key, -8)) as string[]
      assert.strictEqual(res.length, 8)
      for (const m of res) assert.ok(['a', 'b'].includes(m))
    } finally {
      await redisClient?.del(key)
    }
  })

  test('ZRANDMEMBER WITHSCORES returns member/score pairs', async () => {
    const key = await seedScored([
      [1, 'a'],
      [2, 'b'],
      [3, 'c'],
    ])
    try {
      const res = (await redisClient?.zrandmember(
        key,
        2,
        'WITHSCORES',
      )) as string[]
      assert.strictEqual(res.length, 4)
      const scores: Record<string, string> = { a: '1', b: '2', c: '3' }
      for (let i = 0; i < res.length; i += 2) {
        assert.strictEqual(res[i + 1], scores[res[i]])
      }
    } finally {
      await redisClient?.del(key)
    }
  })

  test('ZRANDMEMBER with count 0 returns empty array', async () => {
    const key = await seedScored([[1, 'a']])
    try {
      assert.deepStrictEqual(await redisClient?.zrandmember(key, 0), [])
    } finally {
      await redisClient?.del(key)
    }
  })

  test('ZRANDMEMBER on a missing key returns nil / empty array', async () => {
    const key = `{zmod:${randomKey()}}`
    assert.strictEqual(await redisClient?.zrandmember(key), null)
    assert.deepStrictEqual(await redisClient?.zrandmember(key, 3), [])
  })

  test('ZRANDMEMBER with non-integer count errors', async () => {
    const key = await seedScored([[1, 'a']])
    try {
      await assert.rejects(
        () => redisClient?.call('zrandmember', key, 'x'),
        errorWithMessage('ERR value is not an integer or out of range'),
      )
      // WITHSCORES without a count is parsed as the count token -> not an integer
      await assert.rejects(
        () => redisClient?.call('zrandmember', key, 'WITHSCORES'),
        errorWithMessage('ERR value is not an integer or out of range'),
      )
    } finally {
      await redisClient?.del(key)
    }
  })

  test('ZRANDMEMBER on a wrong-type key returns WRONGTYPE', async () => {
    const key = `{zmod:${randomKey()}}`
    await redisClient?.set(key, 'notazset')
    try {
      await assert.rejects(
        () => redisClient?.zrandmember(key),
        errorWithMessage(
          'WRONGTYPE Operation against a key holding the wrong kind of value',
        ),
      )
    } finally {
      await redisClient?.del(key)
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
      assert.deepStrictEqual(await redisClient?.zrange(key, 0, -1), [
        'a',
        'b',
        'c',
        'd',
        'e',
      ])
      assert.deepStrictEqual(await redisClient?.zrange(key, 1, 3), [
        'b',
        'c',
        'd',
      ])
      assert.deepStrictEqual(
        await redisClient?.zrange(key, 0, -1, 'WITHSCORES'),
        ['a', '1', 'b', '2', 'c', '3', 'd', '4', 'e', '5'],
      )
    } finally {
      await redisClient?.del(key)
    }
  })

  test('ZRANGE REV reverses the index ordering', async () => {
    const key = await seedScored([
      [1, 'a'],
      [2, 'b'],
      [3, 'c'],
    ])
    try {
      assert.deepStrictEqual(await redisClient?.zrange(key, 0, -1, 'REV'), [
        'c',
        'b',
        'a',
      ])
      assert.deepStrictEqual(
        await redisClient?.zrange(key, 0, -1, 'REV', 'WITHSCORES'),
        ['c', '3', 'b', '2', 'a', '1'],
      )
      assert.deepStrictEqual(await redisClient?.zrange(key, 1, 2, 'REV'), [
        'b',
        'a',
      ])
    } finally {
      await redisClient?.del(key)
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
        await redisClient?.zrange(key, '(1', '4', 'BYSCORE'),
        ['b', 'c', 'd'],
      )
      assert.deepStrictEqual(
        await redisClient?.zrange(key, '-inf', '+inf', 'BYSCORE', 'WITHSCORES'),
        ['a', '1', 'b', '2', 'c', '3', 'd', '4', 'e', '5'],
      )
      assert.deepStrictEqual(
        await redisClient?.zrange(
          key,
          '-inf',
          '+inf',
          'BYSCORE',
          'LIMIT',
          1,
          2,
        ),
        ['b', 'c'],
      )
    } finally {
      await redisClient?.del(key)
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
        await redisClient?.zrange(key, 5, 2, 'BYSCORE', 'REV'),
        ['e', 'd', 'c', 'b'],
      )
      assert.deepStrictEqual(
        await redisClient?.zrange(key, 5, 2, 'BYSCORE', 'REV', 'WITHSCORES'),
        ['e', '5', 'd', '4', 'c', '3', 'b', '2'],
      )
      // min given before max with REV -> empty
      assert.deepStrictEqual(
        await redisClient?.zrange(key, 2, 5, 'BYSCORE', 'REV'),
        [],
      )
    } finally {
      await redisClient?.del(key)
    }
  })

  // ---------- modern ZRANGE: BYLEX ----------

  test('ZRANGE BYLEX filters by lex bounds', async () => {
    const key = await seedLex(['a', 'b', 'c', 'd', 'e'])
    try {
      assert.deepStrictEqual(
        await redisClient?.zrange(key, '[a', '[c', 'BYLEX'),
        ['a', 'b', 'c'],
      )
      assert.deepStrictEqual(
        await redisClient?.zrange(key, '-', '+', 'BYLEX', 'LIMIT', 1, 2),
        ['b', 'c'],
      )
    } finally {
      await redisClient?.del(key)
    }
  })

  test('ZRANGE BYLEX REV takes bounds as max min and reverses', async () => {
    const key = await seedLex(['a', 'b', 'c', 'd', 'e'])
    try {
      assert.deepStrictEqual(
        await redisClient?.zrange(key, '[c', '[a', 'BYLEX', 'REV'),
        ['c', 'b', 'a'],
      )
      assert.deepStrictEqual(
        await redisClient?.zrange(key, '+', '-', 'BYLEX', 'REV', 'LIMIT', 1, 2),
        ['d', 'c'],
      )
    } finally {
      await redisClient?.del(key)
    }
  })

  test('ZRANGE on a missing key returns empty array', async () => {
    const key = `{zmod:${randomKey()}}`
    assert.deepStrictEqual(await redisClient?.zrange(key, 0, -1), [])
    assert.deepStrictEqual(
      await redisClient?.zrange(key, '-inf', '+inf', 'BYSCORE'),
      [],
    )
    assert.deepStrictEqual(
      await redisClient?.zrange(key, '-', '+', 'BYLEX'),
      [],
    )
  })

  // ---------- ZRANGESTORE ----------

  test('ZRANGESTORE stores an index range and overwrites destination', async () => {
    const tag = `{zrangestore:${randomKey()}}`
    const source = `${tag}:source`
    const destination = `${tag}:destination`

    try {
      await redisClient?.zadd(source, 1, 'a', 2, 'b', 3, 'c', 4, 'd')
      await redisClient?.set(destination, 'old-value')

      assert.strictEqual(
        await redisClient?.call('zrangestore', destination, source, '1', '2'),
        2,
      )
      assert.deepStrictEqual(
        await redisClient?.zrange(destination, 0, -1, 'WITHSCORES'),
        ['b', '2', 'c', '3'],
      )
      assert.deepStrictEqual(await redisClient?.zrange(source, 0, -1), [
        'a',
        'b',
        'c',
        'd',
      ])
    } finally {
      await redisClient?.del(source)
      await redisClient?.del(destination)
    }
  })

  test('ZRANGESTORE supports BYSCORE and BYLEX ranges', async () => {
    const tag = `{zrangestore-ranges:${randomKey()}}`
    const scoreSource = `${tag}:score-source`
    const scoreDestination = `${tag}:score-destination`
    const lexSource = `${tag}:lex-source`
    const lexDestination = `${tag}:lex-destination`

    try {
      await redisClient?.zadd(
        scoreSource,
        1,
        'a',
        2,
        'b',
        3,
        'c',
        4,
        'd',
        5,
        'e',
      )
      assert.strictEqual(
        await redisClient?.call(
          'zrangestore',
          scoreDestination,
          scoreSource,
          '5',
          '2',
          'BYSCORE',
          'REV',
          'LIMIT',
          '1',
          '2',
        ),
        2,
      )
      assert.deepStrictEqual(
        await redisClient?.zrange(scoreDestination, 0, -1, 'WITHSCORES'),
        ['c', '3', 'd', '4'],
      )

      await redisClient?.zadd(lexSource, 0, 'a', 0, 'b', 0, 'c', 0, 'd')
      assert.strictEqual(
        await redisClient?.call(
          'zrangestore',
          lexDestination,
          lexSource,
          '[d',
          '[b',
          'BYLEX',
          'REV',
          'LIMIT',
          '0',
          '2',
        ),
        2,
      )
      assert.deepStrictEqual(await redisClient?.zrange(lexDestination, 0, -1), [
        'c',
        'd',
      ])
    } finally {
      await redisClient?.del(
        scoreSource,
        scoreDestination,
        lexSource,
        lexDestination,
      )
    }
  })

  test('ZRANGESTORE deletes destination when source is missing or range is empty', async () => {
    const tag = `{zrangestore-empty:${randomKey()}}`
    const source = `${tag}:source`
    const missingSource = `${tag}:missing`
    const destination = `${tag}:destination`

    try {
      await redisClient?.zadd(source, 1, 'a')
      await redisClient?.zadd(destination, 9, 'old')
      assert.strictEqual(
        await redisClient?.call(
          'zrangestore',
          destination,
          source,
          '2',
          '3',
          'BYSCORE',
        ),
        0,
      )
      assert.strictEqual(await redisClient?.exists(destination), 0)

      await redisClient?.zadd(destination, 9, 'old')
      assert.strictEqual(
        await redisClient?.call(
          'zrangestore',
          destination,
          missingSource,
          '0',
          '-1',
        ),
        0,
      )
      assert.strictEqual(await redisClient?.exists(destination), 0)
    } finally {
      await redisClient?.del(source, missingSource, destination)
    }
  })

  test('ZRANGESTORE rejects invalid syntax and wrong-type sources', async () => {
    const tag = `{zrangestore-errors:${randomKey()}}`
    const source = `${tag}:source`
    const destination = `${tag}:destination`
    const stringSource = `${tag}:string-source`

    try {
      await redisClient?.zadd(source, 1, 'a', 2, 'b')
      await redisClient?.set(stringSource, 'not-a-zset')

      await assert.rejects(
        () =>
          redisClient?.call(
            'zrangestore',
            destination,
            source,
            '0',
            '-1',
            'LIMIT',
            '0',
            '1',
          ),
        errorWithMessage(
          'ERR syntax error, LIMIT is only supported in combination with either BYSCORE or BYLEX',
        ),
      )
      await assert.rejects(
        () =>
          redisClient?.call(
            'zrangestore',
            destination,
            source,
            '0',
            '-1',
            'WITHSCORES',
          ),
        errorWithMessage('ERR syntax error'),
      )
      await assert.rejects(
        () =>
          redisClient?.call(
            'zrangestore',
            destination,
            source,
            '-',
            '+',
            'BYSCORE',
            'BYLEX',
          ),
        errorWithMessage('ERR syntax error'),
      )
      await assert.rejects(
        () => redisClient?.call('zrangestore', destination, source, '(1', '4'),
        errorWithMessage('ERR value is not an integer or out of range'),
      )
      await assert.rejects(
        () =>
          redisClient?.call(
            'zrangestore',
            destination,
            source,
            'x',
            '5',
            'BYSCORE',
          ),
        errorWithMessage('ERR min or max is not a float'),
      )
      await assert.rejects(
        () =>
          redisClient?.call(
            'zrangestore',
            destination,
            source,
            'a',
            'c',
            'BYLEX',
          ),
        errorWithMessage('ERR min or max not valid string range item'),
      )
      await assert.rejects(
        () =>
          redisClient?.call(
            'zrangestore',
            destination,
            source,
            '-inf',
            '+inf',
            'BYSCORE',
            'LIMIT',
            'a',
            'b',
          ),
        errorWithMessage('ERR value is not an integer or out of range'),
      )
      await assert.rejects(
        () => redisClient?.call('zrangestore', destination, source, '0'),
        errorWithMessage(
          "ERR wrong number of arguments for 'zrangestore' command",
        ),
      )
      await assert.rejects(
        () =>
          redisClient?.call(
            'zrangestore',
            destination,
            stringSource,
            '0',
            '-1',
          ),
        errorWithMessage(
          'WRONGTYPE Operation against a key holding the wrong kind of value',
        ),
      )
    } finally {
      await redisClient?.del(source, destination, stringSource)
    }
  })

  test('ZRANGESTORE rejects destination and source keys from different slots', async () => {
    const source = `{zrangestore-cross:${randomKey()}}:source`
    const destination = `zrangestore-cross-destination:${randomKey()}`

    try {
      await redisClient?.zadd(source, 1, 'a')
      const directClient = await connectToSlotOwner(redisClient!, source)
      try {
        await assert.rejects(
          () =>
            directClient.call('zrangestore', destination, source, '0', '-1'),
          errorWithMessage(
            "CROSSSLOT Keys in request don't hash to the same slot",
          ),
        )
      } finally {
        directClient.disconnect()
      }
    } finally {
      await redisClient?.del(source)
      await redisClient?.del(destination)
    }
  })

  // ---------- modern ZRANGE: error paths ----------

  test('ZRANGE rejects invalid option combinations', async () => {
    const key = await seedScored([
      [1, 'a'],
      [2, 'b'],
      [3, 'c'],
    ])
    try {
      await assert.rejects(
        () => redisClient?.call('zrange', key, '0', '-1', 'LIMIT', '0', '2'),
        errorWithMessage(
          'ERR syntax error, LIMIT is only supported in combination with either BYSCORE or BYLEX',
        ),
      )
      await assert.rejects(
        () => redisClient?.call('zrange', key, '-', '+', 'BYLEX', 'WITHSCORES'),
        errorWithMessage(
          'ERR syntax error, WITHSCORES not supported in combination with BYLEX',
        ),
      )
      await assert.rejects(
        () => redisClient?.call('zrange', key, '-', '+', 'BYSCORE', 'BYLEX'),
        errorWithMessage('ERR syntax error'),
      )
      // index form requires integer start/stop
      await assert.rejects(
        () => redisClient?.call('zrange', key, '(1', '4'),
        errorWithMessage('ERR value is not an integer or out of range'),
      )
      await assert.rejects(
        () => redisClient?.call('zrange', key, 'x', '5', 'BYSCORE'),
        errorWithMessage('ERR min or max is not a float'),
      )
      await assert.rejects(
        () => redisClient?.call('zrange', key, 'a', 'c', 'BYLEX'),
        errorWithMessage('ERR min or max not valid string range item'),
      )
      await assert.rejects(
        () =>
          redisClient?.call(
            'zrange',
            key,
            '-inf',
            '+inf',
            'BYSCORE',
            'LIMIT',
            'a',
            'b',
          ),
        errorWithMessage('ERR value is not an integer or out of range'),
      )
      await assert.rejects(
        () => redisClient?.call('zrange', key, '0'),
        errorWithMessage("ERR wrong number of arguments for 'zrange' command"),
      )
    } finally {
      await redisClient?.del(key)
    }
  })
})
