import { test, describe, before, after } from 'node:test'
import assert from 'node:assert'
import { RedisClusterType } from 'redis'
import { TestRunner } from '../test-config'
import { errorWithMessage, flushNodeRedisCluster, randomKey } from '../utils'

const testRunner = new TestRunner()

describe(`Sorted Set Lex Range Commands Integration (node-redis, ${testRunner.getBackendName()})`, () => {
  let redisClient: RedisClusterType

  before(async () => {
    redisClient = (await testRunner.setupNodeRedisCluster()) as RedisClusterType
    await flushNodeRedisCluster(redisClient)
  })

  after(async () => {
    await testRunner.cleanup()
  })

  async function seed(members: string[]): Promise<string> {
    const key = `{lex:${randomKey()}}`
    await redisClient.zAdd(
      key,
      members.map(value => ({ score: 0, value })),
    )
    return key
  }

  test('ZRANGEBYLEX returns members within lex bounds', async () => {
    const key = await seed(['a', 'b', 'c', 'd', 'e', 'f', 'g'])
    try {
      assert.deepStrictEqual(await redisClient.zRangeByLex(key, '-', '+'), [
        'a',
        'b',
        'c',
        'd',
        'e',
        'f',
        'g',
      ])
      // `[aaa` is inclusive of "aaa" which sorts after "a", so "a" is excluded
      assert.deepStrictEqual(await redisClient.zRangeByLex(key, '[aaa', '(g'), [
        'b',
        'c',
        'd',
        'e',
        'f',
      ])
      assert.deepStrictEqual(await redisClient.zRangeByLex(key, '(b', '(f'), [
        'c',
        'd',
        'e',
      ])
      assert.deepStrictEqual(await redisClient.zRangeByLex(key, '-', '[c'), [
        'a',
        'b',
        'c',
      ])
      // min > max returns empty
      assert.deepStrictEqual(await redisClient.zRangeByLex(key, '+', '-'), [])
    } finally {
      await redisClient.del(key)
    }
  })

  test('ZRANGEBYLEX honors LIMIT offset count', async () => {
    const key = await seed(['a', 'b', 'c', 'd', 'e', 'f', 'g'])
    try {
      assert.deepStrictEqual(
        await redisClient.zRangeByLex(key, '-', '+', {
          LIMIT: { offset: 2, count: 3 },
        }),
        ['c', 'd', 'e'],
      )
      // negative count returns all remaining from offset
      assert.deepStrictEqual(
        await redisClient.zRangeByLex(key, '-', '+', {
          LIMIT: { offset: 2, count: -1 },
        }),
        ['c', 'd', 'e', 'f', 'g'],
      )
      // offset beyond length returns empty
      assert.deepStrictEqual(
        await redisClient.zRangeByLex(key, '-', '+', {
          LIMIT: { offset: 10, count: 3 },
        }),
        [],
      )
      // count 0 returns empty
      assert.deepStrictEqual(
        await redisClient.zRangeByLex(key, '-', '+', {
          LIMIT: { offset: 0, count: 0 },
        }),
        [],
      )
    } finally {
      await redisClient.del(key)
    }
  })

  test('ZREVRANGEBYLEX returns members in reverse lex order (max then min)', async () => {
    const key = await seed(['a', 'b', 'c', 'd', 'e', 'f', 'g'])
    try {
      assert.deepStrictEqual(
        await redisClient.zRange(key, '+', '-', { BY: 'LEX', REV: true }),
        ['g', 'f', 'e', 'd', 'c', 'b', 'a'],
      )
      assert.deepStrictEqual(
        await redisClient.zRange(key, '[c', '-', { BY: 'LEX', REV: true }),
        ['c', 'b', 'a'],
      )
      assert.deepStrictEqual(
        await redisClient.zRange(key, '+', '-', {
          BY: 'LEX',
          REV: true,
          LIMIT: { offset: 1, count: 2 },
        }),
        ['f', 'e'],
      )
    } finally {
      await redisClient.del(key)
    }
  })

  test('ZLEXCOUNT counts members within lex bounds', async () => {
    const key = await seed(['a', 'b', 'c', 'd', 'e', 'f', 'g'])
    try {
      assert.strictEqual(await redisClient.zLexCount(key, '-', '+'), 7)
      assert.strictEqual(await redisClient.zLexCount(key, '(b', '[d'), 2)
      assert.strictEqual(await redisClient.zLexCount(key, '[z', '+'), 0)
    } finally {
      await redisClient.del(key)
    }
  })

  test('ZREMRANGEBYLEX removes members within lex bounds and returns count', async () => {
    const key = await seed(['a', 'b', 'c', 'd', 'e', 'f', 'g'])
    try {
      assert.strictEqual(await redisClient.zRemRangeByLex(key, '(b', '[d'), 2)
      assert.deepStrictEqual(await redisClient.zRangeByLex(key, '-', '+'), [
        'a',
        'b',
        'e',
        'f',
        'g',
      ])
      // removing all remaining members deletes the key
      assert.strictEqual(await redisClient.zRemRangeByLex(key, '-', '+'), 5)
      assert.strictEqual(await redisClient.exists(key), 0)
    } finally {
      await redisClient.del(key)
    }
  })

  test('lex ordering uses raw byte comparison, not locale', async () => {
    const key = await seed(['B', 'a', 'A', 'b'])
    try {
      // raw byte order: 'A'(65) 'B'(66) 'a'(97) 'b'(98)
      assert.deepStrictEqual(await redisClient.zRangeByLex(key, '-', '+'), [
        'A',
        'B',
        'a',
        'b',
      ])
    } finally {
      await redisClient.del(key)
    }
  })

  test('lex commands on a missing key return empty/zero', async () => {
    const key = `{lex:${randomKey()}}`
    assert.deepStrictEqual(await redisClient.zRangeByLex(key, '-', '+'), [])
    assert.deepStrictEqual(
      await redisClient.zRange(key, '+', '-', { BY: 'LEX', REV: true }),
      [],
    )
    assert.strictEqual(await redisClient.zLexCount(key, '-', '+'), 0)
    assert.strictEqual(await redisClient.zRemRangeByLex(key, '-', '+'), 0)
  })

  test('invalid lex bound returns "not valid string range item" error', async () => {
    const key = await seed(['a', 'b', 'c'])
    const msg = 'ERR min or max not valid string range item'
    try {
      await assert.rejects(
        () => redisClient.zRangeByLex(key, 'g', '+'),
        errorWithMessage(msg),
      )
      await assert.rejects(
        () => redisClient.zRangeByLex(key, '-', 'g'),
        errorWithMessage(msg),
      )
      await assert.rejects(
        () => redisClient.zRangeByLex(key, '', '+'),
        errorWithMessage(msg),
      )
      await assert.rejects(
        () => redisClient.zRange(key, 'g', '-', { BY: 'LEX', REV: true }),
        errorWithMessage(msg),
      )
      await assert.rejects(
        () => redisClient.zLexCount(key, 'g', '+'),
        errorWithMessage(msg),
      )
      await assert.rejects(
        () => redisClient.zRemRangeByLex(key, 'g', '+'),
        errorWithMessage(msg),
      )
    } finally {
      await redisClient.del(key)
    }
  })

  test('lex commands on a wrong-type key return WRONGTYPE', async () => {
    const key = `{lex:${randomKey()}}`
    await redisClient.set(key, 'notazset')
    const msg =
      'WRONGTYPE Operation against a key holding the wrong kind of value'
    try {
      await assert.rejects(
        () => redisClient.zRangeByLex(key, '-', '+'),
        errorWithMessage(msg),
      )
      await assert.rejects(
        () => redisClient.zRange(key, '+', '-', { BY: 'LEX', REV: true }),
        errorWithMessage(msg),
      )
      await assert.rejects(
        () => redisClient.zLexCount(key, '-', '+'),
        errorWithMessage(msg),
      )
      await assert.rejects(
        () => redisClient.zRemRangeByLex(key, '-', '+'),
        errorWithMessage(msg),
      )
    } finally {
      await redisClient.del(key)
    }
  })

  test('lex commands reject wrong arity', async () => {
    const key = await seed(['a', 'b', 'c'])
    try {
      await assert.rejects(
        () => redisClient.sendCommand(key, true, ['ZRANGEBYLEX', key, '-']),
        errorWithMessage(
          "ERR wrong number of arguments for 'zrangebylex' command",
        ),
      )
      // ZLEXCOUNT does not accept LIMIT
      await assert.rejects(
        () =>
          redisClient.sendCommand(key, true, [
            'ZLEXCOUNT',
            key,
            '-',
            '+',
            'LIMIT',
            '0',
            '1',
          ]),
        errorWithMessage(
          "ERR wrong number of arguments for 'zlexcount' command",
        ),
      )
      await assert.rejects(
        () =>
          redisClient.sendCommand(key, false, [
            'ZREMRANGEBYLEX',
            key,
            '-',
            '+',
            'extra',
          ]),
        errorWithMessage(
          "ERR wrong number of arguments for 'zremrangebylex' command",
        ),
      )
    } finally {
      await redisClient.del(key)
    }
  })

  test('ZRANGEBYLEX validates LIMIT clause', async () => {
    const key = await seed(['a', 'b', 'c'])
    try {
      await assert.rejects(
        () =>
          redisClient.sendCommand(key, true, [
            'ZRANGEBYLEX',
            key,
            '-',
            '+',
            'LIMIT',
            'x',
            'y',
          ]),
        errorWithMessage('ERR value is not an integer or out of range'),
      )
      await assert.rejects(
        () =>
          redisClient.sendCommand(key, true, [
            'ZRANGEBYLEX',
            key,
            '-',
            '+',
            'LIMIX',
            '0',
            '1',
          ]),
        errorWithMessage('ERR syntax error'),
      )
      await assert.rejects(
        () =>
          redisClient.sendCommand(key, true, [
            'ZRANGEBYLEX',
            key,
            '-',
            '+',
            'LIMIT',
            '0',
          ]),
        errorWithMessage('ERR syntax error'),
      )
    } finally {
      await redisClient.del(key)
    }
  })
})
