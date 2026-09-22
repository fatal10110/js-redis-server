import { test, describe, before, after } from 'node:test'
import assert from 'node:assert'
import { RedisClientType, RedisClusterType } from 'redis'
import { TestRunner } from '../test-config'
import {
  connectToNodeRedisSlotOwner,
  errorWithMessage,
  flushNodeRedisCluster,
  randomKey,
} from '../utils'

const testRunner = new TestRunner()

describe(`SORT / SORT_RO (node-redis, ${testRunner.getBackendName()})`, () => {
  let redisClient: RedisClusterType

  before(async () => {
    redisClient = (await testRunner.setupNodeRedisCluster()) as RedisClusterType
    await flushNodeRedisCluster(redisClient)
  })

  after(async () => {
    await testRunner.cleanup()
  })

  // Source key and STORE destination must hash to the same slot, so every test
  // shares a hash tag and talks to that slot's owner through a prefix-free
  // directClient.
  async function withOps(
    fn: (client: RedisClientType, k: (name: string) => string) => Promise<void>,
  ): Promise<void> {
    assert.ok(redisClient)
    const tag = `{sort:${randomKey()}}`
    const k = (name: string) => `${tag}:${name}`
    const directClient = await connectToNodeRedisSlotOwner(
      redisClient,
      k('seed'),
    )
    try {
      await fn(directClient, k)
    } finally {
      directClient.destroy()
    }
  }

  // ------------------------------------------------------------------ numeric

  test('SORT numerically ascending by default', async () => {
    await withOps(async (c, k) => {
      await c.rPush(k('l'), ['3', '1', '2', '5', '4'])
      assert.deepStrictEqual(await c.sort(k('l')), ['1', '2', '3', '4', '5'])
    })
  })

  test('SORT DESC reverses numeric order', async () => {
    await withOps(async (c, k) => {
      await c.rPush(k('l'), ['3', '1', '2'])
      assert.deepStrictEqual(await c.sort(k('l'), { DIRECTION: 'DESC' }), [
        '3',
        '2',
        '1',
      ])
    })
  })

  test('SORT sorts floats numerically', async () => {
    await withOps(async (c, k) => {
      await c.rPush(k('l'), ['1.5', '1.1', '1.05', '2'])
      assert.deepStrictEqual(await c.sort(k('l')), ['1.05', '1.1', '1.5', '2'])
    })
  })

  test('SORT keeps duplicate list elements', async () => {
    await withOps(async (c, k) => {
      await c.rPush(k('l'), ['3', '1', '1', '2'])
      assert.deepStrictEqual(await c.sort(k('l')), ['1', '1', '2', '3'])
    })
  })

  test('SORT treats an empty-string element as numeric zero', async () => {
    await withOps(async (c, k) => {
      await c.rPush(k('l'), ['', '2', '1'])
      assert.deepStrictEqual(await c.sort(k('l')), ['', '1', '2'])
    })
  })

  // -------------------------------------------------------------------- ALPHA

  test('SORT ALPHA sorts lexicographically', async () => {
    await withOps(async (c, k) => {
      await c.rPush(k('l'), ['banana', 'apple', 'cherry'])
      assert.deepStrictEqual(await c.sort(k('l'), { ALPHA: true }), [
        'apple',
        'banana',
        'cherry',
      ])
    })
  })

  test('SORT ALPHA DESC reverses lexicographic order', async () => {
    await withOps(async (c, k) => {
      await c.rPush(k('l'), ['b', 'a', 'c'])
      assert.deepStrictEqual(
        await c.sort(k('l'), { ALPHA: true, DIRECTION: 'DESC' }),
        ['c', 'b', 'a'],
      )
    })
  })

  test('SORT without ALPHA rejects non-numeric elements', async () => {
    await withOps(async (c, k) => {
      await c.rPush(k('l'), ['apple', 'banana'])
      await assert.rejects(
        () => c.sort(k('l')),
        errorWithMessage(
          "ERR One or more scores can't be converted into double",
        ),
      )
    })
  })

  // -------------------------------------------------------------------- LIMIT

  test('SORT LIMIT offset count paginates', async () => {
    await withOps(async (c, k) => {
      await c.rPush(k('l'), ['3', '1', '2', '5', '4'])
      assert.deepStrictEqual(
        await c.sort(k('l'), { LIMIT: { offset: 1, count: 2 } }),
        ['2', '3'],
      )
    })
  })

  test('SORT LIMIT with negative count returns all remaining', async () => {
    await withOps(async (c, k) => {
      await c.rPush(k('l'), ['3', '1', '2', '5', '4'])
      assert.deepStrictEqual(
        await c.sort(k('l'), { LIMIT: { offset: 1, count: -1 } }),
        ['2', '3', '4', '5'],
      )
    })
  })

  test('SORT LIMIT offset past the end returns empty', async () => {
    await withOps(async (c, k) => {
      await c.rPush(k('l'), ['3', '1', '2'])
      assert.deepStrictEqual(
        await c.sort(k('l'), { LIMIT: { offset: 10, count: 5 } }),
        [],
      )
    })
  })

  test('SORT LIMIT rejects a non-integer bound', async () => {
    await withOps(async (c, k) => {
      await c.rPush(k('l'), ['1', '2'])
      await assert.rejects(
        () => c.sendCommand(['SORT', k('l'), 'LIMIT', 'a', 'b']),
        errorWithMessage('ERR value is not an integer or out of range'),
      )
    })
  })

  // ------------------------------------------------------------- set / zset

  test('SORT sorts a set numerically', async () => {
    await withOps(async (c, k) => {
      await c.sAdd(k('s'), ['10', '2', '33', '4'])
      assert.deepStrictEqual(await c.sort(k('s')), ['2', '4', '10', '33'])
    })
  })

  test('SORT sorts a zset by member, not score', async () => {
    await withOps(async (c, k) => {
      // scores chosen so score-order and member-order disagree
      await c.zAdd(k('z'), [
        { score: 5, value: '30' },
        { score: 1, value: '20' },
        { score: 9, value: '10' },
      ])
      assert.deepStrictEqual(await c.sort(k('z')), ['10', '20', '30'])
    })
  })

  test('SORT with a constant BY reads a zset in rank order', async () => {
    await withOps(async (c, k) => {
      // sortCommand()'s dontsort path walks the skiplist, so the reply is in
      // rank order. The fixture keeps all three candidate orders apart, so
      // this cannot pass by accident:
      //   insertion a, b, c   ALPHA a, b, c   rank c, a, b
      await c.zAdd(k('z'), [
        { score: 2, value: 'a' },
        { score: 3, value: 'b' },
        { score: 1, value: 'c' },
      ])
      await c.mSet([
        [k('w_a'), 'A'],
        [k('w_b'), 'B'],
        [k('w_c'), 'C'],
      ])

      assert.deepStrictEqual(await c.zRange(k('z'), 0, -1), ['c', 'a', 'b'])
      assert.deepStrictEqual(await c.sort(k('z'), { BY: 'nosort' }), [
        'c',
        'a',
        'b',
      ])
      assert.deepStrictEqual(await c.sortRo(k('z'), { BY: 'nosort' }), [
        'c',
        'a',
        'b',
      ])
      // Any BY pattern without a '*' is constant, so it takes the same path.
      assert.deepStrictEqual(await c.sort(k('z'), { BY: k('konstant') }), [
        'c',
        'a',
        'b',
      ])

      // LIMIT, GET and STORE all consume that same source ordering.
      assert.deepStrictEqual(
        await c.sort(k('z'), { BY: 'nosort', LIMIT: { offset: 1, count: 5 } }),
        ['a', 'b'],
      )
      assert.deepStrictEqual(
        await c.sort(k('z'), { BY: 'nosort', GET: k('w_*') }),
        ['C', 'A', 'B'],
      )
      assert.strictEqual(
        await c.sortStore(k('z'), k('zd'), { BY: 'nosort' }),
        3,
      )
      assert.deepStrictEqual(await c.lRange(k('zd'), 0, -1), ['c', 'a', 'b'])

      // A zset is already ordered, so it is never force-sorted ALPHA the way
      // an unordered set is inside a script — ALPHA would say a, b, c here.
      assert.deepStrictEqual(
        await c.eval("return redis.call('SORT', KEYS[1], 'BY', 'nosort')", {
          keys: [k('z')],
        }),
        ['c', 'a', 'b'],
      )
    })
  })

  test('SORT with a constant BY keeps a zset rank tie-break by raw bytes', async () => {
    await withOps(async (c, k) => {
      // Equal scores rank by memcmp, so the uppercase members sort first —
      // the same order ZRANGE reports, and not the insertion order.
      await c.zAdd(k('z'), [
        { score: 1, value: 'b' },
        { score: 1, value: 'a' },
        { score: 1, value: 'c' },
        { score: 1, value: 'A' },
        { score: 1, value: 'B' },
      ])

      assert.deepStrictEqual(await c.zRange(k('z'), 0, -1), [
        'A',
        'B',
        'a',
        'b',
        'c',
      ])
      assert.deepStrictEqual(await c.sort(k('z'), { BY: 'nosort' }), [
        'A',
        'B',
        'a',
        'b',
        'c',
      ])
    })
  })

  test('SORT with a constant BY reads the source backwards for DESC', async () => {
    await withOps(async (c, k) => {
      // dontsort does not mean "ignore DESC": sortCommand() iterates the list
      // from its head and the skiplist from its tail instead, with LIMIT
      // applied to that reversed walk. A set has no such branch, so DESC is
      // genuinely a no-op there.
      await c.rPush(k('l'), ['a', 'c', 'b'])
      await c.zAdd(k('z'), [
        { score: 2, value: 'a' },
        { score: 3, value: 'b' },
        { score: 1, value: 'c' },
      ])
      await c.sAdd(k('s'), ['a', 'c', 'b'])

      assert.deepStrictEqual(
        await c.sort(k('l'), { BY: 'nosort', DIRECTION: 'DESC' }),
        ['b', 'c', 'a'],
      )
      assert.deepStrictEqual(
        await c.sort(k('z'), { BY: 'nosort', DIRECTION: 'DESC' }),
        ['b', 'a', 'c'],
      )
      assert.deepStrictEqual(
        await c.sortRo(k('z'), { BY: 'nosort', DIRECTION: 'DESC' }),
        ['b', 'a', 'c'],
      )
      assert.deepStrictEqual(
        await c.sort(k('z'), {
          BY: 'nosort',
          DIRECTION: 'DESC',
          LIMIT: { offset: 0, count: 2 },
        }),
        ['b', 'a'],
      )
      assert.deepStrictEqual(
        await c.sort(k('z'), {
          BY: 'nosort',
          DIRECTION: 'DESC',
          LIMIT: { offset: 1, count: 5 },
        }),
        ['a', 'c'],
      )

      // A set is unordered, so DESC changes nothing about its plain reply.
      assert.deepStrictEqual(
        await c.sort(k('s'), { BY: 'nosort', DIRECTION: 'DESC' }),
        await c.sort(k('s'), { BY: 'nosort' }),
      )

      // STORE consumes the reversed order for a list and a zset, while a set
      // is still force-sorted ALPHA first and only then reversed.
      await c.sortStore(k('l'), k('ld'), { BY: 'nosort', DIRECTION: 'DESC' })
      assert.deepStrictEqual(await c.lRange(k('ld'), 0, -1), ['b', 'c', 'a'])
      await c.sortStore(k('z'), k('zd'), { BY: 'nosort', DIRECTION: 'DESC' })
      assert.deepStrictEqual(await c.lRange(k('zd'), 0, -1), ['b', 'a', 'c'])
      await c.sortStore(k('s'), k('sd'), { BY: 'nosort', DIRECTION: 'DESC' })
      assert.deepStrictEqual(await c.lRange(k('sd'), 0, -1), ['c', 'b', 'a'])
    })
  })

  // -------------------------------------------------------------------- STORE

  test('SORT STORE writes the result as a list and returns its length', async () => {
    await withOps(async (c, k) => {
      await c.rPush(k('l'), ['3', '1', '2'])
      const n = await c.sortStore(k('l'), k('dst'))
      assert.strictEqual(n, 3)
      assert.strictEqual(await c.type(k('dst')), 'list')
      assert.deepStrictEqual(await c.lRange(k('dst'), 0, -1), ['1', '2', '3'])
    })
  })

  test('SORT STORE with an empty result deletes the destination', async () => {
    await withOps(async (c, k) => {
      await c.rPush(k('dst'), 'pre-existing')
      const n = await c.sortStore(k('missing'), k('dst'))
      assert.strictEqual(n, 0)
      assert.strictEqual(await c.exists(k('dst')), 0)
    })
  })

  // ------------------------------------------------------------------- BY/GET

  test('SORT BY orders by external keys and GET returns pattern values', async () => {
    await withOps(async (c, k) => {
      await c.rPush(k('ids'), ['2', '1', '3'])
      await c.set(k('weight:1'), '20')
      await c.set(k('weight:2'), '10')
      await c.set(k('weight:3'), '30')
      await c.set(k('name:1'), 'one')
      await c.set(k('name:2'), 'two')
      await c.set(k('name:3'), 'three')

      assert.deepStrictEqual(
        await c.sort(k('ids'), { BY: k('weight:*'), GET: k('name:*') }),
        ['two', 'one', 'three'],
      )
    })
  })

  test('SORT GET # returns source elements and missing pattern values as null', async () => {
    await withOps(async (c, k) => {
      await c.rPush(k('ids'), ['2', '1', '3'])
      await c.set(k('weight:1'), '20')
      await c.set(k('weight:2'), '10')
      await c.set(k('weight:3'), '30')

      assert.deepStrictEqual(
        await c.sort(k('ids'), {
          BY: k('weight:*'),
          GET: ['#', k('missing:*')],
        }),
        ['2', null, '1', null, '3', null],
      )
    })
  })

  test('SORT_RO supports BY and GET external patterns', async () => {
    await withOps(async (c, k) => {
      await c.rPush(k('ids'), ['a', 'b'])
      await c.set(k('weight:a'), '2')
      await c.set(k('weight:b'), '1')
      await c.set(k('name:a'), 'alpha')
      await c.set(k('name:b'), 'bravo')

      assert.deepStrictEqual(
        await c.sortRo(k('ids'), { BY: k('weight:*'), GET: k('name:*') }),
        ['bravo', 'alpha'],
      )
    })
  })

  test('SORT breaks equal numeric weights lexicographically', async () => {
    await withOps(async (c, k) => {
      // Every weight key is missing, so all elements score 0 and sortCompare()
      // falls through to comparing the elements themselves.
      await c.rPush(k('l'), ['3', '1', '2'])
      assert.deepStrictEqual(await c.sort(k('l'), { BY: k('missing:*') }), [
        '1',
        '2',
        '3',
      ])
    })
  })

  test('SORT ALPHA BY missing weights keeps the source order', async () => {
    await withOps(async (c, k) => {
      // ALPHA with a BY whose weights are all absent genuinely compares equal
      // in sortCompare(), so this one is *not* re-ordered.
      await c.rPush(k('l'), ['c', 'a', 'b'])
      assert.deepStrictEqual(
        await c.sort(k('l'), { BY: k('missing:*'), ALPHA: true }),
        ['c', 'a', 'b'],
      )
    })
  })

  test('SORT GET with a constant pattern yields nil for every element', async () => {
    await withOps(async (c, k) => {
      // lookupKeyByPattern() bails out when the pattern has no '*', so the
      // constant key is never read even though it exists.
      await c.rPush(k('l'), ['3', '1', '2'])
      await c.set(k('const'), 'HELLO')
      assert.deepStrictEqual(
        await c.sort(k('l'), { BY: 'nosort', GET: k('const') }),
        [null, null, null],
      )
    })
  })

  test('SORT treats a non-string weight or GET key as missing', async () => {
    await withOps(async (c, k) => {
      await c.rPush(k('l'), ['3', '1', '2'])
      await c.rPush(k('w:1'), ['not-a-string'])
      await c.set(k('w:2'), '5')
      await c.set(k('w:3'), '1')

      assert.deepStrictEqual(await c.sort(k('l'), { BY: k('w:*') }), [
        '1',
        '3',
        '2',
      ])
      assert.deepStrictEqual(await c.sort(k('l'), { GET: k('w:*') }), [
        null,
        '5',
        '1',
      ])
    })
  })

  test('SORT force-sorts a set with a constant BY when the order must be reproducible', async () => {
    await withOps(async (c, k) => {
      await c.sAdd(k('s'), ['c', 'a', 'b'])

      // STORE and scripts must be reproducible, so ALPHA is forced.
      assert.strictEqual(await c.sortStore(k('s'), k('d'), { BY: 'nosort' }), 3)
      assert.deepStrictEqual(await c.lRange(k('d'), 0, -1), ['a', 'b', 'c'])
      assert.deepStrictEqual(
        await c.eval("return redis.call('SORT', KEYS[1], 'BY', 'nosort')", {
          keys: [k('s')],
        }),
        ['a', 'b', 'c'],
      )

      // A list already has a defined order, so it is not force-sorted.
      await c.rPush(k('ll'), ['c', 'a', 'b'])
      await c.sortStore(k('ll'), k('ld'), { BY: 'nosort' })
      assert.deepStrictEqual(await c.lRange(k('ld'), 0, -1), ['c', 'a', 'b'])
    })
  })

  test('SORT BY nosort skips sorting in cluster mode', async () => {
    await withOps(async (c, k) => {
      await c.rPush(k('l'), ['3', '1', '2'])
      assert.deepStrictEqual(await c.sort(k('l'), { BY: 'nosort' }), [
        '3',
        '1',
        '2',
      ])
      assert.deepStrictEqual(await c.sortRo(k('l'), { BY: 'nosort' }), [
        '3',
        '1',
        '2',
      ])
    })
  })

  test('SORT BY a constant pattern skips sorting whatever slot it hashes to', async () => {
    await withOps(async (c, k) => {
      // No '*' means the pattern is constant: every element gets the same
      // weight, so real Redis never looks the key up and never sorts — the
      // cluster slot of the constant is therefore irrelevant.
      await c.rPush(k('l'), ['3', '1', '2'])
      assert.deepStrictEqual(
        await c.sort(k('l'), { BY: `{sort-other:${randomKey()}}:weight` }),
        ['3', '1', '2'],
      )
    })
  })

  test('SORT BY a glob hash-tagged to an untagged source key is allowed', async () => {
    // The source key carries no hash tag of its own, so the BY pattern's tag
    // has to be compared by *slot* against the key, not by tag bytes.
    const key = `sort-untagged:${randomKey()}`
    const directClient = await connectToNodeRedisSlotOwner(redisClient, key)
    try {
      await directClient.rPush(key, ['2', '1'])
      await directClient.set(`{${key}}:weight:1`, '20')
      await directClient.set(`{${key}}:weight:2`, '10')
      assert.deepStrictEqual(
        await directClient.sort(key, { BY: `{${key}}:weight:*` }),
        ['2', '1'],
      )
    } finally {
      directClient.destroy()
    }
  })

  test('SORT rejects BY or GET patterns that hash to a different slot', async () => {
    await withOps(async (c, k) => {
      const otherTag = `{sort-other:${randomKey()}}`
      await c.rPush(k('ids'), '1')
      await c.set(k('weight:1'), '1')

      await assert.rejects(
        () => c.sort(k('ids'), { BY: `${otherTag}:weight:*` }),
        errorWithMessage(
          'ERR BY option of SORT denied in Cluster mode when keys formed by the pattern may be in different slots.',
        ),
      )
      await assert.rejects(
        () =>
          c.sort(k('ids'), {
            BY: k('weight:*'),
            GET: `${otherTag}:name:*`,
          }),
        errorWithMessage(
          'ERR GET option of SORT denied in Cluster mode when keys formed by the pattern may be in different slots.',
        ),
      )
    })
  })

  // --------------------------------------------------------------- edge cases

  test('SORT on a missing key returns an empty array', async () => {
    await withOps(async (c, k) => {
      assert.deepStrictEqual(await c.sort(k('missing')), [])
    })
  })

  test('SORT against a string key fails with WRONGTYPE', async () => {
    await withOps(async (c, k) => {
      await c.set(k('str'), 'hello')
      await assert.rejects(
        () => c.sort(k('str')),
        errorWithMessage(
          'WRONGTYPE Operation against a key holding the wrong kind of value',
        ),
      )
    })
  })

  test('SORT with no key fails with wrong number of arguments', async () => {
    await withOps(async c => {
      await assert.rejects(
        () => c.sendCommand(['SORT']),
        errorWithMessage("ERR wrong number of arguments for 'sort' command"),
      )
    })
  })

  test('SORT with an unknown option fails with a syntax error', async () => {
    await withOps(async (c, k) => {
      await c.rPush(k('l'), ['1', '2'])
      await assert.rejects(
        () => c.sendCommand(['SORT', k('l'), 'FOO']),
        errorWithMessage('ERR syntax error'),
      )
    })
  })

  // ------------------------------------------------------------------ SORT_RO

  test('SORT_RO sorts like SORT', async () => {
    await withOps(async (c, k) => {
      await c.rPush(k('l'), ['3', '1', '2'])
      assert.deepStrictEqual(await c.sortRo(k('l')), ['1', '2', '3'])
    })
  })

  test('SORT_RO rejects STORE with a syntax error', async () => {
    await withOps(async (c, k) => {
      await c.rPush(k('l'), ['1', '2'])
      await assert.rejects(
        () => c.sendCommand(['SORT_RO', k('l'), 'STORE', k('dst')]),
        errorWithMessage('ERR syntax error'),
      )
    })
  })
})
