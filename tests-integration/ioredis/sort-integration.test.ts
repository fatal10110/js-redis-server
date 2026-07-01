import { test, describe, before, after } from 'node:test'
import assert from 'node:assert'
import { Cluster, Redis } from 'ioredis'
import { TestRunner } from '../test-config'
import { connectToSlotOwner, errorWithMessage, randomKey } from '../utils'

const testRunner = new TestRunner()

describe(`SORT / SORT_RO (${testRunner.getBackendName()})`, () => {
  let redisClient: Cluster | undefined

  before(async () => {
    redisClient = await testRunner.setupIoredisCluster('sort-integration')
  })

  after(async () => {
    await testRunner.cleanup()
  })

  // Source key and STORE destination must hash to the same slot, so every test
  // shares a hash tag and talks to that slot's owner through a prefix-free
  // directClient.
  async function withOps(
    fn: (client: Redis, k: (name: string) => string) => Promise<void>,
  ): Promise<void> {
    assert.ok(redisClient)
    const tag = `{sort:${randomKey()}}`
    const k = (name: string) => `${tag}:${name}`
    const directClient = await connectToSlotOwner(redisClient, k('seed'))
    try {
      await fn(directClient, k)
    } finally {
      directClient.disconnect()
    }
  }

  // ------------------------------------------------------------------ numeric

  test('SORT numerically ascending by default', async () => {
    await withOps(async (c, k) => {
      await c.rpush(k('l'), '3', '1', '2', '5', '4')
      assert.deepStrictEqual(await c.sort(k('l')), ['1', '2', '3', '4', '5'])
    })
  })

  test('SORT DESC reverses numeric order', async () => {
    await withOps(async (c, k) => {
      await c.rpush(k('l'), '3', '1', '2')
      assert.deepStrictEqual(await c.sort(k('l'), 'DESC'), ['3', '2', '1'])
    })
  })

  test('SORT sorts floats numerically', async () => {
    await withOps(async (c, k) => {
      await c.rpush(k('l'), '1.5', '1.1', '1.05', '2')
      assert.deepStrictEqual(await c.sort(k('l')), ['1.05', '1.1', '1.5', '2'])
    })
  })

  test('SORT keeps duplicate list elements', async () => {
    await withOps(async (c, k) => {
      await c.rpush(k('l'), '3', '1', '1', '2')
      assert.deepStrictEqual(await c.sort(k('l')), ['1', '1', '2', '3'])
    })
  })

  test('SORT treats an empty-string element as numeric zero', async () => {
    await withOps(async (c, k) => {
      await c.rpush(k('l'), '', '2', '1')
      assert.deepStrictEqual(await c.sort(k('l')), ['', '1', '2'])
    })
  })

  // -------------------------------------------------------------------- ALPHA

  test('SORT ALPHA sorts lexicographically', async () => {
    await withOps(async (c, k) => {
      await c.rpush(k('l'), 'banana', 'apple', 'cherry')
      assert.deepStrictEqual(await c.sort(k('l'), 'ALPHA'), [
        'apple',
        'banana',
        'cherry',
      ])
    })
  })

  test('SORT ALPHA DESC reverses lexicographic order', async () => {
    await withOps(async (c, k) => {
      await c.rpush(k('l'), 'b', 'a', 'c')
      assert.deepStrictEqual(await c.sort(k('l'), 'ALPHA', 'DESC'), [
        'c',
        'b',
        'a',
      ])
    })
  })

  test('SORT LIMIT offset count paginates', async () => {
    await withOps(async (c, k) => {
      await c.rpush(k('l'), '3', '1', '2', '5', '4')
      assert.deepStrictEqual(await c.sort(k('l'), 'LIMIT', '1', '2'), [
        '2',
        '3',
      ])
    })
  })

  test('SORT LIMIT with negative count returns all remaining', async () => {
    await withOps(async (c, k) => {
      await c.rpush(k('l'), '3', '1', '2', '5', '4')
      assert.deepStrictEqual(await c.sort(k('l'), 'LIMIT', '1', '-1'), [
        '2',
        '3',
        '4',
        '5',
      ])
    })
  })

  test('SORT LIMIT offset past the end returns empty', async () => {
    await withOps(async (c, k) => {
      await c.rpush(k('l'), '3', '1', '2')
      assert.deepStrictEqual(await c.sort(k('l'), 'LIMIT', '10', '5'), [])
    })
  })

  test('SORT sorts a set numerically', async () => {
    await withOps(async (c, k) => {
      await c.sadd(k('s'), '10', '2', '33', '4')
      assert.deepStrictEqual(await c.sort(k('s')), ['2', '4', '10', '33'])
    })
  })

  test('SORT sorts a zset by member, not score', async () => {
    await withOps(async (c, k) => {
      // scores chosen so score-order and member-order disagree
      await c.zadd(k('z'), '5', '30', '1', '20', '9', '10')
      assert.deepStrictEqual(await c.sort(k('z')), ['10', '20', '30'])
    })
  })

  // -------------------------------------------------------------------- STORE

  test('SORT STORE writes the result as a list and returns its length', async () => {
    await withOps(async (c, k) => {
      await c.rpush(k('l'), '3', '1', '2')
      const n = await c.sort(k('l'), 'STORE', k('dst'))
      assert.strictEqual(n, 3)
      assert.strictEqual(await c.type(k('dst')), 'list')
      assert.deepStrictEqual(await c.lrange(k('dst'), '0', '-1'), [
        '1',
        '2',
        '3',
      ])
    })
  })

  test('SORT STORE with an empty result deletes the destination', async () => {
    await withOps(async (c, k) => {
      await c.rpush(k('dst'), 'pre-existing')
      const n = await c.sort(k('missing'), 'STORE', k('dst'))
      assert.strictEqual(n, 0)
      assert.strictEqual(await c.exists(k('dst')), 0)
    })
  })

  // ------------------------------------------------------------------- BY/GET

  test('SORT BY orders by external keys and GET returns pattern values', async () => {
    await withOps(async (c, k) => {
      await c.rpush(k('ids'), '2', '1', '3')
      await c.set(k('weight:1'), '20')
      await c.set(k('weight:2'), '10')
      await c.set(k('weight:3'), '30')
      await c.set(k('name:1'), 'one')
      await c.set(k('name:2'), 'two')
      await c.set(k('name:3'), 'three')

      assert.deepStrictEqual(
        await c.sort(k('ids'), 'BY', k('weight:*'), 'GET', k('name:*')),
        ['two', 'one', 'three'],
      )
    })
  })

  test('SORT GET # returns source elements and missing pattern values as null', async () => {
    await withOps(async (c, k) => {
      await c.rpush(k('ids'), '2', '1', '3')
      await c.set(k('weight:1'), '20')
      await c.set(k('weight:2'), '10')
      await c.set(k('weight:3'), '30')

      assert.deepStrictEqual(
        await c.sort(
          k('ids'),
          'BY',
          k('weight:*'),
          'GET',
          '#',
          'GET',
          k('missing:*'),
        ),
        ['2', null, '1', null, '3', null],
      )
    })
  })

  test('SORT_RO supports BY and GET external patterns', async () => {
    await withOps(async (c, k) => {
      await c.rpush(k('ids'), 'a', 'b')
      await c.set(k('weight:a'), '2')
      await c.set(k('weight:b'), '1')
      await c.set(k('name:a'), 'alpha')
      await c.set(k('name:b'), 'bravo')

      assert.deepStrictEqual(
        await c.sort_ro(k('ids'), 'BY', k('weight:*'), 'GET', k('name:*')),
        ['bravo', 'alpha'],
      )
    })
  })

  test('SORT rejects BY or GET patterns that hash to a different slot', async () => {
    await withOps(async (c, k) => {
      const otherTag = `{sort-other:${randomKey()}}`
      await c.rpush(k('ids'), '1')
      await c.set(k('weight:1'), '1')

      await assert.rejects(
        () => c.sort(k('ids'), 'BY', `${otherTag}:weight:*`),
        errorWithMessage(
          'ERR BY option of SORT denied in Cluster mode when keys formed by the pattern may be in different slots.',
        ),
      )
      await assert.rejects(
        () =>
          c.sort(k('ids'), 'BY', k('weight:*'), 'GET', `${otherTag}:name:*`),
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

  test('SORT_RO sorts like SORT', async () => {
    await withOps(async (c, k) => {
      await c.rpush(k('l'), '3', '1', '2')
      assert.deepStrictEqual(await c.sort_ro(k('l')), ['1', '2', '3'])
    })
  })
})
