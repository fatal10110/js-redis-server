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
      assert.deepStrictEqual(await c.call('SORT', k('l')), [
        '1',
        '2',
        '3',
        '4',
        '5',
      ])
    })
  })

  test('SORT DESC reverses numeric order', async () => {
    await withOps(async (c, k) => {
      await c.rpush(k('l'), '3', '1', '2')
      assert.deepStrictEqual(await c.call('SORT', k('l'), 'DESC'), [
        '3',
        '2',
        '1',
      ])
    })
  })

  test('SORT sorts floats numerically', async () => {
    await withOps(async (c, k) => {
      await c.rpush(k('l'), '1.5', '1.1', '1.05', '2')
      assert.deepStrictEqual(await c.call('SORT', k('l')), [
        '1.05',
        '1.1',
        '1.5',
        '2',
      ])
    })
  })

  test('SORT keeps duplicate list elements', async () => {
    await withOps(async (c, k) => {
      await c.rpush(k('l'), '3', '1', '1', '2')
      assert.deepStrictEqual(await c.call('SORT', k('l')), ['1', '1', '2', '3'])
    })
  })

  test('SORT treats an empty-string element as numeric zero', async () => {
    await withOps(async (c, k) => {
      await c.rpush(k('l'), '', '2', '1')
      assert.deepStrictEqual(await c.call('SORT', k('l')), ['', '1', '2'])
    })
  })

  // -------------------------------------------------------------------- ALPHA

  test('SORT ALPHA sorts lexicographically', async () => {
    await withOps(async (c, k) => {
      await c.rpush(k('l'), 'banana', 'apple', 'cherry')
      assert.deepStrictEqual(await c.call('SORT', k('l'), 'ALPHA'), [
        'apple',
        'banana',
        'cherry',
      ])
    })
  })

  test('SORT ALPHA DESC reverses lexicographic order', async () => {
    await withOps(async (c, k) => {
      await c.rpush(k('l'), 'b', 'a', 'c')
      assert.deepStrictEqual(await c.call('SORT', k('l'), 'ALPHA', 'DESC'), [
        'c',
        'b',
        'a',
      ])
    })
  })

  test('SORT without ALPHA rejects non-numeric elements', async () => {
    await withOps(async (c, k) => {
      await c.rpush(k('l'), 'apple', 'banana')
      await assert.rejects(
        () => c.call('SORT', k('l')),
        errorWithMessage(
          "ERR One or more scores can't be converted into double",
        ),
      )
    })
  })

  // -------------------------------------------------------------------- LIMIT

  test('SORT LIMIT offset count paginates', async () => {
    await withOps(async (c, k) => {
      await c.rpush(k('l'), '3', '1', '2', '5', '4')
      assert.deepStrictEqual(await c.call('SORT', k('l'), 'LIMIT', '1', '2'), [
        '2',
        '3',
      ])
    })
  })

  test('SORT LIMIT with negative count returns all remaining', async () => {
    await withOps(async (c, k) => {
      await c.rpush(k('l'), '3', '1', '2', '5', '4')
      assert.deepStrictEqual(await c.call('SORT', k('l'), 'LIMIT', '1', '-1'), [
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
      assert.deepStrictEqual(
        await c.call('SORT', k('l'), 'LIMIT', '10', '5'),
        [],
      )
    })
  })

  test('SORT LIMIT rejects a non-integer bound', async () => {
    await withOps(async (c, k) => {
      await c.rpush(k('l'), '1', '2')
      await assert.rejects(
        () => c.call('SORT', k('l'), 'LIMIT', 'a', 'b'),
        errorWithMessage('ERR value is not an integer or out of range'),
      )
    })
  })

  // ------------------------------------------------------------- set / zset

  test('SORT sorts a set numerically', async () => {
    await withOps(async (c, k) => {
      await c.sadd(k('s'), '10', '2', '33', '4')
      assert.deepStrictEqual(await c.call('SORT', k('s')), [
        '2',
        '4',
        '10',
        '33',
      ])
    })
  })

  test('SORT sorts a zset by member, not score', async () => {
    await withOps(async (c, k) => {
      // scores chosen so score-order and member-order disagree
      await c.zadd(k('z'), '5', '30', '1', '20', '9', '10')
      assert.deepStrictEqual(await c.call('SORT', k('z')), ['10', '20', '30'])
    })
  })

  // -------------------------------------------------------------------- STORE

  test('SORT STORE writes the result as a list and returns its length', async () => {
    await withOps(async (c, k) => {
      await c.rpush(k('l'), '3', '1', '2')
      const n = await c.call('SORT', k('l'), 'STORE', k('dst'))
      assert.strictEqual(n, 3)
      assert.strictEqual(await c.call('TYPE', k('dst')), 'list')
      assert.deepStrictEqual(await c.call('LRANGE', k('dst'), '0', '-1'), [
        '1',
        '2',
        '3',
      ])
    })
  })

  test('SORT STORE with an empty result deletes the destination', async () => {
    await withOps(async (c, k) => {
      await c.rpush(k('dst'), 'pre-existing')
      const n = await c.call('SORT', k('missing'), 'STORE', k('dst'))
      assert.strictEqual(n, 0)
      assert.strictEqual(await c.exists(k('dst')), 0)
    })
  })

  // --------------------------------------------------------------- edge cases

  test('SORT on a missing key returns an empty array', async () => {
    await withOps(async (c, k) => {
      assert.deepStrictEqual(await c.call('SORT', k('missing')), [])
    })
  })

  test('SORT against a string key fails with WRONGTYPE', async () => {
    await withOps(async (c, k) => {
      await c.set(k('str'), 'hello')
      await assert.rejects(
        () => c.call('SORT', k('str')),
        errorWithMessage(
          'WRONGTYPE Operation against a key holding the wrong kind of value',
        ),
      )
    })
  })

  test('SORT with no key fails with wrong number of arguments', async () => {
    await withOps(async c => {
      await assert.rejects(
        () => c.call('SORT'),
        errorWithMessage("ERR wrong number of arguments for 'sort' command"),
      )
    })
  })

  test('SORT with an unknown option fails with a syntax error', async () => {
    await withOps(async (c, k) => {
      await c.rpush(k('l'), '1', '2')
      await assert.rejects(
        () => c.call('SORT', k('l'), 'FOO'),
        errorWithMessage('ERR syntax error'),
      )
    })
  })

  // ------------------------------------------------------------------ SORT_RO

  test('SORT_RO sorts like SORT', async () => {
    await withOps(async (c, k) => {
      await c.rpush(k('l'), '3', '1', '2')
      assert.deepStrictEqual(await c.call('SORT_RO', k('l')), ['1', '2', '3'])
    })
  })

  test('SORT_RO rejects STORE with a syntax error', async () => {
    await withOps(async (c, k) => {
      await c.rpush(k('l'), '1', '2')
      await assert.rejects(
        () => c.call('SORT_RO', k('l'), 'STORE', k('dst')),
        errorWithMessage('ERR syntax error'),
      )
    })
  })
})
