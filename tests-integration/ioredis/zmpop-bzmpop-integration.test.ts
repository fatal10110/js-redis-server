import { test, describe, before, after } from 'node:test'
import assert from 'node:assert'
import { Cluster } from 'ioredis'
import { TestRunner } from '../test-config'
import { connectToSlotOwner, errorWithMessage, randomKey } from '../utils'

const testRunner = new TestRunner()

function waitForPark(ms = 80): Promise<void> {
  return new Promise(resolve => setTimeout(resolve, ms))
}

describe(`ZMPOP / BZMPOP Integration (${testRunner.getBackendName()})`, () => {
  let client1: Cluster | undefined
  let client2: Cluster | undefined

  before(async () => {
    client1 = await testRunner.setupIoredisCluster()
    client2 = await testRunner.setupIoredisCluster()
  })

  after(async () => {
    await testRunner.cleanup()
  })

  test('ZMPOP pops from the first non-empty key with the requested side and count', async () => {
    const tag = `{${randomKey()}}`
    const empty = `${tag}:empty`
    const first = `${tag}:first`
    const second = `${tag}:second`

    try {
      await client1!.del(empty, first, second)
      await client1!.zadd(first, 1, 'a', 2, 'b', 3, 'c')
      await client1!.zadd(second, 10, 'x', 20, 'y')

      assert.deepStrictEqual(
        await client1!.call(
          'ZMPOP',
          '3',
          empty,
          first,
          second,
          'MIN',
          'COUNT',
          '2',
        ),
        [
          first,
          [
            ['a', '1'],
            ['b', '2'],
          ],
        ],
      )
      assert.deepStrictEqual(await client1!.zrange(first, 0, -1), ['c'])
      assert.deepStrictEqual(await client1!.zrange(second, 0, -1), ['x', 'y'])

      assert.deepStrictEqual(
        await client1!.call('ZMPOP', '2', first, second, 'MAX'),
        [first, [['c', '3']]],
      )
      assert.strictEqual(await client1!.exists(first), 0)

      assert.deepStrictEqual(
        await client1!.call('ZMPOP', '2', first, second, 'mAx', 'count', '5'),
        [
          second,
          [
            ['y', '20'],
            ['x', '10'],
          ],
        ],
      )
      assert.strictEqual(await client1!.exists(second), 0)
    } finally {
      await client1!.del(empty, first, second)
    }
  })

  test('ZMPOP returns null when all keys are empty or missing', async () => {
    const tag = `{${randomKey()}}`
    const first = `${tag}:first`
    const second = `${tag}:second`

    try {
      await client1!.del(first, second)
      assert.strictEqual(
        await client1!.call('ZMPOP', '2', first, second, 'MIN'),
        null,
      )
    } finally {
      await client1!.del(first, second)
    }
  })

  test('BZMPOP returns immediately when a sorted set is non-empty', async () => {
    const tag = `{${randomKey()}}`
    const empty = `${tag}:empty`
    const zset = `${tag}:zset`

    try {
      await client1!.del(empty, zset)
      await client1!.zadd(zset, 1, 'one', 2, 'two', 3, 'three')

      assert.deepStrictEqual(
        await client1!.call(
          'BZMPOP',
          '1',
          '2',
          empty,
          zset,
          'MAX',
          'COUNT',
          '2',
        ),
        [
          zset,
          [
            ['three', '3'],
            ['two', '2'],
          ],
        ],
      )
      assert.deepStrictEqual(await client1!.zrange(zset, 0, -1), ['one'])
    } finally {
      await client1!.del(empty, zset)
    }
  })

  test('BZMPOP blocks then returns when a zset write arrives', async () => {
    const tag = `{${randomKey()}}`
    const first = `${tag}:first`
    const second = `${tag}:second`

    try {
      await client1!.del(first, second)

      const blockPromise = client1!.call(
        'BZMPOP',
        '5',
        '2',
        first,
        second,
        'MIN',
        'COUNT',
        '2',
      )
      await waitForPark()
      await client2!.zadd(second, 1, 'a', 2, 'b')

      assert.deepStrictEqual(await blockPromise, [
        second,
        [
          ['a', '1'],
          ['b', '2'],
        ],
      ])
      assert.strictEqual(await client1!.exists(second), 0)
    } finally {
      await client1!.del(first, second)
    }
  })

  test('BZMPOP timeout returns null', async () => {
    const tag = `{${randomKey()}}`
    const first = `${tag}:first`
    const second = `${tag}:second`

    try {
      await client1!.del(first, second)
      assert.strictEqual(
        await client1!.call('BZMPOP', '0.1', '2', first, second, 'MIN'),
        null,
      )
    } finally {
      await client1!.del(first, second)
    }
  })

  test('ZMPOP / BZMPOP error paths match Redis', async () => {
    const tag = `{${randomKey()}}`
    const zset = `${tag}:zset`
    const other = `${tag}:other`
    const stringKey = `${tag}:string`

    try {
      await client1!.del(zset, other, stringKey)
      await client1!.zadd(zset, 1, 'value')
      await client1!.set(stringKey, 'not-a-zset')

      await assert.rejects(
        () => client1!.call('ZMPOP'),
        errorWithMessage("ERR wrong number of arguments for 'zmpop' command"),
      )
      await assert.rejects(
        () => client1!.call('ZMPOP', '0', zset, 'MIN'),
        errorWithMessage('ERR numkeys should be greater than 0'),
      )
      await assert.rejects(
        () => client1!.call('ZMPOP', 'abc', zset, 'MIN'),
        errorWithMessage('ERR numkeys should be greater than 0'),
      )
      await assert.rejects(
        () => client1!.call('ZMPOP', '2', zset, other),
        errorWithMessage('ERR syntax error'),
      )
      await assert.rejects(
        () => client1!.call('ZMPOP', '1', zset, 'MIDDLE'),
        errorWithMessage('ERR syntax error'),
      )
      await assert.rejects(
        () => client1!.call('ZMPOP', '1', zset, 'MIN', 'LIMIT', '1'),
        errorWithMessage('ERR syntax error'),
      )
      await assert.rejects(
        () => client1!.call('ZMPOP', '1', zset, 'MIN', 'COUNT'),
        errorWithMessage('ERR syntax error'),
      )
      await assert.rejects(
        () => client1!.call('ZMPOP', '1', zset, 'MIN', 'COUNT', '0'),
        errorWithMessage('ERR count should be greater than 0'),
      )
      await assert.rejects(
        () => client1!.call('ZMPOP', '1', zset, 'MIN', 'COUNT', '-1'),
        errorWithMessage('ERR count should be greater than 0'),
      )
      await assert.rejects(
        () => client1!.call('ZMPOP', '1', zset, 'MIN', 'COUNT', 'abc'),
        errorWithMessage('ERR count should be greater than 0'),
      )
      await assert.rejects(
        () =>
          client1!.call('ZMPOP', '1', zset, 'MIN', 'COUNT', '1', 'COUNT', '1'),
        errorWithMessage('ERR syntax error'),
      )
      await assert.rejects(
        () => client1!.call('ZMPOP', '1', stringKey, 'MIN'),
        errorWithMessage(
          'WRONGTYPE Operation against a key holding the wrong kind of value',
        ),
      )

      await assert.rejects(
        () => client1!.call('BZMPOP', '-1', '1', zset, 'MIN'),
        errorWithMessage('ERR timeout is negative'),
      )
      await assert.rejects(
        () => client1!.call('BZMPOP', 'abc', '1', zset, 'MIN'),
        errorWithMessage('ERR timeout is not a float or out of range'),
      )

      const directClient = await connectToSlotOwner(client1!, zset)
      try {
        await assert.rejects(
          () => directClient.call('ZMPOP', '2', zset, 'other-slot-key', 'MIN'),
          errorWithMessage(
            "CROSSSLOT Keys in request don't hash to the same slot",
          ),
        )
        await assert.rejects(
          () =>
            directClient.call(
              'BZMPOP',
              '1',
              '2',
              zset,
              'other-slot-key',
              'MIN',
            ),
          errorWithMessage(
            "CROSSSLOT Keys in request don't hash to the same slot",
          ),
        )
      } finally {
        directClient.disconnect()
      }
    } finally {
      await client1!.del(zset, other, stringKey)
    }
  })
})
