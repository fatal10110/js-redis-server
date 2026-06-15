import { test, describe, before, after } from 'node:test'
import assert from 'node:assert'
import { Cluster } from 'ioredis'
import { TestRunner } from '../test-config'
import { connectToSlotOwner, errorWithMessage, randomKey } from '../utils'

const testRunner = new TestRunner()

function waitForPark(ms = 80): Promise<void> {
  return new Promise(resolve => setTimeout(resolve, ms))
}

describe(`LMPOP / BLMPOP Integration (${testRunner.getBackendName()})`, () => {
  let client1: Cluster | undefined
  let client2: Cluster | undefined

  before(async () => {
    client1 = await testRunner.setupIoredisCluster()
    client2 = await testRunner.setupIoredisCluster()
  })

  after(async () => {
    await testRunner.cleanup()
  })

  test('LMPOP pops from the first non-empty key with the requested side and count', async () => {
    const tag = `{${randomKey()}}`
    const empty = `${tag}:empty`
    const first = `${tag}:first`
    const second = `${tag}:second`
    await client1!.del(empty, first, second)
    await client1!.rpush(first, 'a', 'b', 'c')
    await client1!.rpush(second, 'x', 'y')

    assert.deepStrictEqual(
      await client1!.call(
        'LMPOP',
        '3',
        empty,
        first,
        second,
        'LEFT',
        'COUNT',
        '2',
      ),
      [first, ['a', 'b']],
    )
    assert.deepStrictEqual(await client1!.lrange(first, 0, -1), ['c'])
    assert.deepStrictEqual(await client1!.lrange(second, 0, -1), ['x', 'y'])

    assert.deepStrictEqual(
      await client1!.call('LMPOP', '2', first, second, 'RIGHT'),
      [first, ['c']],
    )
    assert.strictEqual(await client1!.exists(first), 0)

    assert.deepStrictEqual(
      await client1!.call('LMPOP', '2', first, second, 'RIGHT', 'COUNT', '5'),
      [second, ['y', 'x']],
    )
    assert.strictEqual(await client1!.exists(second), 0)
  })

  test('LMPOP returns null when all keys are empty or missing', async () => {
    const tag = `{${randomKey()}}`
    const first = `${tag}:first`
    const second = `${tag}:second`
    await client1!.del(first, second)

    assert.strictEqual(
      await client1!.call('LMPOP', '2', first, second, 'LEFT'),
      null,
    )
  })

  test('BLMPOP returns immediately when a list is non-empty', async () => {
    const tag = `{${randomKey()}}`
    const empty = `${tag}:empty`
    const list = `${tag}:list`
    await client1!.del(empty, list)
    await client1!.rpush(list, 'one', 'two', 'three')

    assert.deepStrictEqual(
      await client1!.call(
        'BLMPOP',
        '1',
        '2',
        empty,
        list,
        'RIGHT',
        'COUNT',
        '2',
      ),
      [list, ['three', 'two']],
    )
    assert.deepStrictEqual(await client1!.lrange(list, 0, -1), ['one'])
  })

  test('BLMPOP blocks then returns when a push arrives', async () => {
    const tag = `{${randomKey()}}`
    const first = `${tag}:first`
    const second = `${tag}:second`
    await client1!.del(first, second)

    const blockPromise = client1!.call(
      'BLMPOP',
      '5',
      '2',
      first,
      second,
      'LEFT',
      'COUNT',
      '2',
    )
    await waitForPark()
    await client2!.rpush(second, 'a', 'b')

    assert.deepStrictEqual(await blockPromise, [second, ['a', 'b']])
    assert.strictEqual(await client1!.exists(second), 0)
  })

  test('BLMPOP timeout returns null', async () => {
    const tag = `{${randomKey()}}`
    const first = `${tag}:first`
    const second = `${tag}:second`
    await client1!.del(first, second)

    assert.strictEqual(
      await client1!.call('BLMPOP', '0.1', '2', first, second, 'LEFT'),
      null,
    )
  })

  test('LMPOP / BLMPOP error paths match Redis', async () => {
    const tag = `{${randomKey()}}`
    const list = `${tag}:list`
    const other = `${tag}:other`
    const stringKey = `${tag}:string`
    await client1!.del(list, other, stringKey)
    await client1!.rpush(list, 'value')
    await client1!.set(stringKey, 'not-a-list')

    await assert.rejects(
      () => client1!.call('LMPOP'),
      errorWithMessage("ERR wrong number of arguments for 'lmpop' command"),
    )
    await assert.rejects(
      () => client1!.call('LMPOP', '0', list, 'LEFT'),
      errorWithMessage('ERR numkeys should be greater than 0'),
    )
    await assert.rejects(
      () => client1!.call('LMPOP', 'abc', list, 'LEFT'),
      errorWithMessage('ERR numkeys should be greater than 0'),
    )
    await assert.rejects(
      () => client1!.call('LMPOP', '2', list, other),
      errorWithMessage('ERR syntax error'),
    )
    await assert.rejects(
      () => client1!.call('LMPOP', '1', list, 'MIDDLE'),
      errorWithMessage('ERR syntax error'),
    )
    await assert.rejects(
      () => client1!.call('LMPOP', '1', list, 'LEFT', 'LIMIT', '1'),
      errorWithMessage('ERR syntax error'),
    )
    await assert.rejects(
      () => client1!.call('LMPOP', '1', list, 'LEFT', 'COUNT'),
      errorWithMessage('ERR syntax error'),
    )
    await assert.rejects(
      () => client1!.call('LMPOP', '1', list, 'LEFT', 'COUNT', '0'),
      errorWithMessage('ERR count should be greater than 0'),
    )
    await assert.rejects(
      () => client1!.call('LMPOP', '1', list, 'LEFT', 'COUNT', '-1'),
      errorWithMessage('ERR count should be greater than 0'),
    )
    await assert.rejects(
      () => client1!.call('LMPOP', '1', list, 'LEFT', 'COUNT', 'abc'),
      errorWithMessage('ERR count should be greater than 0'),
    )
    await assert.rejects(
      () => client1!.call('LMPOP', '1', stringKey, 'LEFT'),
      errorWithMessage(
        'WRONGTYPE Operation against a key holding the wrong kind of value',
      ),
    )

    await assert.rejects(
      () => client1!.call('BLMPOP', '-1', '1', list, 'LEFT'),
      errorWithMessage('ERR timeout is negative'),
    )
    await assert.rejects(
      () => client1!.call('BLMPOP', 'abc', '1', list, 'LEFT'),
      errorWithMessage('ERR timeout is not a float or out of range'),
    )

    const directClient = await connectToSlotOwner(client1!, list)
    try {
      await assert.rejects(
        () => directClient.call('LMPOP', '2', list, 'other-slot-key', 'LEFT'),
        errorWithMessage(
          "CROSSSLOT Keys in request don't hash to the same slot",
        ),
      )
      await assert.rejects(
        () =>
          directClient.call('BLMPOP', '1', '2', list, 'other-slot-key', 'LEFT'),
        errorWithMessage(
          "CROSSSLOT Keys in request don't hash to the same slot",
        ),
      )
    } finally {
      directClient.disconnect()
    }

    await client1!.del(list, other, stringKey)
  })
})
