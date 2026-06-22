import { test, describe, before, after } from 'node:test'
import assert from 'node:assert'
import { RedisClusterType } from 'redis'
import { TestRunner } from '../../test-config'
import {
  connectToNodeRedisSlotOwner,
  errorWithMessage,
  flushNodeRedisCluster,
  randomKey,
} from '../../utils'

const testRunner = new TestRunner()

function waitForPark(ms = 80): Promise<void> {
  return new Promise(resolve => setTimeout(resolve, ms))
}

describe(`LMPOP / BLMPOP Integration (node-redis, ${testRunner.getBackendName()})`, () => {
  let client1: RedisClusterType
  let client2: RedisClusterType

  before(async () => {
    client1 = (await testRunner.setupNodeRedisCluster()) as RedisClusterType
    client2 = (await testRunner.setupNodeRedisCluster()) as RedisClusterType
    await flushNodeRedisCluster(client1)
  })

  after(async () => {
    await testRunner.cleanup()
  })

  test('LMPOP pops from the first non-empty key with the requested side and count', async () => {
    const tag = `{${randomKey()}}`
    const empty = `${tag}:empty`
    const first = `${tag}:first`
    const second = `${tag}:second`
    await client1.del([empty, first, second])
    await client1.rPush(first, ['a', 'b', 'c'])
    await client1.rPush(second, ['x', 'y'])

    assert.deepStrictEqual(
      await client1.sendCommand(first, false, [
        'LMPOP',
        '3',
        empty,
        first,
        second,
        'LEFT',
        'COUNT',
        '2',
      ]),
      [first, ['a', 'b']],
    )
    assert.deepStrictEqual(await client1.lRange(first, 0, -1), ['c'])
    assert.deepStrictEqual(await client1.lRange(second, 0, -1), ['x', 'y'])

    assert.deepStrictEqual(
      await client1.sendCommand(first, false, [
        'LMPOP',
        '2',
        first,
        second,
        'RIGHT',
      ]),
      [first, ['c']],
    )
    assert.strictEqual(await client1.exists(first), 0)

    assert.deepStrictEqual(
      await client1.sendCommand(first, false, [
        'LMPOP',
        '2',
        first,
        second,
        'RIGHT',
        'COUNT',
        '5',
      ]),
      [second, ['y', 'x']],
    )
    assert.strictEqual(await client1.exists(second), 0)
  })

  test('LMPOP returns null when all keys are empty or missing', async () => {
    const tag = `{${randomKey()}}`
    const first = `${tag}:first`
    const second = `${tag}:second`
    await client1.del([first, second])

    assert.strictEqual(
      await client1.sendCommand(first, false, [
        'LMPOP',
        '2',
        first,
        second,
        'LEFT',
      ]),
      null,
    )
  })

  test('BLMPOP returns immediately when a list is non-empty', async () => {
    const tag = `{${randomKey()}}`
    const empty = `${tag}:empty`
    const list = `${tag}:list`
    await client1.del([empty, list])
    await client1.rPush(list, ['one', 'two', 'three'])

    assert.deepStrictEqual(
      await client1.sendCommand(list, false, [
        'BLMPOP',
        '1',
        '2',
        empty,
        list,
        'RIGHT',
        'COUNT',
        '2',
      ]),
      [list, ['three', 'two']],
    )
    assert.deepStrictEqual(await client1.lRange(list, 0, -1), ['one'])
  })

  test('BLMPOP blocks then returns when a push arrives', async () => {
    const tag = `{${randomKey()}}`
    const first = `${tag}:first`
    const second = `${tag}:second`
    await client1.del([first, second])

    const blockPromise = client1.sendCommand(first, false, [
      'BLMPOP',
      '5',
      '2',
      first,
      second,
      'LEFT',
      'COUNT',
      '2',
    ])
    await waitForPark()
    await client2.rPush(second, ['a', 'b'])

    assert.deepStrictEqual(await blockPromise, [second, ['a', 'b']])
    assert.strictEqual(await client1.exists(second), 0)
  })

  test('BLMPOP timeout returns null', async () => {
    const tag = `{${randomKey()}}`
    const first = `${tag}:first`
    const second = `${tag}:second`
    await client1.del([first, second])

    assert.strictEqual(
      await client1.sendCommand(first, false, [
        'BLMPOP',
        '0.1',
        '2',
        first,
        second,
        'LEFT',
      ]),
      null,
    )
  })

  test('LMPOP / BLMPOP error paths match Redis', async () => {
    const tag = `{${randomKey()}}`
    const list = `${tag}:list`
    const other = `${tag}:other`
    const stringKey = `${tag}:string`
    await client1.del([list, other, stringKey])
    await client1.rPush(list, 'value')
    await client1.set(stringKey, 'not-a-list')

    await assert.rejects(
      () => client1.sendCommand(undefined, false, ['LMPOP']),
      errorWithMessage("ERR wrong number of arguments for 'lmpop' command"),
    )
    await assert.rejects(
      () => client1.sendCommand(list, false, ['LMPOP', '0', list, 'LEFT']),
      errorWithMessage('ERR numkeys should be greater than 0'),
    )
    await assert.rejects(
      () => client1.sendCommand(list, false, ['LMPOP', 'abc', list, 'LEFT']),
      errorWithMessage('ERR numkeys should be greater than 0'),
    )
    await assert.rejects(
      () => client1.sendCommand(list, false, ['LMPOP', '2', list, other]),
      errorWithMessage('ERR syntax error'),
    )
    await assert.rejects(
      () => client1.sendCommand(list, false, ['LMPOP', '1', list, 'MIDDLE']),
      errorWithMessage('ERR syntax error'),
    )
    await assert.rejects(
      () =>
        client1.sendCommand(list, false, [
          'LMPOP',
          '1',
          list,
          'LEFT',
          'LIMIT',
          '1',
        ]),
      errorWithMessage('ERR syntax error'),
    )
    await assert.rejects(
      () =>
        client1.sendCommand(list, false, ['LMPOP', '1', list, 'LEFT', 'COUNT']),
      errorWithMessage('ERR syntax error'),
    )
    await assert.rejects(
      () =>
        client1.sendCommand(list, false, [
          'LMPOP',
          '1',
          list,
          'LEFT',
          'COUNT',
          '0',
        ]),
      errorWithMessage('ERR count should be greater than 0'),
    )
    await assert.rejects(
      () =>
        client1.sendCommand(list, false, [
          'LMPOP',
          '1',
          list,
          'LEFT',
          'COUNT',
          '-1',
        ]),
      errorWithMessage('ERR count should be greater than 0'),
    )
    await assert.rejects(
      () =>
        client1.sendCommand(list, false, [
          'LMPOP',
          '1',
          list,
          'LEFT',
          'COUNT',
          'abc',
        ]),
      errorWithMessage('ERR count should be greater than 0'),
    )
    await assert.rejects(
      () =>
        client1.sendCommand(stringKey, false, [
          'LMPOP',
          '1',
          stringKey,
          'LEFT',
        ]),
      errorWithMessage(
        'WRONGTYPE Operation against a key holding the wrong kind of value',
      ),
    )

    await assert.rejects(
      () =>
        client1.sendCommand(list, false, ['BLMPOP', '-1', '1', list, 'LEFT']),
      errorWithMessage('ERR timeout is negative'),
    )
    await assert.rejects(
      () =>
        client1.sendCommand(list, false, ['BLMPOP', 'abc', '1', list, 'LEFT']),
      errorWithMessage('ERR timeout is not a float or out of range'),
    )

    const directClient = await connectToNodeRedisSlotOwner(client1, list)
    try {
      await assert.rejects(
        () =>
          directClient.sendCommand([
            'LMPOP',
            '2',
            list,
            'other-slot-key',
            'LEFT',
          ]),
        errorWithMessage(
          "CROSSSLOT Keys in request don't hash to the same slot",
        ),
      )
      await assert.rejects(
        () =>
          directClient.sendCommand([
            'BLMPOP',
            '1',
            '2',
            list,
            'other-slot-key',
            'LEFT',
          ]),
        errorWithMessage(
          "CROSSSLOT Keys in request don't hash to the same slot",
        ),
      )
    } finally {
      directClient.destroy()
    }

    await client1.del([list, other, stringKey])
  })
})
