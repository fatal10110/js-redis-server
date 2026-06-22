import { test, describe, before, after } from 'node:test'
import assert from 'node:assert'
import { Cluster } from 'ioredis'
import { TestRunner } from '../../test-config'
import { connectToSlotOwner, errorWithMessage, randomKey } from '../../utils'

const testRunner = new TestRunner()

function kvArrayGet(items: unknown[], key: string): unknown {
  const index = items.indexOf(key)
  assert.notStrictEqual(index, -1, `expected ${key} field`)
  return items[index + 1]
}

describe(`Stream Commands Integration (${testRunner.getBackendName()})`, () => {
  let redisClient: Cluster | undefined

  before(async () => {
    redisClient = await testRunner.setupIoredisCluster('stream-integration')
  })

  after(async () => {
    await testRunner.cleanup()
  })

  // XTRIM

  // XADD with NOMKSTREAM and MAXLEN options

  // XREAD

  // Stream consumer groups

  test('XSETID sets last-generated-id and advances generated XADD ids', async () => {
    const key = `{xsetid:${randomKey()}}`
    const node = await connectToSlotOwner(redisClient!, key)
    const futureMs = BigInt(Date.now()) + 60_000n

    try {
      await node.xadd(key, '1-0', 'f', 'v')
      assert.strictEqual(await node.xsetid(key, `${futureMs}-0`), 'OK')

      const streamInfo = (await node.xinfo('STREAM', key)) as unknown[]
      assert.strictEqual(kvArrayGet(streamInfo, 'length'), 1)
      assert.strictEqual(
        kvArrayGet(streamInfo, 'last-generated-id'),
        `${futureMs}-0`,
      )
      assert.strictEqual(kvArrayGet(streamInfo, 'entries-added'), 1)

      assert.strictEqual(await node.xadd(key, '*', 'f', 'v'), `${futureMs}-1`)
    } finally {
      await node.del(key)
      node.disconnect()
    }
  })

  test('XSETID ENTRIESADDED updates XINFO STREAM metadata', async () => {
    const key = `{xsetid-meta:${randomKey()}}`
    const node = await connectToSlotOwner(redisClient!, key)

    try {
      await node.xadd(key, '1-0', 'f', 'v')
      assert.strictEqual(
        await node.xsetid(key, '5-0', 'entriesadded', '42'),
        'OK',
      )

      const streamInfo = (await node.xinfo('STREAM', key)) as unknown[]
      assert.strictEqual(kvArrayGet(streamInfo, 'last-generated-id'), '5-0')
      assert.strictEqual(kvArrayGet(streamInfo, 'entries-added'), 42)
      assert.strictEqual(await node.xadd(key, '5-*', 'f', 'next'), '5-1')

      const updatedInfo = (await node.xinfo('STREAM', key)) as unknown[]
      assert.strictEqual(kvArrayGet(updatedInfo, 'entries-added'), 43)
    } finally {
      await node.del(key)
      node.disconnect()
    }
  })

  test('XSETID MAXDELETEDID updates XINFO STREAM metadata', async () => {
    const key = `{xsetid-maxdeleted:${randomKey()}}`
    const node = await connectToSlotOwner(redisClient!, key)

    try {
      await node.xadd(key, '1-0', 'f', 'v')
      assert.strictEqual(
        await node.xsetid(
          key,
          '5-0',
          'maxdeletedid',
          '2-0',
          'ENTRIESADDED',
          '42',
        ),
        'OK',
      )

      const streamInfo = (await node.xinfo('STREAM', key)) as unknown[]
      assert.strictEqual(kvArrayGet(streamInfo, 'last-generated-id'), '5-0')
      assert.strictEqual(kvArrayGet(streamInfo, 'max-deleted-entry-id'), '2-0')
      assert.strictEqual(kvArrayGet(streamInfo, 'entries-added'), 42)

      assert.strictEqual(
        await node.xsetid(
          key,
          '6-0',
          'ENTRIESADDED',
          '7',
          'MAXDELETEDID',
          '3-0',
          'ENTRIESADDED',
          '9',
          'MAXDELETEDID',
          '4-0',
        ),
        'OK',
      )

      const duplicateInfo = (await node.xinfo('STREAM', key)) as unknown[]
      assert.strictEqual(kvArrayGet(duplicateInfo, 'last-generated-id'), '6-0')
      assert.strictEqual(
        kvArrayGet(duplicateInfo, 'max-deleted-entry-id'),
        '4-0',
      )
      assert.strictEqual(kvArrayGet(duplicateInfo, 'entries-added'), 9)
    } finally {
      await node.del(key)
      node.disconnect()
    }
  })

  test('XSETID rejects lower ids, invalid options, and wrong types', async () => {
    const tag = `{xsetid-errors:${randomKey()}}`
    const key = `${tag}:stream`
    const stringKey = `${tag}:string`
    const node = await connectToSlotOwner(redisClient!, key)

    try {
      await node.xadd(key, '5-0', 'f', 'v')
      await node.set(stringKey, 'not-a-stream')

      await assert.rejects(
        () => node.xsetid(`${tag}:missing`, '1-0'),
        errorWithMessage('ERR no such key'),
      )
      await assert.rejects(
        () => node.xsetid(key, '4-0'),
        errorWithMessage(
          'ERR The ID specified in XSETID is smaller than the target stream top item',
        ),
      )
      await assert.rejects(
        () => node.xsetid(key, 'not-an-id'),
        errorWithMessage(
          'ERR Invalid stream ID specified as stream command argument',
        ),
      )
      await assert.rejects(
        () => node.xsetid(key, '6-0', 'ENTRIESADDED', 'nope'),
        errorWithMessage('ERR value is not an integer or out of range'),
      )
      await assert.rejects(
        () => node.xsetid(key, '6-0', 'ENTRIESADDED'),
        errorWithMessage('ERR syntax error'),
      )
      await assert.rejects(
        () => node.xsetid(key, '6-0', 'MAXDELETEDID', 'bad-id'),
        errorWithMessage(
          'ERR Invalid stream ID specified as stream command argument',
        ),
      )
      await assert.rejects(
        () => node.xsetid(key, '6-0', 'MAXDELETEDID'),
        errorWithMessage('ERR syntax error'),
      )
      await assert.rejects(
        () => node.xsetid(key, '6-0', 'BOGUS', '1'),
        errorWithMessage('ERR syntax error'),
      )
      await assert.rejects(
        () => node.xsetid(stringKey, '6-0'),
        errorWithMessage(
          'WRONGTYPE Operation against a key holding the wrong kind of value',
        ),
      )
    } finally {
      await node.del(key, stringKey)
      node.disconnect()
    }
  })
})
