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

  test('XGROUP creates, mutates, and destroys consumer groups', async () => {
    const key = randomKey()
    const node = await connectToSlotOwner(redisClient!, key)
    try {
      await node.xadd(key, '1-0', 'f', '1')
      await node.xadd(key, '2-0', 'f', '2')

      assert.strictEqual(
        await node.call('XGROUP', 'CREATE', key, 'workers', '0'),
        'OK',
      )
      assert.strictEqual(
        await node.call('XGROUP', 'CREATECONSUMER', key, 'workers', 'alice'),
        1,
      )
      assert.strictEqual(
        await node.call('XGROUP', 'CREATECONSUMER', key, 'workers', 'alice'),
        0,
      )

      const read = (await node.call(
        'XREADGROUP',
        'GROUP',
        'workers',
        'alice',
        'COUNT',
        '2',
        'STREAMS',
        key,
        '>',
      )) as [string, [string, string[]][]][]
      assert.deepStrictEqual(read, [
        [
          key,
          [
            ['1-0', ['f', '1']],
            ['2-0', ['f', '2']],
          ],
        ],
      ])

      const pendingSummary = (await node.call(
        'XPENDING',
        key,
        'workers',
      )) as unknown[]
      assert.strictEqual(pendingSummary[0], 2)
      assert.strictEqual(pendingSummary[1], '1-0')
      assert.strictEqual(pendingSummary[2], '2-0')
      assert.ok(Array.isArray(pendingSummary[3]))

      assert.strictEqual(await node.call('XACK', key, 'workers', '1-0'), 1)
      const pendingDetails = (await node.call(
        'XPENDING',
        key,
        'workers',
        '-',
        '+',
        '10',
      )) as unknown[][]
      assert.strictEqual(pendingDetails.length, 1)
      assert.strictEqual(pendingDetails[0][0], '2-0')
      assert.strictEqual(pendingDetails[0][1], 'alice')
      assert.strictEqual(pendingDetails[0][3], 1)

      const consumers = (await node.call(
        'XINFO',
        'CONSUMERS',
        key,
        'workers',
      )) as unknown[][]
      const alice = consumers.find(item => kvArrayGet(item, 'name') === 'alice')
      assert.ok(alice)
      assert.strictEqual(kvArrayGet(alice, 'pending'), 1)

      assert.strictEqual(
        await node.call('XGROUP', 'DELCONSUMER', key, 'workers', 'alice'),
        1,
      )
      assert.strictEqual(
        await node.call('XGROUP', 'DESTROY', key, 'workers'),
        1,
      )
      assert.strictEqual(
        await node.call('XGROUP', 'DESTROY', key, 'workers'),
        0,
      )
    } finally {
      node.disconnect()
    }
  })

  test('XGROUP MKSTREAM and SETID control group delivery position', async () => {
    const key = randomKey()
    const node = await connectToSlotOwner(redisClient!, key)
    try {
      assert.strictEqual(
        await node.call('XGROUP', 'CREATE', key, 'workers', '$', 'MKSTREAM'),
        'OK',
      )
      assert.strictEqual(await node.xlen(key), 0)

      await node.xadd(key, '1-0', 'f', '1')
      await node.xadd(key, '2-0', 'f', '2')
      assert.deepStrictEqual(
        await node.call(
          'XREADGROUP',
          'GROUP',
          'workers',
          'alice',
          'STREAMS',
          key,
          '>',
        ),
        [
          [
            key,
            [
              ['1-0', ['f', '1']],
              ['2-0', ['f', '2']],
            ],
          ],
        ],
      )

      assert.strictEqual(
        await node.call('XGROUP', 'SETID', key, 'workers', '$'),
        'OK',
      )
      await node.xadd(key, '3-0', 'f', '3')
      assert.deepStrictEqual(
        await node.call(
          'XREADGROUP',
          'GROUP',
          'workers',
          'bob',
          'STREAMS',
          key,
          '>',
        ),
        [[key, [['3-0', ['f', '3']]]]],
      )
    } finally {
      node.disconnect()
    }
  })

  test('XCLAIM and XAUTOCLAIM transfer pending stream entries', async () => {
    const key = randomKey()
    const node = await connectToSlotOwner(redisClient!, key)
    try {
      await node.xadd(key, '1-0', 'f', '1')
      await node.xadd(key, '2-0', 'f', '2')
      await node.call('XGROUP', 'CREATE', key, 'workers', '0')
      await node.call(
        'XREADGROUP',
        'GROUP',
        'workers',
        'alice',
        'COUNT',
        '2',
        'STREAMS',
        key,
        '>',
      )

      assert.deepStrictEqual(
        await node.call('XCLAIM', key, 'workers', 'bob', '0', '1-0'),
        [['1-0', ['f', '1']]],
      )

      const claimed = (await node.call(
        'XAUTOCLAIM',
        key,
        'workers',
        'carol',
        '0',
        '0-0',
        'COUNT',
        '10',
      )) as [string, [string, string[]][], string[]]
      assert.strictEqual(claimed[0], '0-0')
      assert.deepStrictEqual(
        claimed[1].map(entry => entry[0]),
        ['1-0', '2-0'],
      )
      assert.deepStrictEqual(claimed[2], [])

      const pendingDetails = (await node.call(
        'XPENDING',
        key,
        'workers',
        '-',
        '+',
        '10',
      )) as unknown[][]
      assert.deepStrictEqual(
        pendingDetails.map(item => [item[0], item[1]]),
        [
          ['1-0', 'carol'],
          ['2-0', 'carol'],
        ],
      )
    } finally {
      node.disconnect()
    }
  })

  test('XREADGROUP history keeps deleted pending entries visible', async () => {
    const key = randomKey()
    const node = await connectToSlotOwner(redisClient!, key)
    try {
      await node.xadd(key, '1-1', 'f', '1')
      await node.xadd(key, '2-2', 'f', '2')
      await node.call('XGROUP', 'CREATE', key, 'workers', '0')
      await node.call(
        'XREADGROUP',
        'GROUP',
        'workers',
        'alice',
        'COUNT',
        '10',
        'STREAMS',
        key,
        '>',
      )
      assert.strictEqual(await node.xdel(key, '1-1'), 1)

      assert.deepStrictEqual(
        await node.call(
          'XREADGROUP',
          'GROUP',
          'workers',
          'alice',
          'STREAMS',
          key,
          '0',
        ),
        [
          [
            key,
            [
              ['1-1', null],
              ['2-2', ['f', '2']],
            ],
          ],
        ],
      )
    } finally {
      node.disconnect()
    }
  })

  test('XREADGROUP history returns an empty per-key list for consumers with no pending entries', async () => {
    const key = randomKey()
    const node = await connectToSlotOwner(redisClient!, key)
    try {
      await node.xadd(key, '1-1', 'f', '1')
      await node.call('XGROUP', 'CREATE', key, 'workers', '0')

      assert.deepStrictEqual(
        await node.call(
          'XREADGROUP',
          'GROUP',
          'workers',
          'alice',
          'STREAMS',
          key,
          '0',
        ),
        [[key, []]],
      )
    } finally {
      node.disconnect()
    }
  })

  test('XINFO reports stream, group, and consumer metadata', async () => {
    const key = randomKey()
    const node = await connectToSlotOwner(redisClient!, key)
    try {
      await node.xadd(key, '1-0', 'f', '1')
      await node.xadd(key, '2-0', 'f', '2')
      await node.call('XGROUP', 'CREATE', key, 'workers', '0')
      await node.call(
        'XREADGROUP',
        'GROUP',
        'workers',
        'alice',
        'COUNT',
        '1',
        'STREAMS',
        key,
        '>',
      )

      const streamInfo = (await node.call(
        'XINFO',
        'STREAM',
        key,
        'FULL',
        'COUNT',
        '1',
      )) as unknown[]
      assert.strictEqual(kvArrayGet(streamInfo, 'length'), 2)
      assert.strictEqual(kvArrayGet(streamInfo, 'last-generated-id'), '2-0')
      assert.deepStrictEqual(kvArrayGet(streamInfo, 'entries'), [
        ['1-0', ['f', '1']],
      ])
      assert.ok(Array.isArray(kvArrayGet(streamInfo, 'groups')))

      const groupsInfo = (await node.call(
        'XINFO',
        'GROUPS',
        key,
      )) as unknown[][]
      assert.strictEqual(groupsInfo.length, 1)
      assert.strictEqual(kvArrayGet(groupsInfo[0], 'name'), 'workers')
      assert.strictEqual(kvArrayGet(groupsInfo[0], 'consumers'), 1)
      assert.strictEqual(kvArrayGet(groupsInfo[0], 'pending'), 1)

      const consumersInfo = (await node.call(
        'XINFO',
        'CONSUMERS',
        key,
        'workers',
      )) as unknown[][]
      assert.strictEqual(consumersInfo.length, 1)
      assert.strictEqual(kvArrayGet(consumersInfo[0], 'name'), 'alice')
      assert.strictEqual(kvArrayGet(consumersInfo[0], 'pending'), 1)
    } finally {
      node.disconnect()
    }
  })

  test('XINFO STREAM FULL defaults to 10 stream entries and PEL rows', async () => {
    const key = randomKey()
    const node = await connectToSlotOwner(redisClient!, key)
    try {
      for (let i = 1; i <= 12; i++) {
        await node.xadd(key, `${i}-0`, 'f', `${i}`)
      }
      await node.call('XGROUP', 'CREATE', key, 'workers', '0')
      await node.call(
        'XREADGROUP',
        'GROUP',
        'workers',
        'alice',
        'COUNT',
        '12',
        'STREAMS',
        key,
        '>',
      )

      const streamInfo = (await node.call(
        'XINFO',
        'STREAM',
        key,
        'FULL',
      )) as unknown[]
      const entries = kvArrayGet(streamInfo, 'entries') as unknown[][]
      assert.strictEqual(entries.length, 10)
      assert.strictEqual(entries[0][0], '1-0')
      assert.strictEqual(entries[9][0], '10-0')

      const groups = kvArrayGet(streamInfo, 'groups') as unknown[][]
      const pending = kvArrayGet(groups[0], 'pending') as unknown[][]
      assert.strictEqual(pending.length, 10)
      assert.strictEqual(pending[0][0], '1-0')
      assert.strictEqual(pending[9][0], '10-0')
    } finally {
      node.disconnect()
    }
  })

  test('stream consumer group commands report Redis-compatible errors', async () => {
    const tag = randomKey()
    const key = `{${tag}}:stream`
    const node = await connectToSlotOwner(redisClient!, key)
    try {
      await assert.rejects(
        () => node.call('XGROUP', 'CREATE', key, 'workers', '0'),
        errorWithMessage(
          'ERR The XGROUP subcommand requires the key to exist. Note that for CREATE you may want to use the MKSTREAM option to create an empty stream automatically.',
        ),
      )

      await node.xadd(key, '1-0', 'f', '1')
      assert.strictEqual(
        await node.call('XGROUP', 'CREATE', key, 'workers', '0'),
        'OK',
      )
      await assert.rejects(
        () => node.call('XGROUP', 'CREATE', key, 'workers', '0'),
        errorWithMessage('BUSYGROUP Consumer Group name already exists'),
      )
      await assert.rejects(
        () =>
          node.call(
            'XREADGROUP',
            'GROUP',
            'missing',
            'alice',
            'STREAMS',
            key,
            '>',
          ),
        errorWithMessage(
          `NOGROUP No such key '${key}' or consumer group 'missing' in XREADGROUP with GROUP option`,
        ),
      )

      const stringKey = `{${tag}}:string`
      await node.set(stringKey, 'value')
      await assert.rejects(
        () => node.call('XINFO', 'GROUPS', stringKey),
        errorWithMessage(
          'WRONGTYPE Operation against a key holding the wrong kind of value',
        ),
      )
    } finally {
      node.disconnect()
    }
  })
})
