import { after, before, describe, test } from 'node:test'
import { TestRunner } from '../test-config'
import { randomKey } from '../utils'
import { RawRedisConnection } from './raw-connection'
import { connectToRawSlotOwner, expectReply, send } from './helpers'

/**
 * Stream-metadata WATCH semantics over bare sockets on a cluster node.
 *
 * Real Redis does NOT invalidate a WATCH on a stream key for consumer-group /
 * pending-entry / last-id metadata changes — only changes to the stream's entry
 * set (XADD, XDEL, ...) touch the watched key. These pin that behavior over the
 * wire (raw WATCH/MULTI/EXEC the client's transaction API can't drive). Moved
 * out of `ioredis/stream/watch.test.ts`, where they used `.call(...)`.
 */
const testRunner = new TestRunner()

type Case = { name: string; seed: string[][]; mutate: string[] }

// Metadata mutations that must leave the WATCH intact: the queued SET still
// runs, so EXEC returns the one-element OK array.
const cleanCases: Case[] = [
  {
    name: 'XSETID to a different id',
    seed: [['XADD', 'K', '5-5', 'f', 'v']],
    mutate: ['XSETID', 'K', '9-9'],
  },
  {
    name: 'XGROUP SETID to a different id',
    seed: [
      ['XADD', 'K', '1-1', 'f', 'v'],
      ['XADD', 'K', '2-2', 'f', 'v'],
      ['XGROUP', 'CREATE', 'K', 'g', '0'],
    ],
    mutate: ['XGROUP', 'SETID', 'K', 'g', '2'],
  },
  {
    name: 'XREADGROUP > delivering new entries',
    seed: [
      ['XADD', 'K', '1-1', 'f', 'v'],
      ['XGROUP', 'CREATE', 'K', 'g', '0'],
    ],
    mutate: [
      'XREADGROUP',
      'GROUP',
      'g',
      'c',
      'COUNT',
      '10',
      'STREAMS',
      'K',
      '>',
    ],
  },
  {
    name: 'XREADGROUP history read (explicit id)',
    seed: [
      ['XADD', 'K', '1-1', 'f', 'v'],
      ['XGROUP', 'CREATE', 'K', 'g', '0'],
      ['XREADGROUP', 'GROUP', 'g', 'c', 'STREAMS', 'K', '>'],
    ],
    mutate: ['XREADGROUP', 'GROUP', 'g', 'c', 'STREAMS', 'K', '0'],
  },
  {
    name: 'XREADGROUP > with no new entries',
    seed: [
      ['XADD', 'K', '1-1', 'f', 'v'],
      ['XGROUP', 'CREATE', 'K', 'g', '$'],
    ],
    mutate: ['XREADGROUP', 'GROUP', 'g', 'c', 'STREAMS', 'K', '>'],
  },
  {
    name: 'XCLAIM that claims a pending entry',
    seed: [
      ['XADD', 'K', '1-1', 'f', 'v'],
      ['XGROUP', 'CREATE', 'K', 'g', '0'],
      ['XREADGROUP', 'GROUP', 'g', 'c1', 'STREAMS', 'K', '>'],
    ],
    mutate: ['XCLAIM', 'K', 'g', 'c2', '0', '1-1'],
  },
  {
    name: 'XCLAIM that claims nothing',
    seed: [
      ['XADD', 'K', '1-1', 'f', 'v'],
      ['XGROUP', 'CREATE', 'K', 'g', '0'],
    ],
    mutate: ['XCLAIM', 'K', 'g', 'c2', '0', '9-9'],
  },
  {
    name: 'XAUTOCLAIM that claims a pending entry',
    seed: [
      ['XADD', 'K', '1-1', 'f', 'v'],
      ['XGROUP', 'CREATE', 'K', 'g', '0'],
      ['XREADGROUP', 'GROUP', 'g', 'c1', 'STREAMS', 'K', '>'],
    ],
    mutate: ['XAUTOCLAIM', 'K', 'g', 'c2', '0', '0'],
  },
  {
    name: 'XAUTOCLAIM that claims nothing',
    seed: [
      ['XADD', 'K', '1-1', 'f', 'v'],
      ['XGROUP', 'CREATE', 'K', 'g', '0'],
    ],
    mutate: ['XAUTOCLAIM', 'K', 'g', 'c2', '0', '0'],
  },
  {
    name: 'XACK acknowledging a pending entry',
    seed: [
      ['XADD', 'K', '1-1', 'f', 'v'],
      ['XGROUP', 'CREATE', 'K', 'g', '0'],
      ['XREADGROUP', 'GROUP', 'g', 'c', 'STREAMS', 'K', '>'],
    ],
    mutate: ['XACK', 'K', 'g', '1-1'],
  },
  {
    name: 'XGROUP CREATE on an existing stream',
    seed: [['XADD', 'K', '1-1', 'f', 'v']],
    mutate: ['XGROUP', 'CREATE', 'K', 'g', '0'],
  },
  {
    name: 'XGROUP DESTROY removing a group',
    seed: [
      ['XADD', 'K', '1-1', 'f', 'v'],
      ['XGROUP', 'CREATE', 'K', 'g', '0'],
    ],
    mutate: ['XGROUP', 'DESTROY', 'K', 'g'],
  },
  {
    name: 'XGROUP CREATECONSUMER creating a consumer',
    seed: [
      ['XADD', 'K', '1-1', 'f', 'v'],
      ['XGROUP', 'CREATE', 'K', 'g', '0'],
    ],
    mutate: ['XGROUP', 'CREATECONSUMER', 'K', 'g', 'c'],
  },
  {
    name: 'XGROUP DELCONSUMER removing a consumer',
    seed: [
      ['XADD', 'K', '1-1', 'f', 'v'],
      ['XGROUP', 'CREATE', 'K', 'g', '0'],
      ['XGROUP', 'CREATECONSUMER', 'K', 'g', 'c'],
    ],
    mutate: ['XGROUP', 'DELCONSUMER', 'K', 'g', 'c'],
  },
]

describe(`Raw TCP cluster stream WATCH semantics (${testRunner.getBackendName()})`, () => {
  let ports: number[]
  const connections: RawRedisConnection[] = []

  before(async () => {
    ports = await testRunner.setupRawCluster()
  })

  after(async () => {
    for (const connection of connections) {
      connection.close()
    }
    connections.length = 0
    await testRunner.cleanup()
  })

  async function ownerOf(key: string): Promise<RawRedisConnection> {
    const conn = await connectToRawSlotOwner(ports, key)
    connections.push(conn)
    return conn
  }

  for (const item of cleanCases) {
    test(`${item.name} does not dirty a watched stream`, async () => {
      const key = `stream:{watch:${randomKey()}}`
      const conn = await ownerOf(key)
      const sub = (args: string[]): string[] =>
        args.map(a => (a === 'K' ? key : a))

      for (const seed of item.seed) await send(conn, sub(seed))

      await expectReply(conn, ['WATCH', key], '+OK\r\n')
      await send(conn, sub(item.mutate))

      await expectReply(conn, ['MULTI'], '+OK\r\n')
      await expectReply(conn, ['SET', key, 'after'], '+QUEUED\r\n')
      await expectReply(conn, ['EXEC'], '*1\r\n+OK\r\n')
    })
  }

  // Creating the watched key itself is a write, even via XGROUP CREATE MKSTREAM:
  // real Redis dirties a WATCH on a key that comes into existence.
  test('XGROUP CREATE MKSTREAM creating the watched key dirties it', async () => {
    const key = `stream:{watch:${randomKey()}}`
    const conn = await ownerOf(key)

    await expectReply(conn, ['WATCH', key], '+OK\r\n')
    await expectReply(
      conn,
      ['XGROUP', 'CREATE', key, 'g', '0', 'MKSTREAM'],
      '+OK\r\n',
    )

    await expectReply(conn, ['MULTI'], '+OK\r\n')
    await expectReply(conn, ['SET', key, 'after'], '+QUEUED\r\n')
    await expectReply(conn, ['EXEC'], '*-1\r\n')
  })

  test('XADD still dirties a watched stream', async () => {
    const key = `stream:{watch:${randomKey()}}`
    const conn = await ownerOf(key)

    await send(conn, ['XADD', key, '1-1', 'f', 'v'])
    await expectReply(conn, ['WATCH', key], '+OK\r\n')
    await send(conn, ['XADD', key, '2-2', 'f', 'v'])

    await expectReply(conn, ['MULTI'], '+OK\r\n')
    await expectReply(conn, ['SET', key, 'after'], '+QUEUED\r\n')
    await expectReply(conn, ['EXEC'], '*-1\r\n')
  })
})
