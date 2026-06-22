import { after, before, describe, test } from 'node:test'
import { TestRunner } from '../test-config'
import { randomKey } from '../utils'
import { RawRedisConnection } from './raw-connection'
import { connectToRawSlotOwner, expectReply } from './helpers'

/**
 * Smoke tests for the raw-tcp cluster helper. These drive cluster-routed
 * behavior (slot-owner routing, raw MULTI/EXEC, CROSSSLOT) over a bare socket —
 * the things the standalone raw-tcp harness cannot express and that previously
 * forced `.call(...)` inside the ioredis suite.
 */
const testRunner = new TestRunner()

describe(`Raw TCP cluster helper (${testRunner.getBackendName()})`, () => {
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

  test('routes a keyed command to its slot owner without MOVED', async () => {
    const key = `rawcluster:${randomKey()}`
    const conn = await ownerOf(key)

    await expectReply(conn, ['SET', key, 'value'], '+OK\r\n')
    await expectReply(conn, ['GET', key], '$5\r\nvalue\r\n')
  })

  test('drives a raw MULTI/EXEC transaction on the slot owner', async () => {
    const key = `rawcluster-txn:${randomKey()}`
    const conn = await ownerOf(key)

    await expectReply(conn, ['MULTI'], '+OK\r\n')
    await expectReply(conn, ['SET', key, 'queued'], '+QUEUED\r\n')
    await expectReply(conn, ['GET', key], '+QUEUED\r\n')
    await expectReply(conn, ['EXEC'], '*2\r\n+OK\r\n$6\r\nqueued\r\n')
  })

  test('rejects a multi-slot command with CROSSSLOT', async () => {
    const conn = await ownerOf('{slot-a}')

    await expectReply(
      conn,
      ['MGET', '{slot-a}', '{slot-b}'],
      "-CROSSSLOT Keys in request don't hash to the same slot\r\n",
    )
  })
})
