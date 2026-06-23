import assert from 'node:assert'
import { after, before, describe, test } from 'node:test'
import { TestRunner } from '../test-config'
import { commandFrame, randomKey } from '../utils'
import { RawRedisConnection } from './raw-connection'
import { connectToRawSlotOwner, expectReply, send } from './helpers'

/**
 * RESP3 wire-encoding tests over a bare socket on a cluster node.
 *
 * node-redis connects with RESP3 by default, so the server must emit the RESP3
 * forms that differ from RESP2: null `_`, map `%`, double `,`. The RESP2
 * transaction/data behavior is covered elsewhere; this pins the RESP3 encoding
 * the client suite can no longer see once those tests use typed methods.
 */
const testRunner = new TestRunner()

describe(`Raw TCP RESP3 wire encoding (${testRunner.getBackendName()})`, () => {
  let ports: number[]
  const connections: RawRedisConnection[] = []

  before(async () => {
    ports = await testRunner.setupRawCluster()
  })

  after(async () => {
    for (const connection of connections) connection.close()
    connections.length = 0
    await testRunner.cleanup()
  })

  // Open a raw connection to `key`'s slot owner and switch it to RESP3.
  async function resp3Owner(key: string): Promise<RawRedisConnection> {
    const conn = await connectToRawSlotOwner(ports, key)
    connections.push(conn)
    conn.write(commandFrame('HELLO', '3'))
    const hello = await conn.readFrame()
    assert.ok(hello instanceof Map, 'HELLO 3 should reply with a RESP3 map')
    return conn
  }

  test('null replies use the RESP3 `_` form', async () => {
    const key = `resp3-null:${randomKey()}`
    const conn = await resp3Owner(key)
    // Missing key GET -> RESP3 null.
    await expectReply(conn, ['GET', key], '_\r\n')
  })

  test('dirtied EXEC returns a RESP3 null, not a RESP2 nil array', async () => {
    const key = `resp3-exec:${randomKey()}`
    const directConn = await resp3Owner(key)
    const mutatingConn = await resp3Owner(key)

    await expectReply(directConn, ['SET', key, 'init'], '+OK\r\n')
    await expectReply(directConn, ['WATCH', key], '+OK\r\n')
    await expectReply(mutatingConn, ['SET', key, 'modified'], '+OK\r\n')

    await expectReply(directConn, ['MULTI'], '+OK\r\n')
    await expectReply(directConn, ['SET', key, 'queued'], '+QUEUED\r\n')
    // RESP3 null, not RESP2 `*-1`.
    await expectReply(directConn, ['EXEC'], '_\r\n')
  })

  test('HGETALL returns a RESP3 `%` map', async () => {
    const key = `resp3-map:${randomKey()}`
    const conn = await resp3Owner(key)
    await send(conn, ['HSET', key, 'f1', 'v1', 'f2', 'v2'])
    await expectReply(
      conn,
      ['HGETALL', key],
      '%2\r\n$2\r\nf1\r\n$2\r\nv1\r\n$2\r\nf2\r\n$2\r\nv2\r\n',
    )
  })

  test('ZSCORE returns a RESP3 `,` double', async () => {
    const key = `resp3-double:${randomKey()}`
    const conn = await resp3Owner(key)
    await send(conn, ['ZADD', key, '3', 'm'])
    await expectReply(conn, ['ZSCORE', key, 'm'], ',3\r\n')
  })
})
