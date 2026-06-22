import { after, before, describe, test } from 'node:test'
import { TestRunner } from '../test-config'
import { randomKey } from '../utils'
import { RawRedisConnection } from './raw-connection'
import {
  connectToRawSlotOwner,
  expectReply,
  expectReplyPrefix,
} from './helpers'

/**
 * Transaction-precedence tests over bare sockets on a cluster node.
 *
 * Real Redis sets CLIENT_DIRTY_EXEC when a queued command fails (e.g. an
 * unknown command) and CLIENT_DIRTY_CAS when a WATCHed key is touched. EXEC
 * must prioritise DIRTY_EXEC: a bad queue returns -EXECABORT regardless of
 * WATCH state, and only a clean queue with a dirtied CAS returns the (nil)
 * abort. See issue #123.
 *
 * These drive raw, interleaved MULTI/EXEC/WATCH across two connections to the
 * same slot owner — something a client's transaction API can't express
 * (ioredis `.multi()` is a client-side pipeline) — so they belong over the
 * wire, not in the ioredis suite where they previously used `.call(...)`.
 */
const testRunner = new TestRunner()

const EXECABORT =
  '-EXECABORT Transaction discarded because of previous errors.\r\n'

describe(`Raw TCP cluster transactions (${testRunner.getBackendName()})`, () => {
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

  test('returns EXECABORT (not nil) when the queue is bad AND a watched key changed', async () => {
    const key = `execdirty:${randomKey()}`
    // Two connections to the same slot owner so WATCH/MULTI/EXEC stay on one
    // node and the mutation is seen by the watching session.
    const directConn = await ownerOf(key)
    const mutatingConn = await ownerOf(key)

    await expectReply(directConn, ['SET', key, 'init'], '+OK\r\n')
    await expectReply(directConn, ['WATCH', key], '+OK\r\n')

    // Dirty the WATCH (CLIENT_DIRTY_CAS) from the other connection.
    await expectReply(mutatingConn, ['SET', key, 'modified'], '+OK\r\n')

    await expectReply(directConn, ['MULTI'], '+OK\r\n')
    // Queue an unknown command -> dirties the transaction (CLIENT_DIRTY_EXEC).
    await expectReplyPrefix(
      directConn,
      ['NOTACOMMAND', 'x'],
      '-ERR unknown command',
    )

    // DIRTY_EXEC must win over DIRTY_CAS: EXECABORT, not a nil array.
    await expectReply(directConn, ['EXEC'], EXECABORT)
  })

  test('returns EXECABORT when the queue is bad and no key is watched', async () => {
    const key = `execdirty:${randomKey()}`
    const directConn = await ownerOf(key)

    await expectReply(directConn, ['MULTI'], '+OK\r\n')
    await expectReplyPrefix(
      directConn,
      ['NOTACOMMAND', 'x'],
      '-ERR unknown command',
    )
    await expectReply(directConn, ['EXEC'], EXECABORT)
  })

  test('returns nil (not EXECABORT) when only a watched key changed and the queue is clean', async () => {
    const key = `execdirty:${randomKey()}`
    const directConn = await ownerOf(key)
    const mutatingConn = await ownerOf(key)

    await expectReply(directConn, ['SET', key, 'init'], '+OK\r\n')
    await expectReply(directConn, ['WATCH', key], '+OK\r\n')
    await expectReply(mutatingConn, ['SET', key, 'modified'], '+OK\r\n')

    await expectReply(directConn, ['MULTI'], '+OK\r\n')
    await expectReply(directConn, ['SET', key, 'queued'], '+QUEUED\r\n')

    // Clean queue + dirty CAS -> aborts with a nil array, never EXECABORT.
    await expectReply(directConn, ['EXEC'], '*-1\r\n')

    // The watched key keeps the other connection's value.
    await expectReply(directConn, ['GET', key], '$8\r\nmodified\r\n')
  })

  test('returns ERR EXEC without MULTI when called outside a transaction', async () => {
    const key = `execdirty:${randomKey()}`
    const conn = await ownerOf(key)

    await expectReply(conn, ['EXEC'], '-ERR EXEC without MULTI\r\n')
  })

  test('DISCARD clears queued writes without executing them', async () => {
    const key = `discard:${randomKey()}`
    const conn = await ownerOf(key)

    await expectReply(conn, ['MULTI'], '+OK\r\n')
    await expectReply(conn, ['SET', key, 'value'], '+QUEUED\r\n')
    await expectReply(conn, ['DISCARD'], '+OK\r\n')
    await expectReply(conn, ['GET', key], '$-1\r\n')
  })

  test('transaction mode errors match Redis', async () => {
    const key = `tx-errors:${randomKey()}`
    const conn = await ownerOf(key)

    await expectReply(conn, ['EXEC'], '-ERR EXEC without MULTI\r\n')
    await expectReply(conn, ['DISCARD'], '-ERR DISCARD without MULTI\r\n')

    await expectReply(conn, ['MULTI'], '+OK\r\n')
    await expectReply(
      conn,
      ['WATCH', key],
      '-ERR WATCH inside MULTI is not allowed\r\n',
    )
    await expectReply(conn, ['EXEC'], '*0\r\n')
  })

  test('EXEC with wrong arity aborts the transaction with EXECABORT', async () => {
    const key = `exec-arity:${randomKey()}`
    const conn = await ownerOf(key)

    await expectReply(conn, ['MULTI'], '+OK\r\n')
    await expectReply(conn, ['SET', key, 'value'], '+QUEUED\r\n')
    await expectReply(
      conn,
      ['EXEC', 'foo'],
      "-EXECABORT Transaction discarded because of: wrong number of arguments for 'exec' command\r\n",
    )

    // Session is back in normal mode: this SET runs immediately and the queued
    // SET above never ran.
    await expectReply(conn, ['SET', key, 'value2'], '+OK\r\n')
    await expectReply(conn, ['GET', key], '$6\r\nvalue2\r\n')
  })

  test('UNWATCH is queued in MULTI and returns OK from EXEC', async () => {
    const key = `unwatch:${randomKey()}`
    const conn = await ownerOf(key)

    await expectReply(conn, ['WATCH', key], '+OK\r\n')
    await expectReply(conn, ['MULTI'], '+OK\r\n')
    await expectReply(conn, ['UNWATCH'], '+QUEUED\r\n')
    await expectReply(conn, ['EXEC'], '*1\r\n+OK\r\n')
  })

  test('arg-content errors stay inline inside MULTI/EXEC (not EXECABORT)', async () => {
    const key = `hexpire-multi-errors:${randomKey()}`
    const conn = await ownerOf(key)

    await expectReply(conn, ['MULTI'], '+OK\r\n')
    await expectReply(conn, ['HSET', key, 'field', 'value'], '+QUEUED\r\n')
    // A bad numeric arg parses fine at queue time -> +QUEUED, fails at EXEC.
    await expectReply(
      conn,
      ['HEXPIRE', key, 'abc', 'FIELDS', '1', 'field'],
      '+QUEUED\r\n',
    )
    await expectReply(conn, ['HGET', key, 'field'], '+QUEUED\r\n')

    // EXEC runs the queue: the bad HEXPIRE surfaces as an inline error element,
    // the other commands still apply.
    await expectReply(
      conn,
      ['EXEC'],
      '*3\r\n:1\r\n-ERR value is not an integer or out of range\r\n$5\r\nvalue\r\n',
    )
  })
})
