import { after, before, describe, test } from 'node:test'
import { TestRunner } from '../test-config'
import { randomKey } from '../utils'
import { RawRedisConnection } from './raw-connection'
import { expectReply } from './helpers'

/**
 * Keyspace / server command-error integration tests over a bare socket.
 *
 * MOVE/COPY DB-index, FLUSH modifiers, SCAN cursors and arity errors are all
 * slot-independent server-side RESP errors. A real client adds no value
 * relaying them (and a cluster client can't even reach the multi-DB paths), so
 * they live here against a standalone server. Moved out of the ioredis suite,
 * where they previously went through `.call(...)`.
 */
const testRunner = new TestRunner()

const DB_OUT_OF_RANGE = '-ERR DB index is out of range\r\n'
const NOT_INTEGER = '-ERR value is not an integer or out of range\r\n'
const SYNTAX = '-ERR syntax error\r\n'
const arity = (cmd: string) =>
  `-ERR wrong number of arguments for '${cmd}' command\r\n`

describe(`Raw TCP keyspace command errors (${testRunner.getBackendName()})`, () => {
  let port: number
  const connections: RawRedisConnection[] = []

  before(async () => {
    port = await testRunner.setupRawStandalone()
  })

  after(async () => {
    for (const connection of connections) {
      connection.close()
    }
    connections.length = 0
    await testRunner.cleanup()
  })

  async function connect(): Promise<RawRedisConnection> {
    const connection = await RawRedisConnection.connect('127.0.0.1', port)
    connections.push(connection)
    return connection
  }

  test('MOVE argument errors', async () => {
    const conn = await connect()
    const key = `move-errors:${randomKey()}`

    await expectReply(conn, ['SET', key, 'value'], '+OK\r\n')

    await expectReply(
      conn,
      ['MOVE', key, '0'],
      '-ERR source and destination objects are the same\r\n',
    )
    await expectReply(conn, ['MOVE', key, '99'], DB_OUT_OF_RANGE)
    await expectReply(conn, ['MOVE', key, '-1'], DB_OUT_OF_RANGE)
    await expectReply(conn, ['MOVE', key, 'abc'], NOT_INTEGER)
    await expectReply(conn, ['MOVE'], arity('move'))
    await expectReply(conn, ['MOVE', 'only-key'], arity('move'))
    await expectReply(conn, ['MOVE', 'key', '1', 'extra'], arity('move'))
  })

  test('COPY argument errors', async () => {
    const conn = await connect()
    const key = `copy-errors:${randomKey()}`
    const dst = `${key}-d`

    await expectReply(conn, ['SET', key, 'value'], '+OK\r\n')

    await expectReply(conn, ['COPY', key, key, 'DB', '99'], DB_OUT_OF_RANGE)
    await expectReply(conn, ['COPY', key, dst, 'DB', '-1'], DB_OUT_OF_RANGE)
    await expectReply(conn, ['COPY', key, dst, 'DB', 'abc'], NOT_INTEGER)
    await expectReply(conn, ['COPY'], arity('copy'))
    await expectReply(conn, ['COPY', 'onlysrc'], arity('copy'))
    await expectReply(conn, ['COPY', key, dst, 'NOPE'], SYNTAX)
    await expectReply(conn, ['COPY', key, dst, 'DB'], SYNTAX)
  })

  test('FLUSHDB / FLUSHALL modifier errors', async () => {
    const conn = await connect()

    await expectReply(conn, ['FLUSHDB', 'FOO'], SYNTAX)
    await expectReply(conn, ['FLUSHDB', 'ASYNC', 'SYNC'], SYNTAX)
    await expectReply(conn, ['FLUSHALL', 'FOO'], SYNTAX)
    await expectReply(conn, ['FLUSHALL', 'ASYNC', 'SYNC'], SYNTAX)
  })

  test('TIME / LASTSAVE / RANDOMKEY reject extra arguments', async () => {
    const conn = await connect()

    await expectReply(conn, ['TIME', 'extra'], arity('time'))
    await expectReply(conn, ['LASTSAVE', 'extra'], arity('lastsave'))
    await expectReply(conn, ['RANDOMKEY', 'extra'], arity('randomkey'))
  })
})
