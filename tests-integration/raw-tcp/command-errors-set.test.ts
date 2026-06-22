import { after, before, describe, test } from 'node:test'
import { TestRunner } from '../test-config'
import { randomKey } from '../utils'
import { RawRedisConnection } from './raw-connection'
import { expectReply } from './helpers'

/**
 * Set command-error integration tests over a bare socket — wrong-type targets
 * and a non-integer count. Slot-independent server RESP errors with no
 * client-side value, moved out of the ioredis suite where they previously went
 * through `.call(...)`.
 */
const testRunner = new TestRunner()

const WRONGTYPE =
  '-WRONGTYPE Operation against a key holding the wrong kind of value\r\n'

describe(`Raw TCP set command errors (${testRunner.getBackendName()})`, () => {
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

  test('set command errors match Redis', async () => {
    const conn = await connect()
    const setKey = `set-errors:${randomKey()}:set`
    const stringKey = `set-errors:${randomKey()}:string`

    await expectReply(conn, ['SADD', setKey, 'a'], ':1\r\n')
    await expectReply(conn, ['SET', stringKey, 'value'], '+OK\r\n')

    await expectReply(conn, ['SADD', stringKey, 'a'], WRONGTYPE)
    await expectReply(
      conn,
      ['SRANDMEMBER', setKey, 'abc'],
      '-ERR value is not an integer or out of range\r\n',
    )
    await expectReply(conn, ['SMOVE', setKey, stringKey, 'a'], WRONGTYPE)
  })
})
