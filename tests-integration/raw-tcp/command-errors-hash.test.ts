import { after, before, describe, test } from 'node:test'
import { TestRunner } from '../test-config'
import { randomKey } from '../utils'
import { RawRedisConnection } from './raw-connection'
import { expectReply } from './helpers'

/**
 * Hash command-error integration tests over a bare socket — wrong arity, bad
 * increment arguments, and non-numeric stored field values. These are
 * slot-independent server RESP errors with no client-side value, moved out of
 * the ioredis suite where they previously went through `.call(...)`.
 */
const testRunner = new TestRunner()

const WRONGTYPE =
  '-WRONGTYPE Operation against a key holding the wrong kind of value\r\n'
const NOT_INTEGER = '-ERR value is not an integer or out of range\r\n'
const NOT_FLOAT = '-ERR value is not a valid float\r\n'

describe(`Raw TCP hash command errors (${testRunner.getBackendName()})`, () => {
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

  test('hash command errors match Redis', async () => {
    const conn = await connect()
    const hashKey = `hash-errors:${randomKey()}:hash`
    const stringKey = `hash-errors:${randomKey()}:string`

    await expectReply(conn, ['SET', stringKey, 'value'], '+OK\r\n')
    await expectReply(
      conn,
      [
        'HSET',
        hashKey,
        'integer',
        'abc',
        'float',
        'abc',
        'float-trailing-garbage',
        '1abc',
        'float-dangling-exponent',
        '1.0e',
        'float-trailing-space',
        '1.5 ',
        'leading-zero',
        '007',
        'negative-zero',
        '-0',
      ],
      ':7\r\n',
    )

    await expectReply(conn, ['HGET', stringKey, 'field'], WRONGTYPE)
    await expectReply(
      conn,
      ['HSET', hashKey, 'field'],
      "-ERR wrong number of arguments for 'hset' command\r\n",
    )
    await expectReply(conn, ['HINCRBY', hashKey, 'integer', 'abc'], NOT_INTEGER)
    await expectReply(conn, ['HINCRBY', hashKey, 'integer', '01'], NOT_INTEGER)
    await expectReply(
      conn,
      ['HINCRBY', hashKey, 'integer', '1'],
      '-ERR hash value is not an integer\r\n',
    )
    await expectReply(
      conn,
      ['HINCRBY', hashKey, 'leading-zero', '1'],
      '-ERR hash value is not an integer\r\n',
    )
    await expectReply(
      conn,
      ['HINCRBY', hashKey, 'negative-zero', '1'],
      '-ERR hash value is not an integer\r\n',
    )
    await expectReply(
      conn,
      ['HINCRBYFLOAT', hashKey, 'float', 'abc'],
      NOT_FLOAT,
    )
    await expectReply(
      conn,
      ['HINCRBYFLOAT', hashKey, 'float', '1.5'],
      '-ERR hash value is not a float\r\n',
    )
    for (const field of [
      'float-trailing-garbage',
      'float-dangling-exponent',
      'float-trailing-space',
    ]) {
      await expectReply(
        conn,
        ['HINCRBYFLOAT', hashKey, field, '1'],
        '-ERR hash value is not a float\r\n',
      )
    }
    for (const increment of ['1abc', '1.0e', '1.5 ']) {
      await expectReply(
        conn,
        ['HINCRBYFLOAT', hashKey, 'missing', increment],
        NOT_FLOAT,
      )
    }
  })
})
