import { after, before, describe, test } from 'node:test'

import { TestRunner } from '../test-config'
import { activeProfile, randomKey } from '../utils'
import { RawRedisConnection } from '../raw-tcp/raw-connection'
import { expectReply } from '../raw-tcp/helpers'

/**
 * EVAL / EVALSHA / FCALL numkeys checks (#519). Checked against
 * redis-server 6.2.14, 7.0.15, 8.0.0 and valkey-server 9.0.0, the same on
 * each: numkeys is any int64, then more than the arguments, then negative
 * (eval.c evalGenericCommand). FCALL looks the function up first and words a
 * non-integer count its own way (functions.c fcallCommandGeneric).
 */
const testRunner = new TestRunner()
const profile = activeProfile
const legacy = profile === 'redis-6.2'

const NEGATIVE = "-ERR Number of keys can't be negative\r\n"
const GREATER = "-ERR Number of keys can't be greater than number of args\r\n"
const NOT_INTEGER = '-ERR value is not an integer or out of range\r\n'
const BAD_NUMBER = '-ERR Bad number of keys provided\r\n'
const NOT_FOUND = '-ERR Function not found\r\n'
const UNKNOWN_SHA = '0123456789012345678901234567890123456789'

describe(`Script numkeys wording (${testRunner.getBackendName()}, ${profile})`, () => {
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

  test('EVAL and EVALSHA refuse a negative numkeys', async () => {
    const conn = await connect()
    const key = randomKey()
    await expectReply(conn, ['EVAL', 'return 1', '-1'], NEGATIVE)
    await expectReply(conn, ['EVAL', 'return 1', '-1', key], NEGATIVE)
    await expectReply(
      conn,
      ['EVAL', 'return 1', '-9223372036854775808'],
      NEGATIVE,
    )
    await expectReply(conn, ['EVALSHA', UNKNOWN_SHA, '-1', key], NEGATIVE)
    await expectReply(conn, ['EVALSHA', UNKNOWN_SHA, '2', key], GREATER)
  })

  test('EVAL reads numkeys as an int64 before the other checks', async () => {
    const conn = await connect()
    const key = randomKey()
    await expectReply(
      conn,
      ['EVAL', 'return 1', '9223372036854775808'],
      NOT_INTEGER,
    )
    await expectReply(conn, ['EVAL', 'return 1', '-0'], NOT_INTEGER)
    await expectReply(conn, ['EVAL', 'return 1', '1x', key], NOT_INTEGER)
    await expectReply(conn, ['EVAL', 'return 1', '2', key], GREATER)
    await expectReply(conn, ['EVAL', 'return 1', '3000000000', key], GREATER)
  })

  test(
    'EVAL_RO and EVALSHA_RO refuse a negative numkeys',
    {
      skip: legacy,
    },
    async () => {
      const conn = await connect()
      const key = randomKey()
      await expectReply(conn, ['EVAL_RO', 'return 1', '-1', key], NEGATIVE)
      await expectReply(conn, ['EVALSHA_RO', UNKNOWN_SHA, '-1', key], NEGATIVE)
    },
  )

  test(
    'FCALL finds the function before it checks numkeys',
    {
      skip: legacy,
    },
    async () => {
      const conn = await connect()
      const id = randomKey()
      const library = `numkeys${id}`
      const fn = `f${id}`
      const key = randomKey()
      await expectReply(
        conn,
        [
          'FUNCTION',
          'LOAD',
          `#!lua name=${library}\nredis.register_function('${fn}', function() return 1 end)`,
        ],
        `$${library.length}\r\n${library}\r\n`,
      )

      try {
        await expectReply(conn, ['FCALL', fn, '-1', key], NEGATIVE)
        await expectReply(conn, ['FCALL_RO', fn, '-1', key], NEGATIVE)
        await expectReply(conn, ['FCALL', fn, '-9223372036854775808'], NEGATIVE)
        await expectReply(conn, ['FCALL', fn, '1x', key], BAD_NUMBER)
        await expectReply(conn, ['FCALL', fn, '-0'], BAD_NUMBER)
        await expectReply(
          conn,
          ['FCALL', fn, '9223372036854775808'],
          BAD_NUMBER,
        )
        await expectReply(conn, ['FCALL', fn, '2', key], GREATER)
        await expectReply(conn, ['FCALL', fn, '0'], ':1\r\n')
        await expectReply(conn, ['FCALL', `no${fn}`, '-1', key], NOT_FOUND)
        await expectReply(conn, ['FCALL', `no${fn}`, '2', key], NOT_FOUND)
        await expectReply(conn, ['FCALL', `no${fn}`, '1x'], NOT_FOUND)
      } finally {
        await expectReply(conn, ['FUNCTION', 'DELETE', library], '+OK\r\n')
      }
    },
  )
})
