import { after, before, describe, test } from 'node:test'
import { TestRunner } from '../test-config'
import { randomKey } from '../utils'
import { RawRedisConnection } from './raw-connection'
import { expectReply } from './helpers'

/**
 * GETRANGE / SUBSTR with both indexes negative and start > end answer an
 * empty bulk before the indexes are resolved or clamped (#650): `GETRANGE s
 * -1 -100` on a 1-byte value is empty, not the clamped first byte. The key
 * lookup and type check still come first. Identical on redis-server 6.2.14,
 * 7.0.15, 7.2.4, 7.4.4, 8.0.0 and valkey-server 8.0.0, 9.0.0 (t_string.c
 * getrangeCommand: `if (start < 0 && end < 0 && start > end)`).
 */
const testRunner = new TestRunner()

const EMPTY = '$0\r\n\r\n'

describe(`Raw TCP GETRANGE negative ranges (${testRunner.getBackendName()})`, () => {
  let conn: RawRedisConnection
  let one: string
  let hello: string

  before(async () => {
    const port = await testRunner.setupRawStandalone()
    conn = await RawRedisConnection.connect('127.0.0.1', port)
    one = `getrange:${randomKey()}:one`
    hello = `getrange:${randomKey()}:hello`
    await expectReply(conn, ['SET', one, 'v'], '+OK\r\n')
    await expectReply(conn, ['SET', hello, 'hello'], '+OK\r\n')
  })

  after(async () => {
    conn.close()
    await testRunner.cleanup()
  })

  test('both negative with start > end is empty before clamping', async () => {
    for (const command of ['GETRANGE', 'SUBSTR']) {
      await expectReply(conn, [command, one, '-1', '-100'], EMPTY)
      await expectReply(conn, [command, one, '-5', '-100'], EMPTY)
      await expectReply(conn, [command, hello, '-100', '-200'], EMPTY)
      await expectReply(
        conn,
        [command, hello, '-9223372036854775807', '-9223372036854775808'],
        EMPTY,
      )
      await expectReply(conn, [command, hello, '-1', '-100'], EMPTY)
      await expectReply(conn, [command, hello, '-2', '-3'], EMPTY)
    }
  })

  test('other ranges still resolve and clamp', async () => {
    await expectReply(conn, ['GETRANGE', hello, '0', '-100'], '$1\r\nh\r\n')
    await expectReply(conn, ['GETRANGE', hello, '-100', '-50'], '$1\r\nh\r\n')
    await expectReply(conn, ['GETRANGE', hello, '3', '-10'], EMPTY)
    await expectReply(conn, ['GETRANGE', hello, '-3', '-2'], '$2\r\nll\r\n')
    await expectReply(
      conn,
      ['GETRANGE', hello, '-9223372036854775808', '-9223372036854775807'],
      '$1\r\nh\r\n',
    )
    await expectReply(conn, ['SUBSTR', hello, '-3', '-2'], '$2\r\nll\r\n')
  })

  test('the key lookup and type check come first', async () => {
    const list = `getrange:${randomKey()}:list`
    await expectReply(conn, ['RPUSH', list, 'a'], ':1\r\n')
    await expectReply(
      conn,
      ['GETRANGE', list, '-1', '-100'],
      '-WRONGTYPE Operation against a key holding the wrong kind of value\r\n',
    )
    await expectReply(
      conn,
      ['GETRANGE', `getrange:${randomKey()}:missing`, '-1', '-100'],
      EMPTY,
    )
    await expectReply(
      conn,
      ['GETRANGE', hello, 'a', '-1'],
      '-ERR value is not an integer or out of range\r\n',
    )
    await expectReply(
      conn,
      ['GETRANGE', hello, '0'],
      "-ERR wrong number of arguments for 'getrange' command\r\n",
    )
  })

  test('inside MULTI and from a script', async () => {
    await expectReply(conn, ['MULTI'], '+OK\r\n')
    await expectReply(conn, ['GETRANGE', one, '-1', '-100'], '+QUEUED\r\n')
    await expectReply(conn, ['EXEC'], '*1\r\n$0\r\n\r\n')
    await expectReply(
      conn,
      ['EVAL', "return redis.call('GETRANGE', KEYS[1], -1, -100)", '1', one],
      EMPTY,
    )
  })
})
