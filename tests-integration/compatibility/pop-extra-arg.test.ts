import assert from 'node:assert/strict'
import { after, before, describe, test } from 'node:test'

import { TestRunner } from '../test-config'
import { activeProfile, commandFrame, randomKey } from '../utils'
import { RawRedisConnection } from '../raw-tcp/raw-connection'
import { expectReply } from '../raw-tcp/helpers'

/**
 * ZPOPMIN / ZPOPMAX / SPOP with more than one token after the key (#619).
 * Checked against redis-server 6.2.14, 7.0.15, 7.2.4, 7.4.4, 8.0.0 and
 * valkey-server 8.0.0, 9.0.0: every version answers `syntax error` from an
 * `argc > 3` check that runs before the count is parsed and before the key is
 * looked up, so a bad count, a missing key or a wrong-type key never shows.
 * The table arity stays -2. LPOP / RPOP answer `wrong number of arguments`
 * for the same shape.
 */
const testRunner = new TestRunner()
const profile = activeProfile

const SYNTAX = '-ERR syntax error\r\n'
const WRONGTYPE =
  '-WRONGTYPE Operation against a key holding the wrong kind of value\r\n'
const EXTRA_ARGS = [
  ['1', '2'],
  ['x', '2'],
  ['-1', '2'],
  ['0', '2'],
  ['1', '2', '3'],
]

describe(`Pop extra argument (${testRunner.getBackendName()}, ${profile})`, () => {
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

  async function seed(conn: RawRedisConnection) {
    const missing = randomKey()
    const str = randomKey()
    const list = randomKey()
    const set = randomKey()
    const zset = randomKey()
    await expectReply(conn, ['SET', str, 'v'], '+OK\r\n')
    await expectReply(conn, ['RPUSH', list, 'a', 'b'], ':2\r\n')
    await expectReply(conn, ['SADD', set, 'a'], ':1\r\n')
    await expectReply(conn, ['ZADD', zset, '1', 'a'], ':1\r\n')
    return { missing, str, list, set, zset }
  }

  test('ZPOPMIN / ZPOPMAX / SPOP answer syntax error before the count and the key', async () => {
    const conn = await connect()
    const keys = await seed(conn)
    for (const command of ['ZPOPMIN', 'ZPOPMAX', 'SPOP']) {
      for (const key of Object.values(keys)) {
        for (const extra of EXTRA_ARGS) {
          await expectReply(conn, [command, key, ...extra], SYNTAX)
        }
      }
    }
    await expectReply(conn, ['ZCARD', keys.zset], ':1\r\n')
    await expectReply(conn, ['SCARD', keys.set], ':1\r\n')
    await expectReply(conn, ['GET', keys.str], '$1\r\nv\r\n')
    await expectReply(conn, ['EXISTS', keys.missing], ':0\r\n')
  })

  test('LPOP / RPOP keep wrong number of arguments', async () => {
    const conn = await connect()
    const { list, str } = await seed(conn)
    for (const command of ['LPOP', 'RPOP']) {
      for (const key of [list, str]) {
        await expectReply(
          conn,
          [command, key, '1', '2'],
          `-ERR wrong number of arguments for '${command.toLowerCase()}' command\r\n`,
        )
      }
    }
    await expectReply(conn, ['LLEN', list], ':2\r\n')
  })

  test('without an extra argument the count and the type are checked as before', async () => {
    const conn = await connect()
    const { str, zset, set } = await seed(conn)
    await expectReply(conn, ['ZPOPMIN', str, '1'], WRONGTYPE)
    await expectReply(conn, ['SPOP', str, '1'], WRONGTYPE)
    await expectReply(conn, ['ZPOPMIN', str], WRONGTYPE)
    await expectReply(
      conn,
      ['SPOP', set, '-1'],
      '-ERR value is out of range, must be positive\r\n',
    )
    await expectReply(
      conn,
      ['ZPOPMIN', zset, '1'],
      '*2\r\n$1\r\na\r\n$1\r\n1\r\n',
    )
    await expectReply(conn, ['SPOP', set], '$1\r\na\r\n')
    await expectReply(
      conn,
      ['ZPOPMIN'],
      "-ERR wrong number of arguments for 'zpopmin' command\r\n",
    )
    await expectReply(
      conn,
      ['SPOP'],
      "-ERR wrong number of arguments for 'spop' command\r\n",
    )
  })

  test('an extra argument inside MULTI fails at EXEC', async () => {
    const conn = await connect()
    const { zset, set } = await seed(conn)
    await expectReply(conn, ['MULTI'], '+OK\r\n')
    await expectReply(conn, ['ZPOPMIN', zset, '1', '2'], '+QUEUED\r\n')
    await expectReply(conn, ['ZPOPMAX', zset, 'x', '2'], '+QUEUED\r\n')
    await expectReply(conn, ['SPOP', set, '1', '2'], '+QUEUED\r\n')
    await expectReply(conn, ['EXEC'], `*3\r\n${SYNTAX}${SYNTAX}${SYNTAX}`)
    await expectReply(conn, ['ZCARD', zset], ':1\r\n')
    await expectReply(conn, ['SCARD', set], ':1\r\n')
  })

  test('an extra argument reaches a script as syntax error', async () => {
    const conn = await connect()
    const { zset, set } = await seed(conn)
    for (const [command, key] of [
      ['zpopmin', zset],
      ['zpopmax', zset],
      ['spop', set],
    ]) {
      await expectReply(
        conn,
        ['EVAL', `return redis.pcall('${command}', KEYS[1], 1, 2)`, '1', key],
        SYNTAX,
      )
    }
  })

  test('COMMAND INFO keeps arity -2', async () => {
    const conn = await connect()
    for (const command of ['zpopmin', 'zpopmax', 'spop']) {
      conn.write(commandFrame('COMMAND', 'INFO', command))
      const reply = (await conn.readRawFrame()).toString()
      assert.ok(
        reply.includes(`$${command.length}\r\n${command}\r\n:-2\r\n`),
        `${command}: ${JSON.stringify(reply)}`,
      )
    }
  })
})
