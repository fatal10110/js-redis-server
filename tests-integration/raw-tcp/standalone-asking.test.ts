import { after, before, describe, test } from 'node:test'
import { TestRunner } from '../test-config'
import { activeProfile, randomKey } from '../utils'
import { RawRedisConnection } from './raw-connection'
import { expectReply } from './helpers'

/**
 * ASKING and RESTORE-ASKING on a standalone (`cluster-enabled no`) server
 * (#616, #85): ASKING is in the command table and refused as disabled (in
 * MULTI it is queued and refused in its EXEC slot, from a script the same);
 * RESTORE-ASKING is RESTORE. Identical on redis 6.2.14, 7.0.15, 7.2.4, 7.4.4,
 * 8.0.0 and valkey 8.0.0, 9.0.0.
 *
 * Profile-aware: run with `REDIS_COMPAT=<preset>` against the mock on that
 * profile or a real server of that version.
 */
const testRunner = new TestRunner()
const DISABLED = '-ERR This instance has cluster support disabled\r\n'

describe(`Raw TCP standalone ASKING / RESTORE-ASKING (${testRunner.getBackendName()}, ${activeProfile})`, () => {
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

  test('ASKING is refused as cluster support disabled', async () => {
    const conn = await connect()
    await expectReply(conn, ['ASKING'], DISABLED)
    await expectReply(conn, ['asking'], DISABLED)
    await expectReply(
      conn,
      ['ASKING', 'x'],
      "-ERR wrong number of arguments for 'asking' command\r\n",
    )
  })

  test('ASKING is queued in MULTI and refused in its EXEC slot', async () => {
    const conn = await connect()
    await expectReply(conn, ['MULTI'], '+OK\r\n')
    await expectReply(conn, ['ASKING'], '+QUEUED\r\n')
    await expectReply(conn, ['EXEC'], `*1\r\n${DISABLED}`)
  })

  test('a script calling ASKING gets the same refusal', async () => {
    const conn = await connect()
    await expectReply(
      conn,
      ['EVAL', "return redis.pcall('ASKING')", '0'],
      DISABLED,
    )
  })

  test('RESTORE-ASKING behaves as RESTORE', async () => {
    const conn = await connect()
    const key = `restore-asking:${randomKey()}`
    await expectReply(
      conn,
      ['RESTORE-ASKING', key, '0', 'x'],
      '-ERR DUMP payload version or checksum are wrong\r\n',
    )
    await expectReply(conn, ['SET', key, 'v'], '+OK\r\n')
    await expectReply(
      conn,
      ['RESTORE-ASKING', key, '0', 'x'],
      '-BUSYKEY Target key name already exists.\r\n',
    )
    await expectReply(conn, ['DEL', key], ':1\r\n')
  })
})
