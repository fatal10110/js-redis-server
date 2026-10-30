import { after, before, describe, test } from 'node:test'
import assert from 'node:assert'
import { STANDALONE_AUTH_PASSWORD, TestRunner } from '../test-config'
import { commandFrame, randomKey } from '../utils'
import { RawRedisConnection } from './raw-connection'

/**
 * With `requirepass` set, a connection that has not authenticated may send at
 * most 10 multibulk elements and 16384-byte bulks (#565). Anything larger is a
 * protocol error, and the server closes the connection. AUTH lifts the caps and
 * RESET puts them back.
 *
 * Redis checks it in `processMultibulkBuffer` (`ll > 10 && authRequired(c)`,
 * `ll > 16384 && authRequired(c)`), after the version's own length bounds.
 * Ground-truthed byte for byte on redis 6.2.14, 7.0.15, 7.2.4, 7.4.4 and 8.0.0
 * and valkey 8.0.0 and 9.0.0, which all behave the same.
 */
const testRunner = new TestRunner()

const NOAUTH = '-NOAUTH Authentication required.\r\n'
const UNAUTH_MULTIBULK =
  '-ERR Protocol error: unauthenticated multibulk length\r\n'
const UNAUTH_BULK = '-ERR Protocol error: unauthenticated bulk length\r\n'

/** How long to wait for the server to hang up before calling it a failure. */
const CLOSE_TIMEOUT_MS = 15000

/** DEL with ten keys: an 11-element multibulk. */
function delOfTen(): string[] {
  return ['DEL', ...Array.from({ length: 10 }, () => randomKey())]
}

async function expectThenClose(
  conn: RawRedisConnection,
  expected: string,
): Promise<void> {
  let timer: NodeJS.Timeout | undefined
  const tail = await Promise.race([
    conn.readUntilClose(),
    new Promise<never>((_, reject) => {
      timer = setTimeout(
        () =>
          reject(
            new Error(
              `server did not close the connection within ${CLOSE_TIMEOUT_MS}ms`,
            ),
          ),
        CLOSE_TIMEOUT_MS,
      )
    }),
  ]).finally(() => clearTimeout(timer))
  assert.strictEqual(tail.toString(), expected)
}

describe(`Raw TCP unauthenticated request caps (${testRunner.getBackendName()})`, () => {
  let port: number
  const opened: RawRedisConnection[] = []

  async function connect(): Promise<RawRedisConnection> {
    const conn = await RawRedisConnection.connect('127.0.0.1', port)
    opened.push(conn)
    return conn
  }

  async function expectReply(
    conn: RawRedisConnection,
    frame: Buffer | string,
    expected: string,
  ): Promise<void> {
    conn.write(frame)
    assert.strictEqual((await conn.readRawFrame()).toString(), expected)
  }

  before(async () => {
    port = await testRunner.setupRawStandaloneAuth()
  })

  after(async () => {
    for (const conn of opened) {
      conn.close()
    }
    await testRunner.cleanup()
  })

  test('10 elements and a 16384-byte bulk are accepted before AUTH', async () => {
    const conn = await connect()
    // Both reach the auth check, which answers NOAUTH and keeps the
    // connection open: the decoder let them through.
    await expectReply(conn, commandFrame(...delOfTen().slice(0, 10)), NOAUTH)
    await expectReply(conn, commandFrame('ECHO', 'x'.repeat(16384)), NOAUTH)
    await expectReply(conn, commandFrame('PING'), NOAUTH)
  })

  const refused: Array<[string, string, string]> = [
    ['an 11-element count line alone', '*11\r\n', UNAUTH_MULTIBULK],
    [
      'an 11-element count with a first bulk',
      '*11\r\n$3\r\nfoo\r\n',
      UNAUTH_MULTIBULK,
    ],
    [
      'a skipped *-1 before an 11-element count',
      '*-1\r\n*11\r\n',
      UNAUTH_MULTIBULK,
    ],
    ['a 16385-byte first bulk', '*1\r\n$16385\r\n', UNAUTH_BULK],
    ['a 16385-byte second bulk', '*2\r\n$3\r\nGET\r\n$16385\r\n', UNAUTH_BULK],
  ]
  for (const [name, bytes, expected] of refused) {
    test(`${name} is refused before AUTH, then the connection closes`, async () => {
      const conn = await connect()
      conn.write(bytes)
      await expectThenClose(conn, expected)
    })
  }

  test('AUTH lifts the caps and RESET restores them', async () => {
    const conn = await connect()
    await expectReply(
      conn,
      commandFrame('AUTH', 'wrong'),
      '-WRONGPASS invalid username-password pair or user is disabled.\r\n',
    )
    await expectReply(
      conn,
      commandFrame('AUTH', STANDALONE_AUTH_PASSWORD),
      '+OK\r\n',
    )
    await expectReply(conn, commandFrame(...delOfTen()), ':0\r\n')
    const big = 'y'.repeat(16385)
    await expectReply(conn, commandFrame('ECHO', big), `$16385\r\n${big}\r\n`)
    await expectReply(conn, commandFrame('RESET'), '+RESET\r\n')
    conn.write(commandFrame(...delOfTen()))
    await expectThenClose(conn, UNAUTH_MULTIBULK)
  })
})
