import { after, before, describe, test } from 'node:test'
import assert from 'node:assert'
import { TestRunner } from '../test-config'
import { activeProfile, commandFrame } from '../utils'
import { RawRedisConnection } from './raw-connection'

/**
 * Raw TCP coverage for RESP2 request framing edge cases (#441): the multibulk
 * element-count bound, the bytes after a bulk payload, and the 64KB cap on an
 * inline request. None of these can be put on the wire by a real client.
 *
 * Ground-truthed on redis 6.2.24, 7.0.15, 8.0 and valkey 7.2.14 / 8.0.0 /
 * 9.0.0. The inline-cap and length-format assertions hold on all of them. The
 * exceptions:
 *  - "bytes after a bulk payload" holds on every Redis version and on the
 *    Valkey `x.0.0` releases, which the Valkey presets model. Valkey patch
 *    releases (7.2.14, 8.0.11, 9.0.6) refuse a bad terminator with `Protocol
 *    error: invalid CRLF in request` instead.
 *  - The unknown-command reply is matched on its prefix only, because 6.2
 *    quotes the name with backticks (`foo`) where 7.0+ uses single quotes.
 *  - The multibulk count bound is version-specific: 6.2 refuses more than
 *    1024*1024 elements. The 7.0+ side is asserted here, skipped on the
 *    `redis-6.2` profile, and both sides are covered per profile in
 *    `tests-integration/compatibility/multibulk-count-gate.test.ts`.
 */
const testRunner = new TestRunner()

/** Redis' `PROTO_INLINE_MAX_SIZE`. */
const INLINE_MAX = 64 * 1024
const TOO_BIG_INLINE = '-ERR Protocol error: too big inline request\r\n'

/** How long to wait for the server to hang up before calling it a failure. */
const CLOSE_TIMEOUT_MS = 15000

/**
 * Assert the connection received exactly `expected` and was then closed by the
 * server. Bounded, because the pre-fix behaviour for several cases here is
 * "keep buffering and never answer", which would otherwise hang the runner.
 */
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

describe(`Raw TCP RESP2 decoder framing (${testRunner.getBackendName()})`, () => {
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

  describe('bytes after a bulk payload', () => {
    // Redis reads exactly `ll` bytes and skips the next two without looking at
    // them, so a wrong terminator is not a protocol error: `foo` dispatches.
    test('a bulk with a wrong terminator is dispatched, not refused', async () => {
      const conn = await connect()

      conn.write('*1\r\n$3\r\nfooXX')

      // 6.2 quotes the name with backticks, 7.0+ with single quotes.
      assert.match(
        (await conn.readRawFrame()).toString(),
        /^-ERR unknown command [`']foo[`'], with args beginning with: \r\n$/,
      )

      // ...and the connection stays usable.
      conn.write(commandFrame('PING'))
      assert.strictEqual((await conn.readRawFrame()).toString(), '+PONG\r\n')
    })

    // The two skipped bytes are consumed whatever they are, so framing of the
    // *next* element and the next command lines up exactly as if they had been
    // CRLF.
    test('the two bytes after each payload are skipped, whatever they are', async () => {
      const conn = await connect()

      conn.write(
        Buffer.concat([
          Buffer.from('*2\r\n$4\r\nPINGXX$2\r\nhi\n\n'),
          commandFrame('PING'),
        ]),
      )

      assert.strictEqual((await conn.readRawFrame()).toString(), '$2\r\nhi\r\n')
      assert.strictEqual((await conn.readRawFrame()).toString(), '+PONG\r\n')
    })
  })

  describe('multibulk element count', () => {
    // `ll <= 0` is skipped outright: Redis never errors on a negative count.
    test('a negative multibulk count is skipped like *0 and *-1', async () => {
      const conn = await connect()

      conn.write(
        Buffer.concat([
          Buffer.from('*-5\r\n*-1\r\n*0\r\n'),
          commandFrame('PING'),
        ]),
      )

      assert.strictEqual((await conn.readRawFrame()).toString(), '+PONG\r\n')
    })

    test('a count above INT_MAX is refused on every version', async () => {
      const conn = await connect()

      conn.write('*2147483648\r\n')

      await expectThenClose(
        conn,
        '-ERR Protocol error: invalid multibulk length\r\n',
      )
    })

    // Accepting a count is otherwise invisible — the server just waits for the
    // elements. A non-'$' element prefix makes it observable: past the count
    // check the parser reaches the element and names the bad byte.
    test(
      'a count above 1024*1024 is accepted on 7.0+',
      {
        skip:
          activeProfile === 'redis-6.2' &&
          '6.2 caps the count at 1024*1024; see multibulk-count-gate.test.ts',
      },
      async () => {
        const conn = await connect()

        conn.write('*1048577\r\n+x\r\n')

        await expectThenClose(
          conn,
          "-ERR Protocol error: expected '$', got '+'\r\n",
        )
      },
    )
  })

  // Redis parses both lengths with `string2ll`: a canonical signed decimal
  // only. A leading zero, a `+` sign or `-0` is a protocol error, even where
  // the value would otherwise be skipped (a count <= 0) or valid.
  describe('length format', () => {
    for (const count of ['-05', '-0', '01', '00', '+1']) {
      test(`multibulk count ${count} is refused`, async () => {
        const conn = await connect()

        conn.write(`*${count}\r\n${commandFrame('PING').toString()}`)

        await expectThenClose(
          conn,
          '-ERR Protocol error: invalid multibulk length\r\n',
        )
      })
    }

    for (const length of ['01', '-0', '04', '+4']) {
      test(`bulk length ${length} is refused`, async () => {
        const conn = await connect()

        conn.write(`*1\r\n$${length}\r\nPING\r\n`)

        await expectThenClose(
          conn,
          '-ERR Protocol error: invalid bulk length\r\n',
        )
      })
    }

    // Any canonical value in int64 range parses, so a count below INT_MIN is
    // skipped like any other count <= 0.
    test('a count far below zero is still skipped', async () => {
      const conn = await connect()

      conn.write(`*-2147483649\r\n${commandFrame('PING').toString()}`)

      assert.strictEqual((await conn.readRawFrame()).toString(), '+PONG\r\n')
    })
  })

  describe('inline request cap', () => {
    test('an unterminated inline request over 64KB is refused and closed', async () => {
      const conn = await connect()

      conn.write(`PING ${'a'.repeat(70000)}`)

      await expectThenClose(conn, TOO_BIG_INLINE)
    })

    // `> PROTO_INLINE_MAX_SIZE`: one byte over is enough.
    test('the cap trips at 64KB + 1 buffered bytes with no newline', async () => {
      const conn = await connect()

      conn.write('a'.repeat(INLINE_MAX + 1))

      await expectThenClose(conn, TOO_BIG_INLINE)
    })

    // Redis checks the cap only when the buffer holds no newline at all, so an
    // inline request sitting at exactly 64KB is not refused, and a later read
    // that brings the newline gets it served — even though the line ends up
    // longer than 64KB.
    test('exactly 64KB unterminated is not refused, and is served once the newline arrives', async () => {
      const conn = await connect()
      const head = `PING ${'a'.repeat(INLINE_MAX - 5)}`
      assert.strictEqual(head.length, INLINE_MAX)

      conn.write(head)
      // Let the server read the first 64KB on its own before the tail.
      await new Promise(resolve => setTimeout(resolve, 100))
      conn.write('a\r\n')

      const expected = 'a'.repeat(INLINE_MAX - 4)
      assert.strictEqual(
        (await conn.readRawFrame()).toString(),
        `$${expected.length}\r\n${expected}\r\n`,
      )
    })

    test('commands before the oversized inline request are answered first', async () => {
      const conn = await connect()

      conn.write(`PING\r\nPING ${'a'.repeat(70000)}`)

      assert.strictEqual((await conn.readRawFrame()).toString(), '+PONG\r\n')
      await expectThenClose(conn, TOO_BIG_INLINE)
    })

    // Inline requests end at `\n`; a preceding `\r` is optional. The cap is
    // about a missing `\n`, so a bare-LF line is a complete request.
    test('an inline request may end in a bare LF', async () => {
      const conn = await connect()

      conn.write('PING hi\n')
      assert.strictEqual((await conn.readRawFrame()).toString(), '$2\r\nhi\r\n')

      conn.write(`PING\n${'a'.repeat(70000)}`)
      assert.strictEqual((await conn.readRawFrame()).toString(), '+PONG\r\n')
      await expectThenClose(conn, TOO_BIG_INLINE)
    })
  })
})
