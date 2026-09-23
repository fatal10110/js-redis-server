import { after, before, describe, test } from 'node:test'
import assert from 'node:assert'
import { TestRunner } from '../test-config'
import { commandFrame, randomKey } from '../utils'
import { RawRedisConnection } from './raw-connection'

/**
 * Raw TCP coverage for the two `proto-max-bulk-len` behaviours whose failure
 * mode is "the server hangs up" rather than "the server replies with an error".
 *
 * Both belong here rather than behind a client, and for the same reason: when
 * the server drops the connection mid-command, ioredis re-queues the in-flight
 * write across its reconnect, the server drops it again, and the promise never
 * settles — so a client-driven version does not fail, it hangs until CI kills
 * the run. Both were measured doing exactly that before landing here.
 * `RawRedisConnection` observes the close directly (`readRawFrame` rejects with
 * "connection closed before a complete frame arrived"), which makes the test
 * fail in milliseconds, and lets the error frame and a following `+PONG` be
 * read off the same connection so liveness is actually asserted.
 */
const testRunner = new TestRunner()

const DEFAULT_PROTO_MAX_BULK_LEN = '536870912'
/** The mock's own ceiling — see MAX_MATERIALISABLE_LENGTH in src/commands/strings.ts. */
const MAX_MATERIALISABLE_LENGTH = 536870912
/** Node's buffer.constants.MAX_LENGTH: accepted as an argument, not servable. */
const BUFFER_MAX_LENGTH = 9007199254740991

describe(`Raw TCP CONFIG SET proto-max-bulk-len parsing (${testRunner.getBackendName()})`, () => {
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

  // The unit suffix comes straight off the wire. With an object-literal lookup
  // `constructor` resolves through the prototype chain to `Object` — already
  // lower-case, so the `toLowerCase()` does not stop it — and `0n * Object`
  // throws a TypeError, which is not a RedisCommandError and so drops the
  // connection instead of answering. Runs against real Redis too: every
  // version answers the ordinary memory-value error.
  test('rejects a prototype key as a memory-unit suffix instead of hanging up', async () => {
    const conn = await RawRedisConnection.connect('127.0.0.1', port)
    connections.push(conn)

    for (const value of [
      'constructor',
      '5constructor',
      'CONSTRUCTOR',
      'toString',
      'valueOf',
      'hasOwnProperty',
      '__proto__',
    ]) {
      conn.write(commandFrame('CONFIG', 'SET', 'proto-max-bulk-len', value))
      let reply: string
      try {
        reply = (await conn.readRawFrame()).toString()
      } catch (error) {
        assert.fail(
          `CONFIG SET proto-max-bulk-len ${value} closed the connection: ${String(error)}`,
        )
      }
      assert.match(
        reply,
        /^-ERR .*argument must be a memory value\r\n$/,
        `CONFIG SET proto-max-bulk-len ${value}`,
      )
    }

    conn.write(commandFrame('PING'))
    assert.strictEqual((await conn.readRawFrame()).toString(), '+PONG\r\n')
  })
})

describe(
  `Raw TCP proto-max-bulk-len allocation ceiling (${testRunner.getBackendName()})`,
  {
    skip:
      testRunner.backend === 'real' &&
      'raises proto-max-bulk-len server-wide; mock backend only',
  },
  () => {
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

    /**
     * Send SETRANGE at `offset` and return the raw reply, or null if the server
     * hung up without answering. Never waits on a client retry, so a
     * reintroduced crash fails the assertion instead of wedging the run.
     */
    async function setrangeAt(
      conn: RawRedisConnection,
      offset: string,
    ): Promise<string | null> {
      conn.write(
        commandFrame('SETRANGE', `a383:alloc:${randomKey()}`, offset, 'xx'),
      )
      try {
        return (await conn.readRawFrame()).toString()
      } catch {
        return null
      }
    }

    /**
     * Offsets that must be refused with the size error rather than allocated.
     *
     * Order matters, and the two groups pin different things:
     *
     * 1. The **ceiling** row goes first, and is the only one here that pins
     *    `MAX_MATERIALISABLE_LENGTH`. If the ceiling is raised or removed, the
     *    guard admits it, `Buffer.alloc` succeeds (512MB — a real but bounded
     *    allocation), and the reply is `:536870913` instead of the error, so
     *    this row fails immediately and the rest never run.
     * 2. The rows after it sit in the band where `Buffer.alloc` *throws*, so
     *    they pin the `allocateStringBuffer` backstop, not the ceiling — with
     *    the ceiling reverted but the backstop kept, all of them stay green.
     *    That is deliberate and worth stating, because it is easy to read the
     *    whole array as ceiling coverage when only the first row is.
     *
     * A lazily-mapped terabyte (offset 1000000000000) is deliberately **not**
     * here. It is the one input that distinguishes the ceiling end to end, but
     * `Buffer.alloc` accepts it and the process is then SIGKILLed during
     * zero-fill — so on a regression it would take the runner down instead of
     * failing an assertion, and no ordering changes that. The invariant it
     * stood for is pinned allocation-free in tests/commands-strings-limits.test.ts.
     */
    const offsets: [label: string, offset: string][] = [
      [
        'just past the materialisable ceiling',
        String(MAX_MATERIALISABLE_LENGTH - 1),
      ],
      ['one past buffer.constants.MAX_LENGTH', String(BUFFER_MAX_LENGTH - 1)],
      ['exactly buffer.constants.MAX_LENGTH', String(BUFFER_MAX_LENGTH - 2)],
      ['inside the unallocatable band', '9007199254740000'],
      ['inside the unallocatable band, lower', '8000000000000000'],
    ]

    test('SETRANGE answers the size error and keeps the connection alive at every unservable offset', async () => {
      const conn = await connect()

      conn.write(
        commandFrame(
          'CONFIG',
          'SET',
          'proto-max-bulk-len',
          '9223372036854775807',
        ),
      )
      assert.strictEqual((await conn.readRawFrame()).toString(), '+OK\r\n')

      try {
        for (const [label, offset] of offsets) {
          assert.strictEqual(
            await setrangeAt(conn, offset),
            '-ERR string exceeds maximum allowed size (proto-max-bulk-len)\r\n',
            `SETRANGE at ${label} (offset ${offset})`,
          )

          // Liveness on the same connection, read as the next frame — this is
          // what a client-driven test cannot assert, because it never gets far
          // enough to send it.
          conn.write(commandFrame('PING'))
          assert.strictEqual(
            (await conn.readRawFrame()).toString(),
            '+PONG\r\n',
            `connection still usable after ${label}`,
          )
        }
      } finally {
        conn.write(
          commandFrame(
            'CONFIG',
            'SET',
            'proto-max-bulk-len',
            DEFAULT_PROTO_MAX_BULK_LEN,
          ),
        )
        await conn.readRawFrame().catch(() => undefined)
      }
    })

    test('SETRANGE still allows a write up to the materialisable ceiling', async () => {
      const conn = await connect()
      const key = `a383:alloc:${randomKey()}`

      // Two bytes landing exactly on the ceiling: allowed, and actually
      // allocated, so the ceiling is a size the process can really produce.
      conn.write(
        commandFrame(
          'SETRANGE',
          key,
          String(MAX_MATERIALISABLE_LENGTH - 2),
          'xx',
        ),
      )
      assert.strictEqual(
        (await conn.readRawFrame()).toString(),
        `:${MAX_MATERIALISABLE_LENGTH}\r\n`,
      )

      conn.write(commandFrame('DEL', key))
      assert.strictEqual((await conn.readRawFrame()).toString(), ':1\r\n')
    })
  },
)
