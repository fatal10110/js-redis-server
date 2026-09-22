import { after, before, describe, test } from 'node:test'
import assert from 'node:assert'
import { TestRunner } from '../test-config'
import { commandFrame, randomKey } from '../utils'
import { RawRedisConnection } from './raw-connection'

/**
 * Raw TCP coverage for `proto-max-bulk-len` at the *protocol* layer (#415).
 *
 * This is real Redis' primary enforcement point: every bulk argument longer
 * than the live limit is rejected while the multibulk frame is still being
 * parsed — before any command handler runs — so one check covers `SET`,
 * `GETSET`, `MSET`, `LPUSH`, `HSET` and everything else. The reply is a
 * protocol error and the server then *hangs up*, which is why this lives here
 * rather than behind a client: a closed connection is not something ioredis /
 * node-redis can assert without wedging on their own reconnect-and-retry.
 *
 * Ground-truthed against redis-server 6.2.24, 7.2.16 and 8.0.6 — identical on
 * all three, so no compatibility gate applies:
 *
 *   $1048576 with proto-max-bulk-len 1048576  -> accepted (waits for payload)
 *   $1048577 with proto-max-bulk-len 1048576  -> -ERR Protocol error: invalid
 *                                                bulk length, then close
 */
const testRunner = new TestRunner()

/** Redis' compiled-in default for `proto-max-bulk-len`: 512MB. */
const DEFAULT_PROTO_MAX_BULK_LEN = 536870912
/** One byte past the default limit — the smallest length Redis refuses. */
const OVER_DEFAULT_LIMIT = DEFAULT_PROTO_MAX_BULK_LEN + 1
const INVALID_BULK_LENGTH = '-ERR Protocol error: invalid bulk length\r\n'

/**
 * A multibulk frame whose final argument is *only* a bulk header claiming
 * `length` bytes; the payload is deliberately never written.
 *
 * Real Redis refuses the header before it reads (or allocates room for) a
 * single byte of the value, so this both matches its behaviour and keeps the
 * test from moving 512MB across a socket. Every case below that runs against a
 * real server uses this form for exactly that reason.
 */
function frameWithOversizedTail(length: number, ...leading: string[]): Buffer {
  const head = leading
    .map(arg => `$${Buffer.byteLength(arg)}\r\n${arg}\r\n`)
    .join('')
  return Buffer.from(`*${leading.length + 1}\r\n${head}$${length}\r\n`)
}

/** `APPEND <key> <value>` as a real frame, with a Buffer value. */
function appendFrame(key: string, value: Buffer): Buffer {
  return Buffer.concat([
    Buffer.from(
      `*3\r\n$6\r\nAPPEND\r\n$${Buffer.byteLength(key)}\r\n${key}\r\n`,
    ),
    Buffer.from(`$${value.length}\r\n`),
    value,
    Buffer.from('\r\n'),
  ])
}

/** How long to wait for the server to hang up before calling it a failure. */
const CLOSE_TIMEOUT_MS = 15000

/**
 * Assert the connection received exactly `expected` and was then closed by the
 * server.
 *
 * `readUntilClose()` waits forever, and the pre-fix behaviour is precisely "the
 * server answers and keeps the connection open" — so without a bound the
 * regression case hangs the runner instead of failing it. The race turns that
 * into a normal assertion failure.
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

describe(`Raw TCP proto-max-bulk-len protocol errors (${testRunner.getBackendName()})`, () => {
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

  // The whole point of the protocol-layer check: it is not a per-command guard.
  // APPEND and SETRANGE already refused an oversized *result* at the command
  // layer; none of these commands did, and all of them must now.
  const oversizedCases: [label: string, leading: string[]][] = [
    ['SET', ['SET', 'pmbl:set']],
    ['GETSET', ['GETSET', 'pmbl:getset']],
    ['SETEX', ['SETEX', 'pmbl:setex', '100']],
    ['MSET', ['MSET', 'pmbl:mset']],
    ['LPUSH', ['LPUSH', 'pmbl:lpush']],
    ['HSET', ['HSET', 'pmbl:hset', 'field']],
    [
      'APPEND (fresh key — never size-checked at the command layer)',
      ['APPEND', 'pmbl:append'],
    ],
    ['ECHO (no key at all)', ['ECHO']],
  ]

  for (const [label, leading] of oversizedCases) {
    test(`${label} rejects an oversized bulk argument and closes the connection`, async () => {
      const conn = await connect()

      conn.write(frameWithOversizedTail(OVER_DEFAULT_LIMIT, ...leading))

      await expectThenClose(conn, INVALID_BULK_LENGTH)
    })
  }

  test('an oversized command-name bulk is refused the same way', async () => {
    const conn = await connect()

    conn.write(Buffer.from(`*1\r\n$${OVER_DEFAULT_LIMIT}\r\n`))

    await expectThenClose(conn, INVALID_BULK_LENGTH)
  })

  test('valid pipelined commands are answered before the protocol error', async () => {
    const conn = await connect()
    const key = `pmbl:${randomKey()}`

    // One write: two good commands, then a frame whose bulk header is too long.
    conn.write(
      Buffer.concat([
        commandFrame('SET', key, 'value'),
        commandFrame('GET', key),
        frameWithOversizedTail(OVER_DEFAULT_LIMIT, 'SET', key),
      ]),
    )

    assert.strictEqual((await conn.readRawFrame()).toString(), '+OK\r\n')
    assert.strictEqual(
      (await conn.readRawFrame()).toString(),
      '$5\r\nvalue\r\n',
    )
    await expectThenClose(conn, INVALID_BULK_LENGTH)
  })

  test('the command behind an oversized argument never runs', async () => {
    const key = `pmbl:${randomKey()}`

    const doomed = await connect()
    doomed.write(frameWithOversizedTail(OVER_DEFAULT_LIMIT, 'SET', key))
    await expectThenClose(doomed, INVALID_BULK_LENGTH)

    const survivor = await connect()
    survivor.write(commandFrame('EXISTS', key))
    assert.strictEqual((await survivor.readRawFrame()).toString(), ':0\r\n')
  })

  test('an inline command is not subject to the bulk limit', async () => {
    const conn = await connect()

    conn.write(Buffer.from('PING\r\n'))

    assert.strictEqual((await conn.readRawFrame()).toString(), '+PONG\r\n')
  })
})

describe(
  `Raw TCP proto-max-bulk-len protocol errors follow the live limit (${testRunner.getBackendName()})`,
  {
    skip:
      testRunner.backend === 'real' &&
      'lowers proto-max-bulk-len server-wide; mock backend only',
  },
  () => {
    let port: number
    const connections: RawRedisConnection[] = []
    const LOWERED = 1048576

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

    async function setLimit(value: number | string): Promise<void> {
      const conn = await connect()
      conn.write(
        commandFrame('CONFIG', 'SET', 'proto-max-bulk-len', String(value)),
      )
      assert.strictEqual((await conn.readRawFrame()).toString(), '+OK\r\n')
    }

    // The issue's exact repro: with the limit at 1MB a 2MB APPEND to a *fresh*
    // key is a protocol error on every real version, even though the command
    // layer deliberately does not size-check APPEND on a missing key.
    //
    // Only the header is written, because that is all the server ever sees: it
    // answers and hangs up on the header, so a client that kept pushing the
    // 2MB payload would simply race the close and take an EPIPE — which is what
    // the issue's transcript shows as "connection closed".
    test('a 2MB argument is a protocol error once the limit is 1MB (#415)', async () => {
      await setLimit(LOWERED)
      const key = `pmbl:live:${randomKey()}`

      try {
        const conn = await connect()
        conn.write(frameWithOversizedTail(2 * LOWERED, 'APPEND', key))

        await expectThenClose(conn, INVALID_BULK_LENGTH)

        const survivor = await connect()
        survivor.write(commandFrame('EXISTS', key))
        assert.strictEqual((await survivor.readRawFrame()).toString(), ':0\r\n')
      } finally {
        await setLimit(DEFAULT_PROTO_MAX_BULK_LEN)
      }
    })

    // `> limit`, not `>=`: a bulk exactly the size of the limit is accepted,
    // payload and all. Pinned here rather than in the always-on suite because
    // proving it at the 512MB default would mean a 512MB argument.
    test('a bulk exactly the size of the limit is accepted', async () => {
      await setLimit(LOWERED)
      const key = `pmbl:live:${randomKey()}`

      try {
        const conn = await connect()
        conn.write(appendFrame(key, Buffer.alloc(LOWERED, 0x78)))
        assert.strictEqual(
          (await conn.readRawFrame()).toString(),
          `:${LOWERED}\r\n`,
        )

        // One byte more is refused, so the boundary is pinned from both sides.
        const over = await connect()
        over.write(
          frameWithOversizedTail(LOWERED + 1, 'APPEND', `pmbl:live:over`),
        )
        await expectThenClose(over, INVALID_BULK_LENGTH)
      } finally {
        await setLimit(DEFAULT_PROTO_MAX_BULK_LEN)
      }
    })

    // Raising the limit must move the ceiling with it — the check reads the
    // live setting rather than a snapshot taken when the connection opened.
    test('raising the limit admits what the lower limit refused', async () => {
      const key = `pmbl:live:${randomKey()}`
      const value = Buffer.alloc(2 * LOWERED, 0x79)

      await setLimit(LOWERED)
      try {
        const refused = await connect()
        refused.write(frameWithOversizedTail(value.length, 'APPEND', key))
        await expectThenClose(refused, INVALID_BULK_LENGTH)

        await setLimit(DEFAULT_PROTO_MAX_BULK_LEN)

        const accepted = await connect()
        accepted.write(appendFrame(key, value))
        assert.strictEqual(
          (await accepted.readRawFrame()).toString(),
          `:${value.length}\r\n`,
        )
        accepted.write(commandFrame('DEL', key))
        assert.strictEqual((await accepted.readRawFrame()).toString(), ':1\r\n')
      } finally {
        await setLimit(DEFAULT_PROTO_MAX_BULK_LEN)
      }
    })
  },
)
