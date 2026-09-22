import { after, before, describe, test } from 'node:test'
import assert from 'node:assert'
import { TestRunner } from '../test-config'
import { commandFrame, randomKey } from '../utils'
import { RawRedisConnection } from './raw-connection'

/**
 * Which bitmap argument errors are *runtime* errors, pinned at the wire.
 *
 * #415 moved SETBIT/GETBIT/BITFIELD validation out of the command schema and
 * into `execute`, because the bit-offset ceiling is derived from the live
 * `proto-max-bulk-len` and the schema has no access to server state. That is a
 * behaviour change with a client-visible edge: inside `MULTI`, a command that
 * fails at *queue* time dirties the transaction into `EXECABORT`, while one
 * that fails at *execution* time replies `+QUEUED` and surfaces its error as an
 * element of the `EXEC` array.
 *
 * Redis puts everything except arity in the second group. Ground-truthed
 * against redis-server 6.2.24 and 7.2.16 — byte-identical on both:
 *
 *   MULTI; GETBIT k 4294967296; EXEC -> +QUEUED, then *1 with the offset error
 *   MULTI; BITFIELD k GET u99 0; EXEC -> +QUEUED, then *1 with the type error
 *   MULTI; GETBIT k; EXEC            -> arity error at queue time, EXECABORT
 *
 * This lives in raw-tcp because `+QUEUED` is the observation: ioredis' and
 * node-redis' `multi()` are client-side pipelines, so neither can show *when*
 * the server rejected a queued command, only the aggregate outcome. Everything
 * here uses the default `proto-max-bulk-len`, so it runs against the real
 * backend too and mutates no server-wide state.
 */
const testRunner = new TestRunner()

/** The lowest bit offset the 512MB default refuses: 512MB * 8. */
const OVER_DEFAULT_OFFSET = '4294967296'
const BIT_OFFSET_ERROR = '-ERR bit offset is not an integer or out of range\r\n'
const BITFIELD_TYPE_ERROR =
  '-ERR Invalid bitfield type. Use something like i16 u8. Note that u64 is not supported but i64 is.\r\n'

describe(`Raw TCP bitmap runtime errors in MULTI (${testRunner.getBackendName()})`, () => {
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

  async function expect(
    conn: RawRedisConnection,
    args: string[],
    expected: string,
  ): Promise<void> {
    conn.write(commandFrame(...args))
    assert.strictEqual(
      (await conn.readRawFrame()).toString(),
      expected,
      args.join(' '),
    )
  }

  const queuedCases: [label: string, command: string[], error: string][] = [
    [
      'SETBIT with an out-of-range offset',
      ['SETBIT', 'bitrt:a', OVER_DEFAULT_OFFSET, '1'],
      BIT_OFFSET_ERROR,
    ],
    [
      'SETBIT with a non-numeric offset',
      ['SETBIT', 'bitrt:a', 'abc', '1'],
      BIT_OFFSET_ERROR,
    ],
    [
      'SETBIT with an invalid bit value',
      ['SETBIT', 'bitrt:a', '5', '2'],
      '-ERR bit is not an integer or out of range\r\n',
    ],
    [
      'GETBIT with an out-of-range offset',
      ['GETBIT', 'bitrt:a', OVER_DEFAULT_OFFSET],
      BIT_OFFSET_ERROR,
    ],
    [
      'BITFIELD with an invalid type',
      ['BITFIELD', 'bitrt:a', 'GET', 'u99', '0'],
      BITFIELD_TYPE_ERROR,
    ],
    [
      'BITFIELD with an out-of-range offset',
      ['BITFIELD', 'bitrt:a', 'GET', 'u8', OVER_DEFAULT_OFFSET],
      BIT_OFFSET_ERROR,
    ],
    [
      'BITFIELD with a truncated operation',
      ['BITFIELD', 'bitrt:a', 'GET', 'u8'],
      '-ERR syntax error\r\n',
    ],
  ]

  for (const [label, command, error] of queuedCases) {
    test(`${label} queues and fails at EXEC`, async () => {
      const conn = await connect()
      const key = `bitrt:${randomKey()}`
      const queued = command.map((arg, i) => (i === 1 ? key : arg))

      await expect(conn, ['MULTI'], '+OK\r\n')
      await expect(conn, queued, '+QUEUED\r\n')

      conn.write(commandFrame('EXEC'))
      assert.strictEqual(
        (await conn.readRawFrame()).toString(),
        `*1\r\n${error}`,
      )
    })
  }

  // Arity is the exception: Redis rejects it before queueing, which dirties the
  // transaction, so EXEC aborts instead of returning a one-element array.
  for (const command of [
    ['SETBIT', 'bitrt:arity'],
    ['GETBIT', 'bitrt:arity'],
    ['BITFIELD'],
  ]) {
    test(`${command[0]} with wrong arity aborts the transaction at queue time`, async () => {
      const conn = await connect()

      await expect(conn, ['MULTI'], '+OK\r\n')
      await expect(
        conn,
        command,
        `-ERR wrong number of arguments for '${command[0]!.toLowerCase()}' command\r\n`,
      )
      await expect(
        conn,
        ['EXEC'],
        '-EXECABORT Transaction discarded because of previous errors.\r\n',
      )
    })
  }

  // A runtime error does not stop the rest of the transaction — the queue runs
  // to completion and only the failing element carries the error.
  test('a failed element does not stop the transaction', async () => {
    const conn = await connect()
    const key = `bitrt:${randomKey()}`

    await expect(conn, ['MULTI'], '+OK\r\n')
    await expect(conn, ['SETBIT', key, '5', '2'], '+QUEUED\r\n')
    await expect(conn, ['SETBIT', key, '1', '1'], '+QUEUED\r\n')

    conn.write(commandFrame('EXEC'))
    assert.strictEqual(
      (await conn.readRawFrame()).toString(),
      '*2\r\n-ERR bit is not an integer or out of range\r\n:0\r\n',
    )
    await expect(conn, ['GETBIT', key, '1'], ':1\r\n')
    await expect(conn, ['DEL', key], ':1\r\n')
  })

  // BITFIELD_RO's GET-only restriction is a second pass in Redis, applied only
  // once every operation has parsed — so a malformed op reports its own error
  // first and only a well-formed non-GET op reaches the RO error.
  test('BITFIELD_RO reports parse errors before the GET-only restriction', async () => {
    const conn = await connect()
    const key = `bitrt:${randomKey()}`

    await expect(
      conn,
      ['BITFIELD_RO', key, 'SET', 'u8', OVER_DEFAULT_OFFSET, '1'],
      BIT_OFFSET_ERROR,
    )
    await expect(
      conn,
      ['BITFIELD_RO', key, 'SET', 'u99', '0', '1'],
      BITFIELD_TYPE_ERROR,
    )
    await expect(
      conn,
      ['BITFIELD_RO', key, 'SET', 'u8', '0', 'notanint'],
      '-ERR value is not an integer or out of range\r\n',
    )
    await expect(conn, ['BITFIELD_RO', key, 'NOPE'], '-ERR syntax error\r\n')
    await expect(
      conn,
      ['BITFIELD_RO', key, 'SET', 'u8', '0', '1', 'GET', 'u8', '0'],
      '-ERR BITFIELD_RO only supports the GET subcommand\r\n',
    )
    // A well-formed GET-only list still works, OVERFLOW included.
    await expect(
      conn,
      ['BITFIELD_RO', key, 'OVERFLOW', 'SAT', 'GET', 'u8', '0'],
      '*1\r\n:0\r\n',
    )
  })

  // Redis checks that enough arguments remain for a subcommand *before* it
  // reads any of them, so a short op is a syntax error however bad its type
  // token is. Only once the op is long enough does the type get parsed.
  // Verified on redis 6.2.24, 7.2.16 and 8.0.6.
  test('a BITFIELD op missing arguments is a syntax error before its type is read', async () => {
    const conn = await connect()
    const key = `bitrt:${randomKey()}`

    for (const ops of [
      ['GET', 'x9'],
      ['SET', 'x9', '0'],
      ['INCRBY', 'x9', '0'],
      ['GET', 'u8', '0', 'SET', 'x9'],
      ['OVERFLOW'],
      ['GET', 'u8'],
      ['SET', 'u8', '0'],
    ]) {
      await expect(conn, ['BITFIELD', key, ...ops], '-ERR syntax error\r\n')
    }
    await expect(
      conn,
      ['BITFIELD_RO', key, 'GET', 'x9'],
      '-ERR syntax error\r\n',
    )

    // Long enough: now the type is parsed, and rejected.
    await expect(conn, ['BITFIELD', key, 'GET', 'x9', '0'], BITFIELD_TYPE_ERROR)
    await expect(
      conn,
      ['BITFIELD', key, 'SET', 'x9', '0', '1'],
      BITFIELD_TYPE_ERROR,
    )
  })
})
