import { after, before, describe, test } from 'node:test'
import { TestRunner } from '../test-config'
import { activeProfile } from '../utils'
import { RawRedisConnection } from './raw-connection'
import { expectReply } from './helpers'

/**
 * The unknown-command error echoes what the client sent, so its exact bytes
 * are pinned here rather than through a client (#384). Real Redis builds it
 * with C `printf`. Redis 7.0+ and every Valkey print
 *
 *     unknown command '%.128s', with args beginning with: %s
 *
 * and grow the args part by `'%.*s' ` per argument. Redis 6.2 prints
 *
 *     unknown command `%s`, with args beginning with: %s
 *
 * and grows it by `` `%.*s`, `` (no cap on the name). Either way the
 * precision is `128 - <bytes so far>` and args are added while the part is
 * under 128 bytes. So the name and args are echoed as raw bytes (never
 * hex-dumped), each is cut at its first NUL (`%s` is a C string), the cut is
 * by byte and may split a UTF-8 sequence, and there is no argument-count cap,
 * only the 128-byte budget, which the quotes and separators count against.
 * CR and LF become spaces, as in every error reply.
 *
 * Profile-aware: run with `REDIS_COMPAT=redis-6.2` against a real 6.2 (or the
 * mock on that profile) for the legacy shape.
 */
const testRunner = new TestRunner()

const legacy = activeProfile === 'redis-6.2'
const Q = legacy ? '`' : "'"
const SEP = legacy ? ', ' : ' '
/** Bytes an echoed arg costs on top of its own: two quotes and a separator. */
const COST = 2 + SEP.length

const HEAD = `-ERR unknown command ${Q}`
const MID = `${Q}, with args beginning with: `
const echo = (...args: string[]) =>
  args.map(arg => `${Q}${arg}${Q}${SEP}`).join('')

describe(`Raw TCP unknown command error (${testRunner.getBackendName()}, ${activeProfile})`, () => {
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

  test('a non-UTF-8 command name is echoed as raw bytes', async () => {
    const conn = await connect()
    const name = Buffer.from([0x66, 0xff, 0xfe, 0x6f])

    await expectReply(
      conn,
      [name],
      Buffer.concat([Buffer.from(HEAD), name, Buffer.from(`${MID}\r\n`)]),
    )
  })

  test('non-UTF-8 args are echoed raw, each cut at its first NUL', async () => {
    const conn = await connect()

    await expectReply(
      conn,
      [
        'nosuchcmd',
        Buffer.from([0xc3, 0x28]),
        Buffer.from([0x00, 0x41]),
        Buffer.from([0x62, 0x00, 0x63]),
      ],
      Buffer.concat([
        Buffer.from(`${HEAD}nosuchcmd${MID}${Q}`),
        Buffer.from([0xc3, 0x28]),
        Buffer.from(`${Q}${SEP}${echo('', 'b')}\r\n`),
      ]),
    )
  })

  test('a command name is cut at its first NUL', async () => {
    const conn = await connect()

    await expectReply(
      conn,
      [Buffer.from([0x61, 0x00, 0x62]), 'x'],
      `${HEAD}a${MID}${echo('x')}\r\n`,
    )
  })

  test('CR and LF in the name and args become spaces', async () => {
    const conn = await connect()

    await expectReply(
      conn,
      ['a\r\nb', 'x\ny'],
      `${HEAD}a  b${MID}${echo('x y')}\r\n`,
    )
  })

  test('a very long command name is cut to 128 bytes (echoed whole on 6.2)', async () => {
    const conn = await connect()

    await expectReply(
      conn,
      ['x'.repeat(200)],
      `${HEAD}${'x'.repeat(legacy ? 200 : 128)}${MID}\r\n`,
    )
  })

  test('a very long arg is cut to the 128-byte budget and ends the list', async () => {
    const conn = await connect()

    await expectReply(
      conn,
      ['nosuchcmd', 'y'.repeat(200), 'z'],
      `${HEAD}nosuchcmd${MID}${echo('y'.repeat(128))}\r\n`,
    )
  })

  test('the last arg that fits is cut to what is left of the budget', async () => {
    const conn = await connect()

    // Two 59-byte args use 2 * (59 + COST) bytes, leaving 4 (7.0+) or 2 (6.2)
    // bytes of precision for the third.
    const left = 128 - 2 * (59 + COST)
    await expectReply(
      conn,
      ['nosuchcmd', 'a'.repeat(59), 'b'.repeat(59), 'c'.repeat(60)],
      `${HEAD}nosuchcmd${MID}${echo('a'.repeat(59), 'b'.repeat(59), 'c'.repeat(left))}\r\n`,
    )
  })

  test('the budget is checked before each arg: one byte decides whether the next arg is echoed', async () => {
    const conn = await connect()

    // After an arg of `keep` bytes, 127 bytes are used, still under 128, so
    // 'b' goes in (it fits the 1 byte of precision left). One byte longer and
    // the budget is spent. 124 / 125 on 7.0+, 123 / 124 on 6.2.
    const keep = 128 - COST - 1
    await expectReply(
      conn,
      ['nosuchcmd', 'a'.repeat(keep), 'b', 'c'],
      `${HEAD}nosuchcmd${MID}${echo('a'.repeat(keep), 'b')}\r\n`,
    )
    await expectReply(
      conn,
      ['nosuchcmd', 'a'.repeat(keep + 1), 'b', 'c'],
      `${HEAD}nosuchcmd${MID}${echo('a'.repeat(keep + 1))}\r\n`,
    )
  })

  test('the cut is by byte and can split a UTF-8 sequence', async () => {
    const conn = await connect()

    // A first arg sized so the precision left for the second is odd (123 on
    // 7.0+, 121 on 6.2): the 'é's that fit whole, plus the first byte of the
    // next one.
    const first = legacy ? 'abc' : 'ab'
    const precision = 128 - (first.length + COST)
    await expectReply(
      conn,
      ['nosuchcmd', first, 'é'.repeat(100)],
      Buffer.concat([
        Buffer.from(
          `${HEAD}nosuchcmd${MID}${echo(first)}${Q}${'é'.repeat((precision - 1) / 2)}`,
        ),
        Buffer.from([0xc3]),
        Buffer.from(`${Q}${SEP}\r\n`),
      ]),
    )
  })

  test('there is no arg-count cap, only the byte budget', async () => {
    const conn = await connect()
    const args = Array.from({ length: 30 }, (_, i) => `a${i}`)

    // 7.0+: 10 * 5 + 13 * 6 = 128 bytes, a0 .. a22. 6.2 spends one byte more
    // per arg: 10 * 6 + 10 * 7 = 130, a0 .. a19.
    const echoed = args.slice(0, legacy ? 20 : 23)
    await expectReply(
      conn,
      ['nosuchcmd', ...args],
      `${HEAD}nosuchcmd${MID}${echo(...echoed)}\r\n`,
    )
  })

  test('no args and an empty name', async () => {
    const conn = await connect()

    await expectReply(conn, ['nosuchcmd'], `${HEAD}nosuchcmd${MID}\r\n`)
    await expectReply(conn, [''], `${HEAD}${MID}\r\n`)
  })
})
