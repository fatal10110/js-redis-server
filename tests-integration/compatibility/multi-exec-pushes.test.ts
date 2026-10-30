import { after, before, describe, test } from 'node:test'
import assert from 'node:assert'

import { TestRunner } from '../test-config'
import { activeProfile, commandFrame, randomKey } from '../utils'
import { RawRedisConnection, respText } from '../raw-tcp/raw-connection'
import { expectReply } from '../raw-tcp/helpers'

/**
 * Profile-dependent MULTI / EXEC rows of #567 and #509.
 *
 * QUIT inside MULTI is answered and closes on every version, but only Redis
 * 7.0+ (valkey 7.2+) feeds it to MONITOR: 6.2 answers QUIT in the connection
 * loop, before call(), so it never shows up there, in MULTI or not.
 *
 * A push for the calling connection produced by a command inside EXEC (its
 * own PUBLISH to a channel it subscribes to): Redis 7.2+ and valkey 8.0+ hold
 * it until the EXEC reply is out (`server.pending_push_messages`); 6.2 and 7.0
 * write it into the output buffer as it happens, inside the EXEC array, ahead
 * of the item that produced it. Checked against redis-server 6.2.14, 7.0.15,
 * 7.2.4, 7.4.4, 8.0.0 and valkey-server 8.0.0, 9.0.0.
 */
const testRunner = new TestRunner()
const profile = activeProfile
const quitFeedsMonitor = profile !== 'redis-6.2'
const pushesAfterReply = !['redis-6.2', 'redis-7.0'].includes(profile)

const bulk = (value: string) => `$${Buffer.byteLength(value)}\r\n${value}\r\n`
const pushType = (resp: 2 | 3) => (resp === 3 ? '>' : '*')
const pushFrame = (resp: 2 | 3, ...items: string[]) =>
  `${pushType(resp)}${items.length}\r\n${items.map(bulk).join('')}`
const confirmation = (resp: 2 | 3, channel: string, count: number) =>
  `${pushType(resp)}3\r\n${bulk('subscribe')}${bulk(channel)}:${count}\r\n`

/** Read whole frames until `expected.length` bytes arrived; compare bytes. */
async function expectBytes(
  conn: RawRedisConnection,
  expected: string,
): Promise<void> {
  const chunks: Buffer[] = []
  let length = 0
  while (length < Buffer.byteLength(expected)) {
    const chunk = await conn.readRawFrame()
    chunks.push(chunk)
    length += chunk.length
  }
  assert.strictEqual(Buffer.concat(chunks).toString('latin1'), expected)
}

/** The argv of a MONITOR line, `<ts> [<db> <addr>] "a" "b"`. */
function monitorArgv(line: string): string[] {
  const argv = line.slice(line.indexOf('] ') + 2)
  return [...argv.matchAll(/"((?:[^"\\]|\\.)*)"/g)].map(match => match[1])
}

describe(`MULTI / EXEC profile rows (${testRunner.getBackendName()}, ${profile})`, () => {
  let port: number
  const connections: RawRedisConnection[] = []

  const connect = async () => {
    const conn = await RawRedisConnection.connect('127.0.0.1', port)
    connections.push(conn)
    return conn
  }

  before(async () => {
    port = await testRunner.setupRawStandalone()
  })

  after(async () => {
    for (const conn of connections) {
      conn.close()
    }
    await testRunner.cleanup()
  })

  test('QUIT feeds MONITOR like the profile, in MULTI and out of it', async () => {
    const monitor = await connect()
    const inMulti = await connect()
    const plain = await connect()
    const sentinel = await connect()
    const key = `multi-quit:${randomKey()}`

    // Other files may share the server: keep only this test's clients' lines.
    const addresses = new Set<string>()
    for (const conn of [inMulti, plain, sentinel]) {
      conn.write(commandFrame('CLIENT', 'INFO'))
      const info = respText(await conn.readFrame())
      addresses.add(/(?:^| )addr=(\S+)/.exec(info)?.[1] ?? '')
    }

    await expectReply(monitor, ['MONITOR'], '+OK\r\n')

    await expectReply(inMulti, ['MULTI'], '+OK\r\n')
    await expectReply(inMulti, ['SET', key, '1'], '+QUEUED\r\n')
    inMulti.write(commandFrame('QUIT', 'x'))
    assert.strictEqual((await inMulti.readUntilClose()).toString(), '+OK\r\n')

    plain.write(commandFrame('QUIT'))
    assert.strictEqual((await plain.readUntilClose()).toString(), '+OK\r\n')

    await expectReply(sentinel, ['ECHO', key], bulk(key))

    const lines: string[][] = []
    for (;;) {
      const line = respText(await monitor.readFrame())
      const address = /\[\d+ (\S+)\]/.exec(line)?.[1] ?? ''
      if (!addresses.has(address)) {
        continue
      }
      const argv = monitorArgv(line)
      lines.push(argv)
      if (argv[0] === 'ECHO') {
        break
      }
    }

    assert.deepStrictEqual(lines, [
      ['MULTI'],
      ...(quitFeedsMonitor ? [['QUIT', 'x'], ['QUIT']] : []),
      ['ECHO', key],
    ])
  })

  test('RESP3 subscribed PUBLISH queued in MULTI: self message vs EXEC reply', async () => {
    const conn = await connect()
    const channel = `multi-pub:${randomKey()}`
    const message = (text: string) => pushFrame(3, 'message', channel, text)

    conn.write(commandFrame('HELLO', '3'))
    assert.ok((await conn.readFrame()) instanceof Map)
    await expectReply(conn, ['SUBSCRIBE', channel], confirmation(3, channel, 1))

    await expectReply(conn, ['MULTI'], '+OK\r\n')
    await expectReply(conn, ['PUBLISH', channel, 'x'], '+QUEUED\r\n')
    await expectReply(conn, ['PUBLISH', channel, 'y'], '+QUEUED\r\n')
    conn.write(commandFrame('EXEC'))
    await expectBytes(
      conn,
      pushesAfterReply
        ? `*2\r\n:1\r\n:1\r\n${message('x')}${message('y')}`
        : `*2\r\n${message('x')}:1\r\n${message('y')}:1\r\n`,
    )
    await expectReply(conn, ['PING'], '+PONG\r\n')

    // The connection is still subscribed after EXEC and after DISCARD.
    conn.write(commandFrame('PUBLISH', channel, 'z'))
    await expectBytes(
      conn,
      pushesAfterReply ? `:1\r\n${message('z')}` : `${message('z')}:1\r\n`,
    )
    await expectReply(conn, ['MULTI'], '+OK\r\n')
    await expectReply(conn, ['DISCARD'], '+OK\r\n')
    conn.write(commandFrame('PUBLISH', channel, 'w'))
    await expectBytes(
      conn,
      pushesAfterReply ? `:1\r\n${message('w')}` : `${message('w')}:1\r\n`,
    )
  })

  for (const resp of [2, 3] as const) {
    test(`RESP${resp} MULTI; SUBSCRIBE a b; PUBLISH a; EXEC delivers every frame`, async () => {
      const conn = await connect()
      const a = `multi-sub:${randomKey()}:a`
      const b = `multi-sub:${randomKey()}:b`

      if (resp === 3) {
        conn.write(commandFrame('HELLO', '3'))
        assert.ok((await conn.readFrame()) instanceof Map)
      }

      await expectReply(conn, ['MULTI'], '+OK\r\n')
      await expectReply(conn, ['SUBSCRIBE', a, b], '+QUEUED\r\n')
      await expectReply(conn, ['PUBLISH', a, 'x'], '+QUEUED\r\n')
      conn.write(commandFrame('EXEC'))
      const subscribed = confirmation(resp, a, 1) + confirmation(resp, b, 2)
      const message = pushFrame(resp, 'message', a, 'x')
      await expectBytes(
        conn,
        pushesAfterReply
          ? `*2\r\n${subscribed}:1\r\n${message}`
          : `*2\r\n${subscribed}${message}:1\r\n`,
      )

      await expectReply(
        conn,
        ['PING'],
        resp === 3 ? '+PONG\r\n' : `*2\r\n${bulk('pong')}${bulk('')}`,
      )
    })
  }
})
