import { after, before, describe, test } from 'node:test'
import assert from 'node:assert'
import { TestRunner } from '../test-config'
import { activeProfile, commandFrame } from '../utils'
import { RawRedisConnection } from './raw-connection'

const testRunner = new TestRunner()

// Valkey 8.0.0 and 8.0.1 refuse (P|S)UNSUBSCRIBE on a connection with no
// subscription of any kind, in RESP2 and RESP3 and at MULTI queue time
// (valkey@8.0.0 server.c:4076-4081, `-NOSUB`); 8.0.2 dropped the check and
// answers the confirmation frames like every Redis (#594). The valkey-8.0
// profile pins 8.0.0. Checked against valkey-server 8.0.0, 8.0.1, 8.0.2,
// 9.0.0 and redis-server 6.2.14 to 8.0.0.
const refuses = activeProfile === 'valkey-8.0'
// SUNSUBSCRIBE is Redis 7.0+.
const kinds =
  activeProfile === 'redis-6.2'
    ? ['unsubscribe', 'punsubscribe']
    : ['unsubscribe', 'punsubscribe', 'sunsubscribe']

const bulk = (value: string) => `$${Buffer.byteLength(value)}\r\n${value}\r\n`
const nosub = (name: string) =>
  `-NOSUB '${name}' command executed not in subscribed mode\r\n`
const confirmation = (
  resp: 2 | 3,
  name: string,
  target: string | null,
  count: number,
) =>
  `${resp === 3 ? '>' : '*'}3\r\n${bulk(name)}${
    target === null ? (resp === 3 ? '_\r\n' : '$-1\r\n') : bulk(target)
  }:${count}\r\n`

describe(`Raw TCP (P|S)UNSUBSCRIBE outside subscribed mode (${testRunner.getBackendName()}, ${activeProfile})`, () => {
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

  async function connect(resp: 2 | 3): Promise<RawRedisConnection> {
    const conn = await RawRedisConnection.connect('127.0.0.1', port)
    connections.push(conn)
    if (resp === 3) {
      conn.write(commandFrame('HELLO', '3'))
      await conn.readFrame()
    }
    return conn
  }

  async function send(conn: RawRedisConnection, ...args: string[]) {
    conn.write(commandFrame(...args))
    return (await conn.readRawFrame()).toString('latin1')
  }

  for (const resp of [2, 3] as const) {
    test(`RESP${resp}: bare and with arguments, never subscribed`, async () => {
      const conn = await connect(resp)
      for (const name of kinds) {
        assert.strictEqual(
          await send(conn, name.toUpperCase()),
          refuses ? nosub(name) : confirmation(resp, name, null, 0),
          name,
        )
        conn.write(commandFrame(name.toUpperCase(), 'a', 'b'))
        if (refuses) {
          assert.strictEqual(
            (await conn.readRawFrame()).toString('latin1'),
            nosub(name),
          )
        } else {
          for (const target of ['a', 'b']) {
            assert.strictEqual(
              (await conn.readRawFrame()).toString('latin1'),
              confirmation(resp, name, target, 0),
            )
          }
        }
      }
      assert.strictEqual(await send(conn, 'PING'), '+PONG\r\n')
    })
  }

  test('inside MULTI: refused at queue time, EXEC aborts', async () => {
    const conn = await connect(2)
    assert.strictEqual(await send(conn, 'MULTI'), '+OK\r\n')
    assert.strictEqual(
      await send(conn, 'UNSUBSCRIBE'),
      refuses ? nosub('unsubscribe') : '+QUEUED\r\n',
    )
    assert.strictEqual(
      await send(conn, 'PUNSUBSCRIBE'),
      refuses ? nosub('punsubscribe') : '+QUEUED\r\n',
    )
    assert.strictEqual(
      await send(conn, 'EXEC'),
      refuses
        ? '-EXECABORT Transaction discarded because of previous errors.\r\n'
        : `*2\r\n${confirmation(2, 'unsubscribe', null, 0)}${confirmation(2, 'punsubscribe', null, 0)}`,
    )
  })

  test('any subscription counts: a channel one allows PUNSUBSCRIBE', async () => {
    const conn = await connect(2)
    assert.strictEqual(
      await send(conn, 'SUBSCRIBE', 'a'),
      confirmation(2, 'subscribe', 'a', 1),
    )
    assert.strictEqual(
      await send(conn, 'PUNSUBSCRIBE'),
      confirmation(2, 'punsubscribe', null, 1),
    )
    assert.strictEqual(
      await send(conn, 'UNSUBSCRIBE'),
      confirmation(2, 'unsubscribe', 'a', 0),
    )
    assert.strictEqual(
      await send(conn, 'UNSUBSCRIBE'),
      refuses ? nosub('unsubscribe') : confirmation(2, 'unsubscribe', null, 0),
    )
  })

  test('RESP3: a pattern subscription keeps the connection subscribed', async () => {
    const conn = await connect(3)
    assert.strictEqual(
      await send(conn, 'PSUBSCRIBE', 'p*'),
      confirmation(3, 'psubscribe', 'p*', 1),
    )
    assert.strictEqual(
      await send(conn, 'UNSUBSCRIBE'),
      confirmation(3, 'unsubscribe', null, 1),
    )
    assert.strictEqual(
      await send(conn, 'PUNSUBSCRIBE'),
      confirmation(3, 'punsubscribe', 'p*', 0),
    )
    assert.strictEqual(
      await send(conn, 'PUNSUBSCRIBE'),
      refuses
        ? nosub('punsubscribe')
        : confirmation(3, 'punsubscribe', null, 0),
    )
  })
})
