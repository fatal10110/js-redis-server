import { after, before, describe, test } from 'node:test'
import assert from 'node:assert'
import { TestRunner } from '../test-config'
import { activeProfile, commandFrame, randomKey } from '../utils'
import { RawRedisConnection } from './raw-connection'

const testRunner = new TestRunner()

// Redis 6.2 names the command (`c->cmd->name`) and has no sharded Pub/Sub;
// 7.0 names the looked-up table entry (`c->cmd->fullname`, `config|get`) and
// lists `(P|S)` (#384, #514).
const legacy = activeProfile === 'redis-6.2'
const allowed = legacy
  ? '(P)SUBSCRIBE / (P)UNSUBSCRIBE / PING / QUIT / RESET'
  : '(P|S)SUBSCRIBE / (P|S)UNSUBSCRIBE / PING / QUIT / RESET'

function refusal(name: string): Buffer {
  return Buffer.from(
    `-ERR Can't execute '${name}': only ${allowed} are allowed in this context\r\n`,
  )
}

describe(`Raw TCP subscribed-mode refusal wording (${testRunner.getBackendName()}, ${activeProfile})`, () => {
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

  async function subscribed(): Promise<RawRedisConnection> {
    const conn = await RawRedisConnection.connect('127.0.0.1', port)
    connections.push(conn)
    conn.write(commandFrame('SUBSCRIBE', `raw-subscribed:${randomKey()}`))
    await conn.readRawFrame()
    return conn
  }

  async function send(
    conn: RawRedisConnection,
    ...args: string[]
  ): Promise<Buffer> {
    conn.write(commandFrame(...args))
    return conn.readRawFrame()
  }

  test('names a plain command and lists the profile allowed commands', async () => {
    const conn = await subscribed()
    assert.deepStrictEqual(await send(conn, 'GET', randomKey()), refusal('get'))
    assert.deepStrictEqual(await send(conn, 'MULTI'), refusal('multi'))
    assert.deepStrictEqual(await send(conn, 'INFO'), refusal('info'))
    assert.deepStrictEqual(
      await send(conn, 'EVAL', 'return 1', '0'),
      refusal('eval'),
    )
  })

  test('names a container subcommand as its table entry from 7.0', async () => {
    const conn = await subscribed()
    const stream = randomKey()
    const cases: Array<[string[], string]> = [
      [['CONFIG', 'GET', 'maxmemory'], 'config|get'],
      [['CLIENT', 'ID'], 'client|id'],
      [['XINFO', 'STREAM', stream], 'xinfo|stream'],
      [['PUBSUB', 'NUMSUB'], 'pubsub|numsub'],
      [['SCRIPT', 'FLUSH'], 'script|flush'],
      [['COMMAND', 'INFO', 'get'], 'command|info'],
      [['XGROUP', 'CREATE', stream, 'g', '$'], 'xgroup|create'],
    ]
    for (const [args, name] of cases) {
      assert.deepStrictEqual(
        await send(conn, ...args),
        refusal(legacy ? name.split('|')[0] : name),
        args.join(' '),
      )
    }
  })

  test('an unknown subcommand and the table arity are checked first', async () => {
    const conn = await subscribed()
    assert.deepStrictEqual(
      await send(conn, 'GET'),
      Buffer.from("-ERR wrong number of arguments for 'get' command\r\n"),
    )
    assert.deepStrictEqual(
      await send(conn, 'CONFIG'),
      Buffer.from("-ERR wrong number of arguments for 'config' command\r\n"),
    )
    if (legacy) {
      // 6.2 resolves no subcommand before the subscribed check, and CONFIG's
      // table arity is -2.
      assert.deepStrictEqual(
        await send(conn, 'CONFIG', 'GET'),
        refusal('config'),
      )
      assert.deepStrictEqual(
        await send(conn, 'CONFIG', 'BOGUS'),
        refusal('config'),
      )
      assert.deepStrictEqual(
        await send(conn, 'CLIENT', 'BOGUS'),
        refusal('client'),
      )
      return
    }
    assert.deepStrictEqual(
      await send(conn, 'CONFIG', 'GET'),
      Buffer.from(
        "-ERR wrong number of arguments for 'config|get' command\r\n",
      ),
    )
    assert.deepStrictEqual(
      await send(conn, 'CONFIG', 'BOGUS'),
      Buffer.from("-ERR unknown subcommand 'BOGUS'. Try CONFIG HELP.\r\n"),
    )
    assert.deepStrictEqual(
      await send(conn, 'CLIENT', 'BOGUS'),
      Buffer.from("-ERR unknown subcommand 'BOGUS'. Try CLIENT HELP.\r\n"),
    )
  })

  test('RESP3 runs any command while subscribed', async () => {
    const conn = await RawRedisConnection.connect('127.0.0.1', port)
    connections.push(conn)
    await send(conn, 'HELLO', '3')
    await send(conn, 'SUBSCRIBE', `raw-subscribed:${randomKey()}`)
    assert.deepStrictEqual(
      await send(conn, 'GET', randomKey()),
      Buffer.from('_\r\n'),
    )
    assert.deepStrictEqual(
      await send(conn, 'ECHO', 'hi'),
      Buffer.from('$2\r\nhi\r\n'),
    )
  })
})
