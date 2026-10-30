import { after, afterEach, before, describe, test } from 'node:test'
import assert from 'node:assert'
import { setTimeout as delay } from 'node:timers/promises'
import { TestRunner } from '../test-config'
import { activeProfile, commandFrame, randomKey } from '../utils'
import { RawRedisConnection } from './raw-connection'

const testRunner = new TestRunner()

/** Redis 7.2 / Valkey 7.2 refuse a timeout whose end time would overflow. */
const checksTimeoutOverflow = !['redis-6.2', 'redis-7.0'].includes(
  activeProfile,
)

/**
 * Valkey 8.1 gave CLIENT PAUSE its own handler (arity -3): arguments past the
 * mode are ignored, and the mode is only read when it is the last argument.
 */
const ignoresExtraPauseArgs = activeProfile === 'valkey-9.0'

/** Redis 6.2 answers QUIT in processCommand, ahead of the pause check. */
const quitBeforePause = activeProfile === 'redis-6.2'

describe(`Raw TCP CLIENT PAUSE / UNPAUSE (${testRunner.getBackendName()})`, () => {
  let port: number
  const connections: RawRedisConnection[] = []

  before(async () => {
    port = await testRunner.setupRawStandalone()
  })

  afterEach(async () => {
    // The real servers are shared: never leave a WRITE pause behind (an ALL
    // pause in these tests is short and lapses on its own).
    const cleanup = await connect()
    await reply(cleanup, 'CLIENT', 'UNPAUSE')
    for (const connection of connections) {
      connection.close()
    }
    connections.length = 0
  })

  after(async () => {
    await testRunner.cleanup()
  })

  async function connect(): Promise<RawRedisConnection> {
    const connection = await RawRedisConnection.connect('127.0.0.1', port)
    connections.push(connection)
    return connection
  }

  async function reply(
    connection: RawRedisConnection,
    ...args: string[]
  ): Promise<string> {
    connection.write(commandFrame(...args))
    return (await connection.readRawFrame()).toString()
  }

  /** Send a command and report whether it is still unanswered after `ms`. */
  async function sendPending(
    connection: RawRedisConnection,
    args: string[],
    ms = 150,
  ): Promise<{ pending: boolean; reply: Promise<string> }> {
    connection.write(commandFrame(...args))
    const replyPromise = connection
      .readRawFrame()
      .then(frame => frame.toString())
    const winner = await Promise.race([
      replyPromise.then(() => 'reply' as const),
      delay(ms).then(() => 'pending' as const),
    ])
    return { pending: winner === 'pending', reply: replyPromise }
  }

  test('rejects a bad mode before the timeout, and bad timeouts', async () => {
    const client = await connect()
    const modeError = '-ERR CLIENT PAUSE mode must be WRITE or ALL\r\n'
    assert.strictEqual(
      await reply(client, 'CLIENT', 'PAUSE', '100', 'FOO'),
      modeError,
    )
    assert.strictEqual(
      await reply(client, 'CLIENT', 'PAUSE', 'abc', 'FOO'),
      modeError,
    )

    const notInteger = '-ERR timeout is not an integer or out of range\r\n'
    for (const timeout of [
      'abc',
      '1.5',
      '99999999999999999999',
      '9223372036854775808',
    ]) {
      assert.strictEqual(
        await reply(client, 'CLIENT', 'PAUSE', timeout),
        notInteger,
        timeout,
      )
    }
    assert.strictEqual(
      await reply(client, 'CLIENT', 'PAUSE', '-1', 'write'),
      '-ERR timeout is negative\r\n',
    )
    assert.strictEqual(
      await reply(client, 'CLIENT', 'PAUSE', '9223372036854775807', 'WRITE'),
      checksTimeoutOverflow ? '-ERR timeout is out of range\r\n' : '+OK\r\n',
    )
    // An overflowing end time never pauses anything.
    assert.strictEqual(await reply(client, 'SET', randomKey(), 'v'), '+OK\r\n')
  })

  test('extra arguments after the mode', async () => {
    const client = await connect()
    const expected = ignoresExtraPauseArgs
      ? '+OK\r\n'
      : activeProfile === 'redis-6.2'
        ? "-ERR Unknown subcommand or wrong number of arguments for 'PAUSE'. Try CLIENT HELP.\r\n"
        : "-ERR unknown subcommand or wrong number of arguments for 'PAUSE'. Try CLIENT HELP.\r\n"
    assert.strictEqual(
      await reply(client, 'CLIENT', 'PAUSE', '0', 'WRITE', 'x'),
      expected,
    )
  })

  test('CLIENT PAUSE 0 and an UNPAUSE with no pause answer +OK and pause nothing', async () => {
    const client = await connect()
    assert.strictEqual(await reply(client, 'CLIENT', 'UNPAUSE'), '+OK\r\n')
    assert.strictEqual(await reply(client, 'CLIENT', 'PAUSE', '0'), '+OK\r\n')
    assert.strictEqual(
      await reply(client, 'CLIENT', 'PAUSE', '0', 'all'),
      '+OK\r\n',
    )
    assert.strictEqual(await reply(client, 'GET', randomKey()), '$-1\r\n')
  })

  test('WRITE postpones writes of every client until UNPAUSE; reads go on', async () => {
    const issuer = await connect()
    const writer = await connect()
    const reader = await connect()
    const key = randomKey()

    assert.strictEqual(
      await reply(issuer, 'CLIENT', 'PAUSE', '5000', 'write'),
      '+OK\r\n',
    )
    const set = await sendPending(writer, ['SET', key, 'v'])
    assert.ok(set.pending, 'SET answered during a WRITE pause')
    const ownWrite = await sendPending(issuer, ['DEL', key], 10)
    assert.ok(ownWrite.pending, 'the issuer is paused too')

    assert.strictEqual(await reply(reader, 'GET', key), '$-1\r\n')
    assert.strictEqual(await reply(reader, 'PING'), '+PONG\r\n')

    const started = Date.now()
    assert.strictEqual(await reply(reader, 'CLIENT', 'UNPAUSE'), '+OK\r\n')
    assert.strictEqual(await set.reply, '+OK\r\n')
    assert.strictEqual(await ownWrite.reply, ':1\r\n')
    assert.ok(Date.now() - started < 2000, 'UNPAUSE ended the pause')
  })

  test('a WRITE pause ends on its own when the timeout lapses', async () => {
    const issuer = await connect()
    const writer = await connect()
    const started = Date.now()
    assert.strictEqual(
      await reply(issuer, 'CLIENT', 'PAUSE', '400', 'WRITE'),
      '+OK\r\n',
    )
    const del = await sendPending(writer, ['DEL', randomKey()])
    assert.ok(del.pending)
    assert.strictEqual(await del.reply, ':0\r\n')
    assert.ok(Date.now() - started >= 350, 'DEL ran before the pause lapsed')
  })

  test('WRITE postpones a write before MULTI queues it, and an EXEC with a queued write', async () => {
    const issuer = await connect()
    const client = await connect()
    const key = randomKey()

    assert.strictEqual(await reply(client, 'MULTI'), '+OK\r\n')
    assert.strictEqual(await reply(client, 'SET', key, 'v'), '+QUEUED\r\n')
    assert.strictEqual(
      await reply(issuer, 'CLIENT', 'PAUSE', '5000', 'WRITE'),
      '+OK\r\n',
    )
    assert.strictEqual(await reply(client, 'GET', key), '+QUEUED\r\n')
    const exec = await sendPending(client, ['EXEC'])
    assert.ok(exec.pending, 'EXEC of a transaction with a write ran')
    assert.strictEqual(await reply(issuer, 'CLIENT', 'UNPAUSE'), '+OK\r\n')
    assert.strictEqual(await exec.reply, '*2\r\n+OK\r\n$1\r\nv\r\n')

    assert.strictEqual(
      await reply(issuer, 'CLIENT', 'PAUSE', '5000', 'WRITE'),
      '+OK\r\n',
    )
    assert.strictEqual(await reply(client, 'MULTI'), '+OK\r\n')
    assert.strictEqual(await reply(client, 'GET', key), '+QUEUED\r\n')
    const queued = await sendPending(client, ['DEL', key])
    assert.ok(queued.pending, 'DEL was queued during a WRITE pause')
    assert.strictEqual(await reply(issuer, 'CLIENT', 'UNPAUSE'), '+OK\r\n')
    assert.strictEqual(await queued.reply, '+QUEUED\r\n')
    assert.strictEqual(await reply(client, 'EXEC'), '*2\r\n$1\r\nv\r\n:1\r\n')
  })

  test('ALL postpones every command, CLIENT UNPAUSE included, until the timeout', async () => {
    const issuer = await connect()
    const reader = await connect()
    const other = await connect()
    const started = Date.now()

    assert.strictEqual(
      await reply(issuer, 'CLIENT', 'PAUSE', '600', 'all'),
      '+OK\r\n',
    )
    const ping = await sendPending(reader, ['PING'])
    assert.ok(ping.pending, 'PING answered during an ALL pause')
    const own = await sendPending(issuer, ['PING'], 10)
    assert.ok(own.pending, 'the issuer is paused too')

    // Refused before the pause check: answered at once.
    assert.strictEqual(
      await reply(other, 'SET', 'onlykey'),
      "-ERR wrong number of arguments for 'set' command\r\n",
    )
    const unpause = await sendPending(other, ['CLIENT', 'UNPAUSE'], 10)
    assert.ok(unpause.pending, 'UNPAUSE answered during an ALL pause')

    assert.strictEqual(await ping.reply, '+PONG\r\n')
    assert.strictEqual(await own.reply, '+PONG\r\n')
    assert.strictEqual(await unpause.reply, '+OK\r\n')
    assert.ok(Date.now() - started >= 500, 'an ALL pause ended early')
  })

  test('ALL postpones QUIT from 7.0; Redis 6.2 answers it at once', async () => {
    const issuer = await connect()
    const quitter = await connect()

    assert.strictEqual(
      await reply(issuer, 'CLIENT', 'PAUSE', '500', 'ALL'),
      '+OK\r\n',
    )
    const quit = await sendPending(quitter, ['QUIT'])
    assert.strictEqual(quit.pending, !quitBeforePause)
    assert.strictEqual(await quit.reply, '+OK\r\n')
  })

  test('a later pause keeps the longer end time and the stricter mode', async () => {
    const issuer = await connect()
    const reader = await connect()
    const started = Date.now()

    assert.strictEqual(
      await reply(issuer, 'CLIENT', 'PAUSE', '700', 'WRITE'),
      '+OK\r\n',
    )
    assert.strictEqual(
      await reply(issuer, 'CLIENT', 'PAUSE', '100', 'ALL'),
      '+OK\r\n',
    )
    await delay(300)
    const get = await sendPending(reader, ['GET', randomKey()])
    assert.ok(get.pending, 'the ALL pause did not keep the longer end time')
    assert.strictEqual(await get.reply, '$-1\r\n')
    assert.ok(Date.now() - started >= 600)
  })
})
