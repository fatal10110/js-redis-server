import { describe, test } from 'node:test'
import assert from 'node:assert'
import { setTimeout as delay } from 'node:timers/promises'
import {
  ClientSession,
  RedisServerState,
  createRedisCommandExecutor,
  encodeRedisValue,
  type CompatibilitySpec,
  type RedisResult,
} from '../src/internal'
import { ClientPauseState } from '../src/state/client-pause'

function createServer(compatibility: CompatibilitySpec = 'redis-8.0') {
  const server = new RedisServerState({ compatibility, databaseCount: 16 })
  const executor = createRedisCommandExecutor({ compatibility: server.profile })
  const connect = () => new ClientSession({ server, executor })
  return { server, connect }
}

function buf(...values: string[]): Buffer[] {
  return values.map(value => Buffer.from(value))
}

/** The reply on the wire (RESP2), without its final CRLF. */
function reply(result: RedisResult): string {
  return encodeRedisValue(result.value).toString().replace(/\r\n$/, '')
}

async function run(
  session: ClientSession,
  command: string,
  ...args: string[]
): Promise<string> {
  return reply(await session.execute(command, buf(...args)))
}

/** Whether `promise` is still pending after `ms`. */
async function isPending(promise: Promise<unknown>, ms = 30): Promise<boolean> {
  return Promise.race([promise.then(() => false), delay(ms).then(() => true)])
}

describe('ClientPauseState', () => {
  test('keeps the later end time and the stricter mode', () => {
    const pause = new ClientPauseState()
    const now = Date.now()
    assert.strictEqual(pause.mode, null)

    pause.pause(now + 10_000, 'write')
    assert.strictEqual(pause.mode, 'write')
    pause.pause(now + 100, 'all')
    assert.strictEqual(pause.mode, 'all')
    pause.pause(now + 50, 'write')
    assert.strictEqual(pause.mode, 'all', 'a WRITE pause downgraded ALL')
    pause.unpause()
    assert.strictEqual(pause.mode, null)
  })

  test('an end time in the past pauses nothing', () => {
    const pause = new ClientPauseState()
    pause.pause(0, 'all')
    assert.strictEqual(pause.mode, null)
    pause.pause(-1, 'write')
    assert.strictEqual(pause.mode, null)
  })

  test('settles its waiters on UNPAUSE and when the end time passes', async () => {
    const pause = new ClientPauseState()
    pause.pause(Date.now() + 10_000, 'write')
    const waiting = pause.whenUnpaused()
    assert.ok(await isPending(waiting))
    pause.unpause()
    await waiting

    pause.pause(Date.now() + 40, 'all')
    const started = Date.now()
    await pause.whenUnpaused()
    assert.ok(Date.now() - started >= 30)
    assert.strictEqual(pause.mode, null)
  })

  test('its timer only keeps the process alive while a waiter remains', async () => {
    const pause = new ClientPauseState()
    const timer = () =>
      (pause as unknown as { timer?: NodeJS.Timeout }).timer?.hasRef()
    pause.pause(Date.now() + 600_000, 'write')
    assert.strictEqual(timer(), false, 'an idle pause holds the process')

    const first = new AbortController()
    const second = new AbortController()
    void pause.whenUnpaused(first.signal)
    void pause.whenUnpaused(second.signal)
    assert.strictEqual(timer(), true)
    first.abort()
    assert.strictEqual(timer(), true, 'a waiter still remains')
    second.abort()
    assert.strictEqual(timer(), false, 'aborted waiters hold the process')
    pause.unpause()
  })
})

describe('CLIENT PAUSE / UNPAUSE', () => {
  test('closing the server lifts the pause', async () => {
    const { server, connect } = createServer()
    const issuer = connect()
    const writer = connect()

    assert.strictEqual(
      await run(issuer, 'client', 'pause', '600000', 'write'),
      '+OK',
    )
    const set = writer.execute('set', buf('k', 'v'))
    assert.ok(await isPending(set))
    writer.close()
    issuer.close()
    server.close()
    assert.strictEqual(server.clientPause.mode, null)
    await set.catch(() => undefined)
  })

  test('Redis 6.2 answers QUIT during a pause, 7.0 postpones it', async () => {
    for (const [compatibility, postponed] of [
      ['redis-6.2', false],
      ['redis-7.0', true],
    ] as const) {
      const { connect } = createServer(compatibility)
      const issuer = connect()
      const quitter = connect()
      // An ALL pause holds back UNPAUSE too: let a short one lapse.
      assert.strictEqual(
        await run(issuer, 'client', 'pause', '150', 'all'),
        '+OK',
      )
      const quit = run(quitter, 'quit')
      assert.strictEqual(await isPending(quit), postponed, compatibility)
      assert.strictEqual(await quit, '+OK')
    }
  })

  test('a WRITE pause holds back writes of every client until UNPAUSE', async () => {
    const { connect } = createServer()
    const issuer = connect()
    const writer = connect()

    assert.strictEqual(
      await run(issuer, 'client', 'pause', '10000', 'write'),
      '+OK',
    )
    const set = run(writer, 'set', 'k', 'v')
    const publish = run(writer, 'publish', 'ch', 'm')
    assert.ok(await isPending(set), 'SET ran during a WRITE pause')

    assert.strictEqual(await run(issuer, 'get', 'k'), '$-1')
    assert.strictEqual(await run(issuer, 'eval_ro', 'return 1', '0'), ':1')
    const evalWrite = run(issuer, 'eval', 'return 1', '0')
    assert.ok(await isPending(evalWrite), 'EVAL is may-replicate')

    assert.strictEqual(await run(issuer, 'client', 'unpause'), '+OK')
    assert.strictEqual(await set, '+OK')
    assert.strictEqual(await publish, ':0')
    assert.strictEqual(await evalWrite, ':1')
  })

  test('an ALL pause holds back every client but a MONITOR connection', async () => {
    const { connect } = createServer()
    const issuer = connect()
    const monitor = connect()
    await run(monitor, 'monitor')

    assert.strictEqual(await run(issuer, 'client', 'pause', '80'), '+OK')
    const ping = run(issuer, 'ping')
    assert.ok(await isPending(ping))
    assert.strictEqual(await run(monitor, 'ping'), '+PONG')
    assert.strictEqual(await ping, '+PONG')
  })

  test('valkey 8.1 ignores arguments past the mode', async () => {
    const valkey81 = createServer({ flavor: 'valkey', version: '8.1.0' })
    const client = valkey81.connect()
    assert.strictEqual(
      await run(client, 'client', 'pause', '10000', 'WRITE', 'x'),
      '+OK',
    )
    // The mode is read only when it is the last argument: this is ALL.
    assert.strictEqual(valkey81.server.clientPause.mode, 'all')
    valkey81.server.clientPause.unpause()

    const valkey80 = createServer('valkey-8.0').connect()
    assert.strictEqual(
      await run(valkey80, 'client', 'pause', '0', 'WRITE', 'x'),
      "-ERR unknown subcommand or wrong number of arguments for 'pause'. Try CLIENT HELP.",
    )
  })

  test('an end time past LLONG_MAX is refused from 7.2, ignored before', async () => {
    const llongMax = '9223372036854775807'
    const redis70 = createServer('redis-7.0')
    assert.strictEqual(
      await run(redis70.connect(), 'client', 'pause', llongMax, 'WRITE'),
      '+OK',
    )
    assert.strictEqual(redis70.server.clientPause.mode, null)
    assert.strictEqual(
      await run(
        createServer('redis-7.2').connect(),
        'client',
        'pause',
        llongMax,
      ),
      '-ERR timeout is out of range',
    )
  })
})

describe('CLIENT REPLY / NO-TOUCH', () => {
  test('OFF and SKIP mark replies omitted on the session', async () => {
    const { connect } = createServer()
    const client = connect()
    const omitted = async (...args: string[]) =>
      (await client.execute(args[0], buf(...args.slice(1)))).options
        ?.omitReply === true

    assert.strictEqual(await omitted('client', 'reply', 'skip'), true)
    assert.strictEqual(await omitted('ping'), true)
    assert.strictEqual(await omitted('ping'), false)
    assert.strictEqual(await omitted('client', 'reply', 'off'), true)
    assert.strictEqual(await omitted('set', 'k', 'v'), true)
    assert.strictEqual(await omitted('client', 'reply', 'on'), false)
    assert.strictEqual(await omitted('get', 'k'), false)
  })

  test('NO-TOUCH is a 7.2 subcommand', async () => {
    assert.strictEqual(
      await run(
        createServer('redis-7.0').connect(),
        'client',
        'no-touch',
        'on',
      ),
      "-ERR unknown subcommand 'no-touch'. Try CLIENT HELP.",
    )
    assert.strictEqual(
      await run(
        createServer('redis-7.2').connect(),
        'client',
        'no-touch',
        'on',
      ),
      '+OK',
    )
  })
})
