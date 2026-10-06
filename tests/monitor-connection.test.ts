import { describe, test } from 'node:test'
import assert from 'node:assert'
import {
  ClientSession,
  RedisServerState,
  createRedisCommandExecutor,
  type CompatibilitySpec,
  type RedisMonitorCommandEvent,
  type RedisResult,
} from '../src/internal'

function createServer(compatibility?: CompatibilitySpec, requirepass?: string) {
  const server = new RedisServerState({
    databaseCount: 2,
    compatibility,
    requirepass,
  })
  const executor = createRedisCommandExecutor({ compatibility: server.profile })
  const session = () => new ClientSession({ server, executor })
  return { server, session }
}

function recordFeed(server: RedisServerState): RedisMonitorCommandEvent[] {
  const events: RedisMonitorCommandEvent[] = []
  server.monitorFeed.subscribe(event => events.push(event))
  return events
}

function argv(event: RedisMonitorCommandEvent): string[] {
  return [event.command, ...event.args].map(arg => arg.toString())
}

function buffers(...values: string[]): Buffer[] {
  return values.map(value => Buffer.from(value))
}

/** Start MONITOR on `session` and deliver its +OK. */
async function startMonitor(session: ClientSession): Promise<void> {
  const reply = await session.execute('monitor', [])
  assert.strictEqual(reply.value.kind, 'simple-string')
  reply.options?.afterReply?.()
}

/** The push frames already queued on `session`, as text, without waiting. */
async function queuedPushes(session: ClientSession): Promise<string[]> {
  const reader = new AbortController()
  const pushes = session.readPushes(reader.signal)[Symbol.asyncIterator]()
  const lines: string[] = []
  for (;;) {
    const next = await Promise.race([
      pushes.next(),
      new Promise<null>(resolve => setImmediate(() => resolve(null))),
    ])
    if (next === null || next.done) {
      break
    }
    lines.push(String((next.value as RedisResult).value.value))
  }
  reader.abort()
  return lines
}

function errorText(result: RedisResult): string {
  assert.strictEqual(result.value.kind, 'error')
  return result.value.message
}

describe('MONITOR: script commands are stamped at dispatch (#433)', () => {
  test('EVAL, EVALSHA and FCALL read no later than the [0 lua] lines they produce', async () => {
    const { server, session } = createServer('redis-7.0')
    const actor = session()
    const events = recordFeed(server)
    const body =
      "redis.call('set', KEYS[1], '1'); return redis.call('incr', KEYS[1])"

    const sha = await actor.execute('script', buffers('LOAD', body))
    const library = `#!lua name=monlib\nredis.register_function('monfn', function(keys) ${body.replaceAll('KEYS', 'keys')} end)`
    await actor.execute('function', buffers('LOAD', library))
    events.length = 0

    const calls: Array<[string, Buffer[]]> = [
      ['EVAL', buffers(body, '1', 'k')],
      ['EVALSHA', [sha.value.value as Buffer, ...buffers('1', 'k')]],
      ['FCALL', buffers('monfn', '1', 'k')],
    ]
    for (const [command, args] of calls) {
      events.length = 0
      await actor.execute(command, args)

      assert.deepStrictEqual(
        events.map(event => event.clientAddress === 'lua'),
        [false, true, true],
        command,
      )
      assert.strictEqual(argv(events[0])[0], command)
      for (const nested of events.slice(1)) {
        assert.ok(
          events[0].timestampMicros <= nested.timestampMicros,
          `${command} stamped ${events[0].timestampMicros}, after its ${argv(nested)[0]} at ${nested.timestampMicros}`,
        )
      }
    }
  })

  test('EVAL replayed by EXEC is stamped before its nested lines, EXEC after them', async () => {
    const { server, session } = createServer()
    const actor = session()
    const events = recordFeed(server)

    await actor.execute('multi', [])
    await actor.execute('eval', buffers("return redis.call('incr', 'n')", '0'))
    await actor.execute('exec', [])

    assert.deepStrictEqual(events.map(argv), [
      ['multi'],
      ['eval', "return redis.call('incr', 'n')", '0'],
      ['incr', 'n'],
      ['exec'],
    ])
    for (let i = 1; i < events.length; i++) {
      assert.ok(events[i - 1].timestampMicros <= events[i].timestampMicros)
    }
  })
})

describe('MONITOR: commands sent on the monitoring connection (#456)', () => {
  test('its own command is fed to it, after the reply', async () => {
    const { session } = createServer()
    const monitor = session()
    await startMonitor(monitor)

    const reply = await monitor.execute('ping', buffers('hello'))
    assert.deepStrictEqual(await queuedPushes(monitor), [])

    reply.options?.afterReply?.()
    const lines = await queuedPushes(monitor)
    assert.strictEqual(lines.length, 1)
    assert.match(lines[0], /^\d+\.\d{6} \[0 [^\]]+\] "ping" "hello"$/)
    monitor.close()
  })

  test('QUIT closes without its own line', async () => {
    const { session } = createServer()
    const monitor = session()
    await startMonitor(monitor)

    const reply = await monitor.execute('quit', [])
    assert.strictEqual(reply.options?.close, true)
    reply.options?.afterReply?.()
    assert.deepStrictEqual(await queuedPushes(monitor), [])
    monitor.close()
  })

  const refusals: Array<[CompatibilitySpec, string]> = [
    ['redis-6.2', "Replica can't interract with the keyspace"],
    ['redis-7.0', "Replica can't interact with the keyspace"],
    ['redis-8.0', "Replica can't interact with the keyspace"],
    ['valkey-8.0', "Replica can't interact with the keyspace"],
    ['valkey-9.0', "Replica can't interact with the keyspace"],
  ]
  for (const [profile, message] of refusals) {
    test(`${profile}: readonly, write and may_replicate commands are refused and not fed`, async () => {
      const { server, session } = createServer(profile)
      const monitor = session()
      await startMonitor(monitor)
      const events = recordFeed(server)

      for (const args of [
        ['get', 'k'],
        ['set', 'k', 'v'],
        ['dbsize'],
        ['publish', 'c', 'm'],
        ['eval', 'return 1', '0'],
        ['evalsha', 'e0e1f9fabfc9d4800c877a703b823ac0578ff8db', '0'],
      ]) {
        const [command, ...rest] = args
        const reply = await monitor.execute(command, buffers(...rest))
        assert.strictEqual(errorText(reply), message, args.join(' '))
      }
      assert.deepStrictEqual(events.map(argv), [])

      // Neither readonly nor write: run and fed as usual.
      for (const args of [['ping'], ['echo', 'x'], ['select', '1']]) {
        const [command, ...rest] = args
        const reply = await monitor.execute(command, buffers(...rest))
        assert.notStrictEqual(reply.value.kind, 'error', args.join(' '))
      }
      assert.deepStrictEqual(events.map(argv), [
        ['ping'],
        ['echo', 'x'],
        ['select', '1'],
      ])
      monitor.close()
    })
  }

  test('the flags are the resolved entry: SCRIPT is refused whole on 6.2, by subcommand from 7.0', async () => {
    const legacy = createServer('redis-6.2').session()
    await startMonitor(legacy)
    assert.strictEqual(
      errorText(await legacy.execute('script', buffers('EXISTS', 'abc'))),
      "Replica can't interract with the keyspace",
    )
    // 6.2 answers QUIT before any check.
    assert.strictEqual((await legacy.execute('quit', [])).options?.close, true)
    legacy.close()

    const modern = createServer('redis-7.0').session()
    await startMonitor(modern)
    const exists = await modern.execute('script', buffers('EXISTS', 'abc'))
    assert.strictEqual(exists.value.kind, 'array')
    assert.strictEqual(
      errorText(await modern.execute('xinfo', buffers('STREAM', 's'))),
      "Replica can't interact with the keyspace",
    )
    modern.close()
  })

  test('a refusal inside MULTI dirties the transaction', async () => {
    const { session } = createServer()
    const monitor = session()
    await startMonitor(monitor)

    await monitor.execute('multi', [])
    assert.strictEqual(
      errorText(await monitor.execute('get', buffers('k'))),
      "Replica can't interact with the keyspace",
    )
    assert.strictEqual(
      errorText(await monitor.execute('exec', [])),
      'Transaction discarded because of previous errors.',
    )
    monitor.close()
  })
})

describe('MONITOR: commands refused before call() are not fed', () => {
  test('NOAUTH', async () => {
    const { server, session } = createServer(undefined, 'secret')
    const events = recordFeed(server)
    const actor = session()

    assert.match(
      errorText(await actor.execute('get', buffers('k'))),
      /^Authentication required/,
    )
    assert.deepStrictEqual(events.map(argv), [])
  })

  test('the subscribed-context refusal', async () => {
    const { server, session } = createServer()
    const events = recordFeed(server)
    const actor = session()

    await actor.execute('subscribe', buffers('ch'))
    assert.match(
      errorText(await actor.execute('get', buffers('k'))),
      /^Can't execute 'get'/,
    )
    assert.deepStrictEqual(events.map(argv), [['subscribe', 'ch']])
    actor.close()
  })
})
