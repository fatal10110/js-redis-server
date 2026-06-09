import { describe, test } from 'node:test'
import assert from 'node:assert'
import {
  ClientSession,
  InMemoryConnectionTransport,
  RedisResult,
  RedisServerState,
  RedisValue,
  type ResponseStream,
  Resp2SessionAdapter,
  createRedisCommandExecutor,
  defineCommand,
  t,
} from '../src'
import { createRedisSessionHarness as createSession } from './core-session-test-helpers'
import { commandFrame } from './shared-test-helpers'

describe('new transaction commands', () => {
  test('queues commands in MULTI and executes them through EXEC', async () => {
    const { session, server } = createSession()

    assert.deepStrictEqual(await session.execute('multi', []), RedisResult.ok())
    assert.deepStrictEqual(
      await session.execute('set', [Buffer.from('key'), Buffer.from('value')]),
      queued(),
    )
    assert.deepStrictEqual(
      await session.execute('get', [Buffer.from('key')]),
      queued(),
    )
    assert.strictEqual(
      server.getDatabase(0).getString(Buffer.from('key')),
      null,
    )

    assert.deepStrictEqual(
      await session.execute('exec', []),
      RedisResult.create(
        RedisValue.array([
          RedisValue.simpleString('OK'),
          RedisValue.bulkString(Buffer.from('value')),
        ]),
      ),
    )
  })

  test('DISCARD clears queued commands without executing them', async () => {
    const { session, server } = createSession()

    assert.deepStrictEqual(await session.execute('multi', []), RedisResult.ok())
    assert.deepStrictEqual(
      await session.execute('set', [Buffer.from('key'), Buffer.from('value')]),
      queued(),
    )
    assert.deepStrictEqual(
      await session.execute('discard', []),
      RedisResult.ok(),
    )

    assert.strictEqual(
      server.getDatabase(0).getString(Buffer.from('key')),
      null,
    )
  })

  test('marks queue-time errors dirty and aborts EXEC', async () => {
    const { session } = createSession()

    assert.deepStrictEqual(await session.execute('multi', []), RedisResult.ok())
    assert.deepStrictEqual(
      await session.execute('missing-command', []),
      RedisResult.error(
        "unknown command 'missing-command', with args beginning with: ",
        'ERR',
      ),
    )
    assert.deepStrictEqual(
      await session.execute('exec', []),
      RedisResult.error(
        'Transaction discarded because of previous errors.',
        'EXECABORT',
      ),
    )
  })

  test('runs caller policies before transaction queueing', async () => {
    const server = new RedisServerState()
    const executor = createRedisCommandExecutor({
      policies: [
        {
          name: 'reject-set',
          beforeExecute(plan) {
            if (plan.definition.name === 'set') {
              return RedisResult.error('blocked by policy')
            }
          },
        },
      ],
    })
    const session = new ClientSession({ server, executor })

    assert.deepStrictEqual(await session.execute('multi', []), RedisResult.ok())
    assert.deepStrictEqual(
      await session.execute('set', [Buffer.from('key'), Buffer.from('value')]),
      RedisResult.error('blocked by policy'),
    )
    assert.deepStrictEqual(
      await session.execute('exec', []),
      RedisResult.error(
        'Transaction discarded because of previous errors.',
        'EXECABORT',
      ),
    )
    assert.strictEqual(
      server.getDatabase(0).getString(Buffer.from('key')),
      null,
    )
  })

  test('queues push-only commands in MULTI', async () => {
    const streamCommand = defineCommand({
      name: 'subscribe',
      schema: t.object({
        channel: t.bulk(),
      }),
      flags: ['pubsub'],
      capabilities: { pushOnly: true },
      keys: () => [],
      execute: () => createSingleFrameStream(),
    })
    const server = new RedisServerState()
    const executor = createRedisCommandExecutor({
      extraCommands: [streamCommand],
    })
    const session = new ClientSession({ server, executor })

    assert.deepStrictEqual(await session.execute('multi', []), RedisResult.ok())
    assert.deepStrictEqual(
      await session.execute('subscribe', [Buffer.from('updates')]),
      queued(),
    )
  })

  test('keeps runtime command errors as EXEC array elements', async () => {
    const { session, server } = createSession()
    server.getDatabase(0).updateList(Buffer.from('list'), list => {
      list.values.push(Buffer.from('value'))
    })

    assert.deepStrictEqual(await session.execute('multi', []), RedisResult.ok())
    assert.deepStrictEqual(
      await session.execute('get', [Buffer.from('list')]),
      queued(),
    )

    assert.deepStrictEqual(
      await session.execute('exec', []),
      RedisResult.create(
        RedisValue.array([
          RedisValue.error(
            'Operation against a key holding the wrong kind of value',
            'WRONGTYPE',
          ),
        ]),
      ),
    )
  })

  test('SELECT inside MULTI switches the DB for later queued commands', async () => {
    const { session, server } = createSession({ databaseCount: 2 })

    assert.deepStrictEqual(await session.execute('multi', []), RedisResult.ok())
    assert.deepStrictEqual(
      await session.execute('select', [Buffer.from('1')]),
      queued(),
    )
    assert.deepStrictEqual(
      await session.execute('set', [Buffer.from('key'), Buffer.from('value')]),
      queued(),
    )

    assert.deepStrictEqual(
      await session.execute('exec', []),
      RedisResult.create(
        RedisValue.array([
          RedisValue.simpleString('OK'),
          RedisValue.simpleString('OK'),
        ]),
      ),
    )

    // SET must land in DB 1 (selected mid-EXEC), not the pre-SELECT DB 0.
    assert.strictEqual(
      server.getDatabase(0).getString(Buffer.from('key')),
      null,
    )
    assert.deepStrictEqual(
      server.getDatabase(1).getString(Buffer.from('key')),
      Buffer.from('value'),
    )
  })

  test('WATCH aborts EXEC with a null transaction result', async () => {
    const server = new RedisServerState()
    const executor = createRedisCommandExecutor()
    const first = new ClientSession({ server, executor })
    const second = new ClientSession({ server, executor })
    const key = Buffer.from('watched')

    assert.deepStrictEqual(
      await first.execute('watch', [key]),
      RedisResult.ok(),
    )
    assert.deepStrictEqual(
      await second.execute('set', [key, Buffer.from('other')]),
      RedisResult.ok(),
    )
    assert.deepStrictEqual(await first.execute('multi', []), RedisResult.ok())
    assert.deepStrictEqual(
      await first.execute('set', [key, Buffer.from('mine')]),
      queued(),
    )
    assert.deepStrictEqual(
      await first.execute('exec', []),
      RedisResult.create(RedisValue.nullArray()),
    )
    assert.deepStrictEqual(
      server.getDatabase(0).getString(key),
      Buffer.from('other'),
    )
  })

  test('queues UNWATCH inside MULTI and includes its EXEC reply', async () => {
    const { session } = createSession()

    assert.deepStrictEqual(
      await session.execute('watch', [Buffer.from('watched')]),
      RedisResult.ok(),
    )
    assert.deepStrictEqual(await session.execute('multi', []), RedisResult.ok())
    assert.deepStrictEqual(await session.execute('unwatch', []), queued())
    assert.deepStrictEqual(
      await session.execute('exec', []),
      RedisResult.create(RedisValue.array([RedisValue.simpleString('OK')])),
    )
  })

  test('transaction commands return expected mode errors', async () => {
    const { session } = createSession()

    assert.deepStrictEqual(
      await session.execute('exec', []),
      RedisResult.error('EXEC without MULTI', 'ERR'),
    )
    assert.deepStrictEqual(
      await session.execute('discard', []),
      RedisResult.error('DISCARD without MULTI', 'ERR'),
    )

    assert.deepStrictEqual(await session.execute('multi', []), RedisResult.ok())
    assert.deepStrictEqual(
      await session.execute('watch', [Buffer.from('key')]),
      RedisResult.error('WATCH inside MULTI is not allowed', 'ERR'),
    )
    assert.deepStrictEqual(
      await session.execute('exec', []),
      RedisResult.create(RedisValue.array([])),
    )
  })

  test('runs transaction commands through the RESP2 adapter', async () => {
    const { session } = createSession()
    const transport = new InMemoryConnectionTransport()
    const adapter = new Resp2SessionAdapter({ transport, session })
    const running = adapter.run()

    transport.feed(
      Buffer.concat([
        commandFrame('MULTI'),
        commandFrame('SET', 'key', 'value'),
        commandFrame('GET', 'key'),
        commandFrame('EXEC'),
      ]),
    )
    transport.endRead()

    await running

    assert.strictEqual(
      transport.getWrittenBuffer().toString(),
      '+OK\r\n+QUEUED\r\n+QUEUED\r\n*2\r\n+OK\r\n$5\r\nvalue\r\n',
    )
  })
})

function queued(): RedisResult {
  return RedisResult.create(RedisValue.simpleString('QUEUED'))
}

function createSingleFrameStream(): ResponseStream {
  return {
    kind: 'response-stream',
    closed: Promise.resolve(),
    frames: async function* () {
      yield RedisResult.create(
        RedisValue.push('message', [
          RedisValue.bulkString(Buffer.from('updates')),
        ]),
      )
    },
    close: () => {},
  }
}
