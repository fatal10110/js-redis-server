import { describe, test } from 'node:test'
import assert from 'node:assert'
import {
  ClientSession,
  CommandExecutor,
  createRedisCommandRegistry,
  InMemoryConnectionTransport,
  RedisResult,
  RedisServerState,
  RedisValue,
  Resp2SessionAdapter,
  defineCommand,
  t,
} from '../src/internal'
import type { ResponseStream } from '../src/internal'
import { createRedisSessionHarness as createHarness } from './core-session-test-helpers'
import { commandFrame } from './shared-test-helpers'

describe('new transport-neutral session path', () => {
  test('runs commands through ClientSession with selected database state', async () => {
    const { session } = createHarness({ databaseCount: 2 })

    assert.deepStrictEqual(
      await session.execute('set', [Buffer.from('key'), Buffer.from('v0')]),
      RedisResult.ok(),
    )
    assert.deepStrictEqual(
      await session.execute('get', [Buffer.from('key')]),
      RedisResult.create(RedisValue.bulkString(Buffer.from('v0'))),
    )

    await session.execute('select', [Buffer.from('1')])

    assert.deepStrictEqual(
      await session.execute('get', [Buffer.from('key')]),
      RedisResult.create(RedisValue.bulkString(null)),
    )
  })

  test('executes RESP2 frames over an in-memory ConnectionTransport', async () => {
    const { session } = createHarness()
    const transport = new InMemoryConnectionTransport()
    const adapter = new Resp2SessionAdapter({ transport, session })
    const running = adapter.run()

    transport.feed(
      Buffer.concat([
        commandFrame('SET', 'key', 'value'),
        commandFrame('GET', 'key'),
      ]),
    )
    transport.endRead()

    await running

    assert.strictEqual(
      transport.getWrittenBuffer().toString(),
      '+OK\r\n$5\r\nvalue\r\n',
    )
  })

  test('buffers partial RESP2 frames until a complete command arrives', async () => {
    const { session } = createHarness()
    const transport = new InMemoryConnectionTransport()
    const adapter = new Resp2SessionAdapter({ transport, session })
    const running = adapter.run()
    const frame = commandFrame('PING')

    transport.feed(frame.subarray(0, 4))
    assert.strictEqual(transport.getWrittenBuffer().length, 0)

    transport.feed(frame.subarray(4))
    transport.endRead()

    await running

    assert.strictEqual(transport.getWrittenBuffer().toString(), '+PONG\r\n')
  })

  test('ignores empty RESP2 commands and parses quoted inline arguments', async () => {
    const { session } = createHarness()
    const transport = new InMemoryConnectionTransport()
    const adapter = new Resp2SessionAdapter({ transport, session })
    const running = adapter.run()

    transport.feed(
      Buffer.from(
        '\r\n*0\r\nPING "hello world"\r\nSET quoted "a\\x20b"\r\nGET quoted\r\n',
      ),
    )
    transport.endRead()

    await running

    assert.strictEqual(
      transport.getWrittenBuffer().toString(),
      '$11\r\nhello world\r\n+OK\r\n$3\r\na b\r\n',
    )
  })

  test('honors RedisResult close options in the RESP2 session adapter', async () => {
    const { session } = createHarness()
    const transport = new InMemoryConnectionTransport()
    const adapter = new Resp2SessionAdapter({ transport, session })
    const running = adapter.run()

    transport.feed(commandFrame('QUIT'))
    transport.endRead()

    await running

    assert.strictEqual(transport.signal.aborted, true)
    assert.strictEqual(transport.getWrittenBuffer().toString(), '+OK\r\n')
  })

  test('flushes responses for valid pipelined frames before a protocol error', async () => {
    const { session } = createHarness()
    const transport = new InMemoryConnectionTransport()
    const adapter = new Resp2SessionAdapter({ transport, session })
    const running = adapter.run()

    // A single TCP write: two valid commands followed by a malformed frame
    // (bulk length is not numeric). Real Redis replies to the valid commands
    // first, then sends the protocol error and closes the connection.
    transport.feed(
      Buffer.concat([
        commandFrame('SET', 'key', 'value'),
        commandFrame('GET', 'key'),
        Buffer.from('*1\r\n$abc\r\n'),
      ]),
    )
    transport.endRead()

    await running

    assert.strictEqual(
      transport.getWrittenBuffer().toString(),
      '+OK\r\n$5\r\nvalue\r\n-ERR Protocol error: invalid bulk length\r\n',
    )
    assert.strictEqual(transport.signal.aborted, true)
  })

  test('drains ResponseStream frames through the same adapter', async () => {
    const { session } = createHarness({ extraCommands: [streamCommand] })
    const transport = new InMemoryConnectionTransport()
    const adapter = new Resp2SessionAdapter({ transport, session })
    const running = adapter.run()

    transport.feed(commandFrame('STREAM'))
    transport.endRead()

    await running

    assert.strictEqual(
      transport.getWrittenBuffer().toString(),
      '*2\r\n$7\r\nmessage\r\n$7\r\nupdates\r\n',
    )
  })

  test('tracks WATCH invalidation through database mutation events', () => {
    const { server, session } = createHarness()
    const key = Buffer.from('watched')

    session.watch([key])
    assert.strictEqual(session.isWatchDirty(), false)

    server.getDatabase(0).setString(key, Buffer.from('value'))
    assert.strictEqual(session.isWatchDirty(), true)

    session.unwatch()
    assert.strictEqual(session.isWatchDirty(), false)
  })

  test('parks release the serialization turn and reacquire before resuming', async () => {
    const server = new RedisServerState()
    const registry = createRedisCommandRegistry()
    let parked = false
    let releaseBlocked!: (value: Buffer | null) => void
    const parkedSignal = defer<void>()

    registry.register(
      defineCommand({
        name: 'block-once',
        schema: t.object({}),
        flags: ['blocking'],
        capabilities: { blocking: true },
        keys: () => [],
        execute: async (_args, ctx) => {
          const value = await ctx.park({
            waitFor: new Promise<Buffer | null>(resolve => {
              releaseBlocked = resolve
              parked = true
              parkedSignal.resolve()
            }),
            signal: ctx.signal,
          })

          return RedisResult.create(RedisValue.bulkString(value))
        },
      }),
    )

    const executor = new CommandExecutor({ registry })
    const first = new ClientSession({ server, executor })
    const second = new ClientSession({ server, executor })

    const blocked = first.execute('block-once', [])
    await parkedSignal.promise
    assert.strictEqual(parked, true)

    const writeWhileParked = await withTimeout(
      second.execute('set', [Buffer.from('key'), Buffer.from('value')]),
    )
    assert.deepStrictEqual(writeWhileParked, RedisResult.ok())

    releaseBlocked(Buffer.from('done'))
    assert.deepStrictEqual(
      await withTimeout(blocked),
      RedisResult.create(RedisValue.bulkString(Buffer.from('done'))),
    )
  })

  test('serializes sessions on the same database by default', async () => {
    const registry = createRedisCommandRegistry()
    let releaseHeld!: () => void
    const entered = defer<void>()

    registry.register(
      defineCommand({
        name: 'hold-turn',
        schema: t.object({}),
        flags: ['readonly'],
        keys: () => [],
        execute: async () => {
          entered.resolve()
          await new Promise<void>(resolve => {
            releaseHeld = resolve
          })
          return RedisResult.ok()
        },
      }),
    )

    const server = new RedisServerState()
    const executor = new CommandExecutor({ registry })
    const first = new ClientSession({ server, executor })
    const second = new ClientSession({ server, executor })

    const held = first.execute('hold-turn', [])
    await entered.promise

    const competing = second.execute('set', [
      Buffer.from('key'),
      Buffer.from('value'),
    ])
    assert.strictEqual(await settlesWithin(competing, 20), false)

    releaseHeld()
    assert.deepStrictEqual(await withTimeout(held), RedisResult.ok())
    assert.deepStrictEqual(await withTimeout(competing), RedisResult.ok())
  })
})

const streamCommand = defineCommand({
  name: 'stream',
  schema: t.object({}),
  flags: ['pubsub'],
  capabilities: { pushOnly: true },
  keys: () => [],
  execute: () => createSingleFrameStream(),
})

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

function defer<TValue>() {
  let resolve!: (value: TValue | PromiseLike<TValue>) => void
  let reject!: (reason?: unknown) => void
  const promise = new Promise<TValue>((promiseResolve, promiseReject) => {
    resolve = promiseResolve
    reject = promiseReject
  })

  return { promise, resolve, reject }
}

async function withTimeout<TValue>(
  promise: Promise<TValue>,
  timeoutMs = 100,
): Promise<TValue> {
  let timer: NodeJS.Timeout | undefined
  try {
    return await Promise.race([
      promise,
      new Promise<never>((_resolve, reject) => {
        timer = setTimeout(() => {
          reject(new Error('Timed out waiting for command'))
        }, timeoutMs)
      }),
    ])
  } finally {
    if (timer) {
      clearTimeout(timer)
    }
  }
}

async function settlesWithin<TValue>(
  promise: Promise<TValue>,
  timeoutMs: number,
): Promise<boolean> {
  return Promise.race([
    promise.then(() => true),
    new Promise<false>(resolve => {
      setTimeout(() => resolve(false), timeoutMs)
    }),
  ])
}
