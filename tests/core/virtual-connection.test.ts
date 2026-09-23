import { describe, test } from 'node:test'
import assert from 'node:assert'
import { once } from 'node:events'
import {
  RedisServerState,
  createRedisCommandExecutor,
} from '../../src/internal'
import { createVirtualConnection } from '../../src/core/transports/virtual-connection'
import { commandFrame } from '../shared-test-helpers'

/** Read exactly `byteLength` bytes off the client socket, buffering chunks. */
async function readBytes(
  socket: NodeJS.ReadableStream,
  byteLength: number,
): Promise<Buffer> {
  const chunks: Buffer[] = []
  let total = 0
  while (total < byteLength) {
    const chunk = (socket.read() as Buffer | null) ?? null
    if (chunk) {
      chunks.push(chunk)
      total += chunk.length
      continue
    }
    await once(socket, 'readable')
  }
  return Buffer.concat(chunks)
}

function freshPipeline() {
  const state = new RedisServerState({ databaseCount: 16 })
  const executor = createRedisCommandExecutor()
  return { state, executor }
}

/**
 * Poll until the socket has buffered `byteLength` bytes, without ever reading
 * from it — the point being to leave the peer non-draining.
 */
async function waitForBuffered(
  socket: { readableLength: number },
  byteLength: number,
  timeoutMs = 2000,
): Promise<void> {
  const deadline = Date.now() + timeoutMs
  while (socket.readableLength < byteLength) {
    if (Date.now() > deadline) {
      assert.fail(
        `timed out: buffered ${socket.readableLength} of ${byteLength} bytes — ` +
          'the server stalled waiting for the client to read',
      )
    }
    await new Promise(resolve => setTimeout(resolve, 5))
  }
}

describe('createVirtualConnection', () => {
  test('emits connect on next tick and round-trips RESP bytes', async () => {
    const { state, executor } = freshPipeline()
    const { clientSocket, close } = createVirtualConnection({ state, executor })

    // ioredis StandaloneConnector resolves the stream then waits for 'connect'.
    await once(clientSocket, 'connect')

    clientSocket.write(commandFrame('SET', 'k', 'v'))
    const setReply = await readBytes(clientSocket, '+OK\r\n'.length)
    assert.strictEqual(setReply.toString(), '+OK\r\n')

    clientSocket.write(commandFrame('GET', 'k'))
    const getReply = await readBytes(clientSocket, '$1\r\nv\r\n'.length)
    assert.strictEqual(getReply.toString(), '$1\r\nv\r\n')

    close()
  })

  test('exposes net.Socket-shaped no-op methods used by ioredis', async () => {
    const { state, executor } = freshPipeline()
    const { clientSocket, close } = createVirtualConnection({ state, executor })

    // These must exist and be chainable/no-throw — ioredis calls them on its
    // stream during setup. They must not throw.
    assert.strictEqual(typeof clientSocket.setNoDelay, 'function')
    assert.strictEqual(typeof clientSocket.setKeepAlive, 'function')
    assert.strictEqual(typeof clientSocket.setTimeout, 'function')
    assert.strictEqual(typeof clientSocket.ref, 'function')
    assert.strictEqual(typeof clientSocket.unref, 'function')
    assert.doesNotThrow(() => {
      clientSocket.setNoDelay(true)
      clientSocket.setKeepAlive(true, 0)
      clientSocket.setTimeout(0)
      clientSocket.ref()
      clientSocket.unref()
    })
    assert.strictEqual(typeof clientSocket.remoteAddress, 'string')
    assert.strictEqual(typeof clientSocket.remotePort, 'number')

    close()
  })

  test('keeps writing to a client that never reads (backpressure-free)', async () => {
    const { state, executor } = freshPipeline()
    const value = Buffer.alloc(1024 * 1024, 0x78)
    state.getDatabase(0).setString(Buffer.from('big'), value)

    const { clientSocket, close } = createVirtualConnection({ state, executor })
    await once(clientSocket, 'connect')

    // Nothing is attached to 'data' and nothing calls read(), so the client end
    // stays paused and never drains. An in-process server must not block on
    // that: a duplexPair parks its write callback until the peer reads, so at
    // the default 16 KiB high-water mark the adapter's awaited write chain
    // stalls mid-reply and PING is never answered.
    clientSocket.write(
      Buffer.concat([commandFrame('GET', 'big'), commandFrame('PING')]),
    )

    const expected = Buffer.concat([
      Buffer.from(`$${value.length}\r\n`),
      value,
      Buffer.from('\r\n'),
      Buffer.from('+PONG\r\n'),
    ])
    await waitForBuffered(clientSocket, expected.length)

    const replies = await readBytes(clientSocket, expected.length)
    assert.strictEqual(replies.length, expected.length)
    assert.ok(replies.equals(expected), 'reply bytes differ')

    close()
  })

  test('close() tears down the server session (no leaked sessions)', async () => {
    const { state, executor } = freshPipeline()
    assert.strictEqual(state.getConnectedClients().length, 0)

    const { clientSocket, close, done } = createVirtualConnection({
      state,
      executor,
    })
    await once(clientSocket, 'connect')

    assert.strictEqual(state.getConnectedClients().length, 1)

    close()
    // The adapter loop tears the session down in its finally; await it.
    await done
    assert.strictEqual(state.getConnectedClients().length, 0)
  })

  test('destroying the client socket tears down the server session', async () => {
    const { state, executor } = freshPipeline()
    const { clientSocket, done } = createVirtualConnection({ state, executor })
    await once(clientSocket, 'connect')
    assert.strictEqual(state.getConnectedClients().length, 1)

    clientSocket.destroy()
    // Await the adapter loop settling rather than a single tick — robust if the
    // disposal chain ever grows an extra await.
    await done

    assert.strictEqual(state.getConnectedClients().length, 0)
  })

  test('tearing down with an active SUBSCRIBE settles cleanly', async () => {
    const { state, executor } = freshPipeline()
    const { clientSocket, close, done } = createVirtualConnection({
      state,
      executor,
    })
    await once(clientSocket, 'connect')

    // Open a push stream over the virtual wire, then read the subscribe
    // confirmation so the subscription is established server-side.
    clientSocket.write(commandFrame('SUBSCRIBE', 'ch'))
    const confirmation = await readBytes(
      clientSocket,
      '*3\r\n$9\r\nsubscribe\r\n$2\r\nch\r\n:1\r\n'.length,
    )
    assert.match(confirmation.toString(), /subscribe/)
    assert.strictEqual(state.getConnectedClients().length, 1)

    close()
    // The adapter's finally must drain the active push stream; if it didn't,
    // `done` would never settle and this test would time out.
    await done
    assert.strictEqual(state.getConnectedClients().length, 0)
  })

  test('the bridge attaches no error listener of its own', () => {
    const { state, executor } = freshPipeline()
    const { clientSocket } = createVirtualConnection({ state, executor })

    // A permanent listener would silently swallow errors for a consumer that
    // has no handler, where a net.Socket would surface an unhandled 'error'.
    assert.strictEqual(clientSocket.listenerCount('error'), 0)
    clientSocket.destroy()
  })

  test('address props are read-only but still stubbable', () => {
    const { state, executor } = freshPipeline()
    const { clientSocket } = createVirtualConnection({ state, executor })

    // Reflect.set reports the refused assignment regardless of strict mode.
    assert.strictEqual(Reflect.set(clientSocket, 'remoteAddress', 'x'), false)
    assert.strictEqual(clientSocket.remoteAddress, '127.0.0.1')

    // sinon-style stubbing redefines the property rather than assigning it.
    Object.defineProperty(clientSocket, 'remoteAddress', { value: '10.0.0.1' })
    assert.strictEqual(clientSocket.remoteAddress, '10.0.0.1')
    clientSocket.destroy()
  })
})

type Recorded = { events: string[]; bytes(): string; closed: Promise<void> }

/** Record the client-visible event sequence, collapsing runs of 'data'. */
function record(socket: NodeJS.EventEmitter): Recorded {
  const events: string[] = []
  let bytes = ''
  const closed = new Promise<void>(resolve => {
    socket.once('close', () => resolve())
  })

  socket.on('connect', () => events.push('connect'))
  socket.on('end', () => events.push('end'))
  socket.on('finish', () => events.push('finish'))
  socket.on('close', () => events.push('close'))
  socket.on('error', (err: Error) => events.push(`error:${err.message}`))
  socket.on('data', (chunk: Buffer) => {
    bytes += chunk.toString()
    if (events.at(-1) !== 'data') {
      events.push('data')
    }
  })

  return { events, bytes: () => bytes, closed }
}

/** Fail with a clear message rather than hanging the test runner. */
async function within<T>(
  promise: Promise<T>,
  label: string,
  timeoutMs = 2000,
): Promise<T> {
  let timer: NodeJS.Timeout | undefined
  try {
    return await Promise.race([
      promise,
      new Promise<never>((_resolve, reject) => {
        timer = setTimeout(
          () => reject(new Error(`timed out waiting for ${label}`)),
          timeoutMs,
        )
      }),
    ])
  } finally {
    if (timer) {
      clearTimeout(timer)
    }
  }
}

/**
 * Server-initiated teardown, driven two ways: the client sends QUIT, or the
 * owner calls close() while a reply sits unread.
 */
type Trigger = {
  name: string
  reply: string
  run(conn: ReturnType<typeof createVirtualConnection>): Promise<void>
}

const TRIGGERS: Trigger[] = [
  {
    name: 'QUIT',
    reply: '+OK\r\n',
    run: async conn => {
      conn.clientSocket.write(commandFrame('QUIT'))
    },
  },
  {
    name: 'close() with an unread reply',
    reply: '+PONG\r\n',
    run: async conn => {
      conn.clientSocket.write(commandFrame('PING'))
      await waitForBuffered(conn.clientSocket, '+PONG\r\n'.length)
      conn.close()
    },
  },
]

describe('createVirtualConnection — server-side half-close', () => {
  // (c) A client that reads as data arrives — what ioredis and node-redis do.
  const flowingCases: Array<{ name: string; send: Buffer; reply: RegExp }> = [
    {
      name: 'QUIT',
      send: commandFrame('QUIT'),
      reply: /^\+OK\r\n$/,
    },
    {
      name: 'a protocol error',
      send: Buffer.from('*1\r\n$abc\r\n'),
      reply: /^-ERR Protocol error/,
    },
    {
      name: 'a pipelined SET / GET / QUIT',
      send: Buffer.concat([
        commandFrame('SET', 'k', 'v'),
        commandFrame('GET', 'k'),
        commandFrame('QUIT'),
      ]),
      reply: /^\+OK\r\n\$1\r\nv\r\n\+OK\r\n$/,
    },
  ]

  for (const { name, send, reply } of flowingCases) {
    test(`${name}: a reading client sees [connect, data, end, finish, close]`, async () => {
      const { state, executor } = freshPipeline()
      const conn = createVirtualConnection({ state, executor })
      const seen = record(conn.clientSocket)
      await once(conn.clientSocket, 'connect')

      conn.clientSocket.write(send)
      await within(conn.done, 'the session to end')
      await within(seen.closed, 'the client to close')

      // Real Redis 7.2 gives [connect, end, close] around the reply bytes. The
      // 'finish' between them is net.Socket's allowHalfOpen: false: having read
      // the server's EOF, the client ends its own writable before closing.
      assert.deepStrictEqual(seen.events, [
        'connect',
        'data',
        'end',
        'finish',
        'close',
      ])
      assert.match(seen.bytes(), reply)
      assert.strictEqual(state.getConnectedClients().length, 0)
    })
  }

  test('close() with nothing to send: a reading client sees [connect, end, finish, close]', async () => {
    const { state, executor } = freshPipeline()
    const conn = createVirtualConnection({ state, executor })
    const seen = record(conn.clientSocket)
    await once(conn.clientSocket, 'connect')

    conn.close()
    await within(conn.done, 'the session to end')
    await within(seen.closed, 'the client to close')

    assert.deepStrictEqual(seen.events, ['connect', 'end', 'finish', 'close'])
    assert.strictEqual(conn.clientSocket.destroyed, true)
  })

  // (a) A client that is not reading at the moment the server closes.
  for (const trigger of TRIGGERS) {
    test(`${trigger.name}: a paused client that resumes late gets every byte, then end, then close`, async () => {
      const { state, executor } = freshPipeline()
      const conn = createVirtualConnection({ state, executor })
      await once(conn.clientSocket, 'connect')

      await trigger.run(conn)
      await within(conn.done, 'the session to end')
      assert.strictEqual(state.getConnectedClients().length, 0)

      // Destroying a paused stream strands its buffer: it never starts emitting
      // 'data'. So the client end must still be alive, holding the reply.
      assert.strictEqual(conn.clientSocket.destroyed, false)
      assert.strictEqual(conn.clientSocket.readableLength, trigger.reply.length)

      const seen = record(conn.clientSocket)
      conn.clientSocket.resume()
      await within(seen.closed, 'the client to close after resuming')

      assert.strictEqual(seen.bytes(), trigger.reply)
      assert.deepStrictEqual(seen.events, ['data', 'end', 'finish', 'close'])
    })
  }

  // (b) A client that never reads at all.
  for (const trigger of TRIGGERS) {
    test(`${trigger.name}: a client that never reads leaves no server-side state behind`, async () => {
      const { state, executor } = freshPipeline()
      const conn = createVirtualConnection({ state, executor })
      await once(conn.clientSocket, 'connect')

      await trigger.run(conn)

      // The session must end promptly without any help from the client.
      await within(conn.done, 'the session to end', 500)
      assert.strictEqual(state.getConnectedClients().length, 0)

      // Give anything still pending a chance to misbehave.
      await new Promise(resolve => setTimeout(resolve, 50))
      assert.strictEqual(state.getConnectedClients().length, 0)

      // Only the consumer's own socket object stays half-open, holding the
      // unread reply — exactly what a real TCP socket does.
      assert.strictEqual(conn.clientSocket.destroyed, false)
      assert.strictEqual(conn.clientSocket.readableLength, trigger.reply.length)

      // …and its owner can still dispose of it cleanly.
      const closed = new Promise(resolve =>
        conn.clientSocket.once('close', resolve),
      )
      conn.clientSocket.destroy()
      await within(closed, 'the client to close on destroy()')
      assert.strictEqual(state.getConnectedClients().length, 0)
    })
  }
})

describe('createVirtualConnection — client-side half-close', () => {
  test('client end() first: the server closes too, and the client sees finish, end, close', async () => {
    const { state, executor } = freshPipeline()
    const conn = createVirtualConnection({ state, executor })
    const seen = record(conn.clientSocket)
    await once(conn.clientSocket, 'connect')

    // What ioredis disconnect() does. Redis drops a client whose connection
    // hits EOF, so the server must answer with its own FIN — not just vanish.
    conn.clientSocket.end()

    await within(conn.done, 'the session to end')
    await within(seen.closed, 'the client to close')

    assert.deepStrictEqual(seen.events, ['connect', 'finish', 'end', 'close'])
    assert.strictEqual(state.getConnectedClients().length, 0)
  })

  test('SUBSCRIBE a b c and end() in the same tick: all three confirmations arrive', async () => {
    const { state, executor } = freshPipeline()
    const conn = createVirtualConnection({ state, executor })
    const seen = record(conn.clientSocket)
    await once(conn.clientSocket, 'connect')

    // The confirmations are already queued when the client's EOF is read. main
    // (and 4127ed6) deliver all three; tearing down on EOF must not cut the
    // background drain off after the first.
    conn.clientSocket.write(commandFrame('SUBSCRIBE', 'a', 'b', 'c'))
    conn.clientSocket.end()

    await within(conn.done, 'the session to end')
    await within(seen.closed, 'the client to close')

    assert.strictEqual(
      seen.bytes(),
      ['a', 'b', 'c']
        .map(
          (channel, i) =>
            `*3\r\n$9\r\nsubscribe\r\n$1\r\n${channel}\r\n:${i + 1}\r\n`,
        )
        .join(''),
    )
    assert.strictEqual(state.getConnectedClients().length, 0)
  })

  test('client end() after a pipelined command: the reply still arrives', async () => {
    const { state, executor } = freshPipeline()
    const conn = createVirtualConnection({ state, executor })
    const seen = record(conn.clientSocket)
    await once(conn.clientSocket, 'connect')

    conn.clientSocket.write(commandFrame('PING'))
    conn.clientSocket.end()

    await within(conn.done, 'the session to end')
    await within(seen.closed, 'the client to close')

    assert.strictEqual(seen.bytes(), '+PONG\r\n')
    // The relative order of 'data' and 'finish' depends on when the server
    // consumes the EOF, as it does on a real socket; the invariants are that
    // every reply byte precedes 'end', and 'close' comes last.
    assert.ok(seen.events.includes('finish'), `no finish: ${seen.events}`)
    assert.ok(
      seen.events.indexOf('data') < seen.events.indexOf('end'),
      `data after end: ${seen.events}`,
    )
    assert.strictEqual(seen.events.at(-1), 'close')
  })
})

describe('createVirtualConnection — writing after the server has closed', () => {
  const UNREAD = '+PONG\r\n+OK\r\n'

  /**
   * PING + QUIT without reading, then wait until the server end is gone,
   * leaving the client half-open with the reply unread.
   */
  async function halfOpenAfterQuit() {
    const { state, executor } = freshPipeline()
    const conn = createVirtualConnection({ state, executor })
    await once(conn.clientSocket, 'connect')

    conn.clientSocket.write(
      Buffer.concat([commandFrame('PING'), commandFrame('QUIT')]),
    )
    await within(conn.done, 'the session to end')
    // The server end is destroyed on the immediate after close(); let it land.
    await new Promise(resolve => setTimeout(resolve, 20))

    assert.strictEqual(conn.clientSocket.destroyed, false)
    assert.strictEqual(conn.clientSocket.readableLength, UNREAD.length)
    return { state, conn }
  }

  test('the first write is accepted, and the unread reply and EOF still arrive', async () => {
    const { conn } = await halfOpenAfterQuit()
    const errors: string[] = []
    conn.clientSocket.on('error', err =>
      errors.push((err as NodeJS.ErrnoException).code ?? err.message),
    )

    // Real TCP: the kernel accepts the first write to a closed peer, and the
    // client still reads the reply and EOF (pinned with a net.Socket probe).
    // Failing the write would destroy the client and discard that reply. The
    // callback must still settle — a duplexPair end whose peer is gone would
    // otherwise never call back at all.
    const written = new Promise<Error | null | undefined>(resolve =>
      conn.clientSocket.write(commandFrame('PING'), resolve),
    )
    assert.strictEqual(
      (await within(written, 'the write callback')) ?? null,
      null,
    )
    assert.strictEqual(conn.clientSocket.destroyed, false)

    const seen = record(conn.clientSocket)
    conn.clientSocket.resume()
    await within(seen.closed, 'the client to close after reading')

    assert.strictEqual(seen.bytes(), UNREAD)
    assert.deepStrictEqual(seen.events, ['data', 'end', 'finish', 'close'])
    assert.deepStrictEqual(errors, [])
  })

  test("a write inside the client's 'end' handler is accepted, and a later one fails quietly", async () => {
    const { conn } = await halfOpenAfterQuit()
    const errors: string[] = []
    conn.clientSocket.on('error', err =>
      errors.push((err as NodeJS.ErrnoException).code ?? err.message),
    )

    // Real Redis 7.2 accepts this write (cb ok, then finish); failing it with
    // EPIPE would also emit 'error' and crash a consumer with no handler.
    let inEnd: Promise<Error | null | undefined> | undefined
    conn.clientSocket.once('end', () => {
      inEnd = new Promise(resolve =>
        conn.clientSocket.write(commandFrame('PING'), resolve),
      )
    })

    const seen = record(conn.clientSocket)
    conn.clientSocket.resume()
    await within(seen.closed, 'the client to close')

    assert.ok(inEnd, "no write was made in the 'end' handler")
    assert.strictEqual(
      (await within(inEnd, 'the write callback')) ?? null,
      null,
    )
    assert.strictEqual(seen.bytes(), UNREAD)
    assert.deepStrictEqual(seen.events, ['data', 'end', 'finish', 'close'])

    // By now allowHalfOpen: false has ended the writable, so a further write
    // fails its callback — ERR_STREAM_WRITE_AFTER_END, where a net.Socket says
    // EPIPE — and, like a net.Socket, emits no 'error'.
    const after = new Promise<NodeJS.ErrnoException | null | undefined>(
      resolve => conn.clientSocket.write(commandFrame('PING'), resolve),
    )
    assert.strictEqual(
      (await within(after, 'the late write callback'))?.code,
      'ERR_STREAM_WRITE_AFTER_END',
    )
    await new Promise(resolve => setImmediate(resolve))
    assert.deepStrictEqual(errors, [])
  })

  test('end(cb) calls back, and the unread reply is still delivered', async () => {
    const { conn } = await halfOpenAfterQuit()

    const ended = new Promise<Error | null | undefined>(resolve =>
      conn.clientSocket.end(resolve),
    )
    // Called back without an error (Node passes null for "no error").
    assert.strictEqual(
      (await within(ended, 'the end() callback')) ?? null,
      null,
    )

    const seen = record(conn.clientSocket)
    conn.clientSocket.resume()
    await within(seen.closed, 'the client to close')

    assert.strictEqual(seen.bytes(), UNREAD)
    assert.deepStrictEqual(seen.events, ['data', 'end', 'close'])
  })
})

describe('createVirtualConnection — errored teardown', () => {
  // On Node 22 a duplexPair does not propagate teardown at all; on Node 24 it
  // destroys the peer on an errored destroy, but without the error. The bridge
  // matches Node 24 on both, so this runs the same everywhere.
  test("a client destroy(err) ends the session, and the error stays the client's own", async () => {
    const { state, executor } = freshPipeline()
    const conn = createVirtualConnection({ state, executor })
    await once(conn.clientSocket, 'connect')

    const errors: string[] = []
    conn.clientSocket.on('error', err => errors.push(err.message))
    const closed = new Promise(resolve =>
      conn.clientSocket.once('close', resolve),
    )

    conn.clientSocket.destroy(new Error('boom'))

    await within(conn.done, 'the session to end')
    await within(closed, 'the client to close')
    assert.strictEqual(state.getConnectedClients().length, 0)
    assert.deepStrictEqual(errors, ['boom'])
  })
})
