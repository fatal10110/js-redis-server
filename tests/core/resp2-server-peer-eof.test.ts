import { describe, test, before, after } from 'node:test'
import assert from 'node:assert'
import { connect, type Socket } from 'node:net'
import { once } from 'node:events'
import {
  RedisServerState,
  Resp2Server,
  createRedisCommandExecutor,
} from '../../src/internal'
import { commandFrame } from '../shared-test-helpers'

/** Well past loopback socket buffers on both ends, so server output backs up. */
const MESSAGES = 64
const PAYLOAD = 'x'.repeat(512 * 1024)

describe('Resp2Server — a client that sends FIN with output backed up', () => {
  let server: Resp2Server
  let state: RedisServerState
  let port: number
  let publisher: Socket
  const serverSockets = new Set<Socket>()

  /** Poll until `read()` returns `expected`; resolves with the last value. */
  async function waitFor(
    read: () => number | Promise<number>,
    expected: number,
    timeoutMs = 2000,
  ): Promise<number> {
    const deadline = Date.now() + timeoutMs
    let value = await read()
    while (value !== expected && Date.now() < deadline) {
      await new Promise(resolve => setTimeout(resolve, 20))
      value = await read()
    }
    return value
  }

  const connections = () =>
    new Promise<number>((resolve, reject) =>
      server.server.getConnections((err, count) =>
        err ? reject(err) : resolve(count),
      ),
    )

  /** Poll until the server's socket count reaches `expected`. */
  async function waitForConnections(
    expected: number,
    timeoutMs = 2000,
  ): Promise<number> {
    const deadline = Date.now() + timeoutMs
    let count = await connections()
    while (count !== expected && Date.now() < deadline) {
      await new Promise(resolve => setTimeout(resolve, 20))
      count = await connections()
    }
    return count
  }

  async function open(): Promise<Socket> {
    const socket = connect(port, '127.0.0.1')
    await once(socket, 'connect')
    return socket
  }

  /** Collect bytes off a flowing socket until `pattern` shows up. */
  function readUntil(socket: Socket, pattern: string): Promise<string> {
    return new Promise(resolve => {
      let seen = ''
      const onData = (chunk: Buffer) => {
        seen += chunk.toString('latin1')
        if (seen.includes(pattern)) {
          socket.off('data', onData)
          resolve(seen)
        }
      }
      socket.on('data', onData)
    })
  }

  /** Run commands on the publisher and wait until all have been executed. */
  async function publisherRun(frames: Buffer[]): Promise<void> {
    const pong = readUntil(publisher, '+PONG\r\n')
    for (const frame of frames) {
      publisher.write(frame)
    }
    publisher.write(commandFrame('PING'))
    await pong
  }

  before(async () => {
    state = new RedisServerState()
    server = new Resp2Server({
      server: state,
      executor: createRedisCommandExecutor(),
    })
    server.server.on('connection', socket => {
      serverSockets.add(socket)
      socket.once('close', () => serverSockets.delete(socket))
    })
    await server.listen(0)
    port = server.getPort()
    publisher = await open()
  })

  after(async () => {
    publisher.destroy()
    // A leaked server socket would otherwise keep close() pending forever.
    for (const socket of serverSockets) {
      socket.destroy()
    }
    await server.close()
  })

  const cases = [
    {
      name: 'SUBSCRIBE',
      start: commandFrame('SUBSCRIBE', 'ch'),
      started: ':1\r\n',
      flood: () =>
        Array.from({ length: MESSAGES }, () =>
          commandFrame('PUBLISH', 'ch', PAYLOAD),
        ),
    },
    {
      name: 'MONITOR',
      start: commandFrame('MONITOR'),
      started: '+OK\r\n',
      flood: () =>
        Array.from({ length: MESSAGES }, (_, i) =>
          commandFrame('SET', `k${i}`, PAYLOAD),
        ),
    },
  ]

  for (const { name, start, started, flood } of cases) {
    test(`${name}: the server drops the connection promptly`, async () => {
      const baseline = await waitForConnections(1)
      assert.strictEqual(baseline, 1, 'only the publisher should be connected')

      const client = await open()
      const confirmed = readUntil(client, started)
      client.write(start)
      await confirmed

      // Stop reading, then flood: the server's writes to this client back up
      // behind a peer that no longer drains them.
      client.pause()
      await publisherRun(flood())
      assert.strictEqual(await connections(), baseline + 1)

      // FIN. Redis frees a client whose connection hits EOF, dropping output it
      // could not flush; main did the same. Waiting on that output instead
      // would hold this socket open for as long as the peer does not read.
      client.end()
      const after = await waitForConnections(baseline)

      client.destroy()
      assert.strictEqual(
        after,
        baseline,
        `server still holds ${after - baseline} connection(s) after the client's FIN`,
      )
    })
  }

  test('killed first by the server, then FIN: the socket still goes', async () => {
    assert.strictEqual(await waitForConnections(1), 1, 'publisher only')
    const killer = await open()
    const client = await open()

    const idReply = readUntil(client, '\r\n')
    client.write(commandFrame('CLIENT', 'ID'))
    const id = (await idReply).match(/^:(\d+)\r\n/)?.[1]
    assert.ok(id, 'no CLIENT ID reply')

    const confirmed = readUntil(client, '+OK\r\n')
    client.write(commandFrame('MONITOR'))
    await confirmed
    client.pause()
    await publisherRun(
      Array.from({ length: 16 }, (_, i) =>
        commandFrame('SET', `kill${i}`, 'x'.repeat(1024 * 1024)),
      ),
    )

    // The SERVER ends the connection first. That half-close waits for output
    // that cannot flush to a client that has stopped reading...
    const killed = readUntil(killer, ':1\r\n')
    killer.write(commandFrame('CLIENT', 'KILL', 'ID', id))
    await killed

    // ...and then the client sends FIN. Its EOF must still tear the socket
    // down, as on main; the earlier server-side close does not excuse it.
    client.end()
    const sessions = await waitFor(() => state.getConnectedClients().length, 2)
    const sockets = await waitForConnections(2)

    killer.destroy()
    client.destroy()
    // Publisher + killer: expected 2/2 (main and 4127ed6 give 2/2).
    assert.deepStrictEqual(
      { sessions, sockets },
      { sessions: 2, sockets: 2 },
      'the killed client left state behind',
    )
  })
})
