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
  let port: number
  let publisher: Socket
  const serverSockets = new Set<Socket>()

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
    server = new Resp2Server({
      server: new RedisServerState(),
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
})
