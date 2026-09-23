import { describe, test } from 'node:test'
import assert from 'node:assert'
import { once } from 'node:events'
import { duplexPair, type Duplex } from 'node:stream'
import {
  RedisServerState,
  attachSession,
  createRedisCommandExecutor,
  createVirtualConnection,
} from '../../src/internal'
import { SocketConnectionTransport } from '../../src/core/transports/socket-connection-transport'
import { commandFrame } from '../shared-test-helpers'

/**
 * SocketConnectionTransport takes any Duplex. These use a plain duplexPair at
 * its default 16 KiB high-water mark — a stream with real backpressure, unlike
 * createVirtualConnection's unbounded wire — so a paused client makes the
 * server's writes back up, the same as a TCP peer that stops reading.
 */

const MONITOR_OK = '+OK\r\n'
const SETS = 40
const PAYLOAD = 'x'.repeat(256 * 1024)

async function settlesWithin(
  promise: Promise<unknown>,
  timeoutMs: number,
): Promise<boolean> {
  let timer: NodeJS.Timeout | undefined
  try {
    return await Promise.race([
      promise.then(() => true),
      new Promise<false>(resolve => {
        timer = setTimeout(() => resolve(false), timeoutMs)
      }),
    ])
  } finally {
    if (timer) {
      clearTimeout(timer)
    }
  }
}

function readUntil(stream: Duplex, pattern: string): Promise<void> {
  return new Promise(resolve => {
    let seen = ''
    const onData = (chunk: Buffer) => {
      seen += chunk.toString('latin1')
      if (seen.includes(pattern)) {
        stream.off('data', onData)
        resolve()
      }
    }
    stream.on('data', onData)
  })
}

/**
 * A MONITOR client on a bounded pair, paused with the server's output backed
 * up behind it. Traffic comes from a second, reading connection.
 */
async function pausedMonitorWithBackedUpOutput() {
  const state = new RedisServerState({ databaseCount: 16 })
  const executor = createRedisCommandExecutor()

  const [client, server] = duplexPair()
  // A raw duplexPair on Node 22 never tells one end that the other was
  // destroyed — nothing any transport could observe. Node 24 pushes EOF to the
  // survivor on a clean destroy, as a TCP FIN would; wire that here so the
  // destroy() case runs the same on both (pushing EOF twice is a no-op).
  client.once('close', () => {
    if (!server.destroyed) {
      server.push(null)
    }
  })

  const attached = attachSession(new SocketConnectionTransport(server), {
    state,
    executor,
  })

  const confirmed = readUntil(client, MONITOR_OK)
  client.write(commandFrame('MONITOR'))
  await confirmed
  client.pause()

  const writer = createVirtualConnection({ state, executor })
  await once(writer.clientSocket, 'connect')
  const pong = readUntil(writer.clientSocket, '+PONG\r\n')
  for (let i = 0; i < SETS; i++) {
    writer.clientSocket.write(commandFrame('SET', `k${i}`, PAYLOAD))
  }
  writer.clientSocket.write(commandFrame('PING'))
  await pong

  // The server has more to send than the paused client's buffer holds.
  assert.ok(
    client.readableLength < SETS * PAYLOAD.length,
    'expected the server output to be backed up',
  )
  assert.strictEqual(state.getConnectedClients().length, 2)

  return { state, client, server, attached, writer }
}

describe('attachSession over a bounded Duplex — client closes first', () => {
  for (const [name, closeClient] of [
    ['end()', (client: Duplex) => client.end()],
    ['destroy()', (client: Duplex) => client.destroy()],
  ] as const) {
    test(`client ${name} with backed-up output: the session ends promptly`, async () => {
      const { state, client, server, attached, writer } =
        await pausedMonitorWithBackedUpOutput()

      // Redis frees a client whose connection hits EOF, dropping output it
      // has not flushed (main did the same over TCP). Waiting on that output
      // instead parks the session behind a peer that will never read it.
      closeClient(client)

      const settled = await settlesWithin(attached.done, 2000)
      const sessionsLeft = state.getConnectedClients().length
      const serverLeaked = !server.destroyed

      writer.close()
      client.destroy()
      server.destroy()

      assert.strictEqual(settled, true, 'session teardown hung')
      // Only the writer's session remains.
      assert.strictEqual(sessionsLeft, 1)
      assert.strictEqual(serverLeaked, false, 'server end left open')
    })
  }
})
