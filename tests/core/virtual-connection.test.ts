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
})
