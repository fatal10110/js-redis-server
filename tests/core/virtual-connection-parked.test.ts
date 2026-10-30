import { describe, test } from 'node:test'
import assert from 'node:assert'
import { once } from 'node:events'
import {
  RedisServerState,
  createRedisCommandExecutor,
} from '../../src/internal'
import { isClientBlocked } from '../../src/commands/blocking'
import { createVirtualConnection } from '../../src/core/transports/virtual-connection'
import { commandFrame } from '../shared-test-helpers'

/**
 * A client on the in-process wire parked in a blocking command that ends or
 * destroys its socket is freed, and the next push is not consumed on its
 * behalf (#448, fixed by #554).
 */

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
    if (timer) clearTimeout(timer)
  }
}

async function until(
  condition: () => boolean,
  label: string,
  timeoutMs = 2000,
): Promise<void> {
  const deadline = Date.now() + timeoutMs
  while (!condition()) {
    assert.ok(Date.now() < deadline, `timed out waiting for ${label}`)
    await new Promise(resolve => setTimeout(resolve, 5))
  }
}

async function connect(state: RedisServerState) {
  const connection = createVirtualConnection({
    state,
    executor: createRedisCommandExecutor(),
  })
  await once(connection.clientSocket, 'connect')
  return connection
}

type Connection = Awaited<ReturnType<typeof connect>>

/** Send one command and read its fixed-size reply. */
async function roundTrip(
  connection: Connection,
  args: string[],
  expected: string,
): Promise<void> {
  connection.clientSocket.write(commandFrame(...args))
  const reply = await within(
    readBytes(connection.clientSocket, Buffer.byteLength(expected)),
    `the reply to ${args[0]}`,
  )
  assert.strictEqual(reply.toString(), expected)
}

const blockedCount = (state: RedisServerState) =>
  state.getConnectedClients().filter(isClientBlocked).length

const kinds = [
  {
    name: 'BLPOP',
    park: (key: string) => commandFrame('BLPOP', key, '0'),
    push: (key: string) => ['RPUSH', key, 'v'],
    pushReply: ':1\r\n',
    length: 'LLEN',
    wake: (key: string) => `*2\r\n$${key.length}\r\n${key}\r\n$1\r\nv\r\n`,
  },
  {
    name: 'XREAD BLOCK',
    park: (key: string) =>
      commandFrame('XREAD', 'BLOCK', '0', 'STREAMS', key, '$'),
    push: (key: string) => ['XADD', key, '1-1', 'f', 'v'],
    pushReply: '$3\r\n1-1\r\n',
    length: 'XLEN',
    wake: (key: string) =>
      `*1\r\n*2\r\n$${key.length}\r\n${key}\r\n*1\r\n*2\r\n$3\r\n1-1\r\n*2\r\n$1\r\nf\r\n$1\r\nv\r\n`,
  },
] as const

const hangUps = [
  [
    'end(cb)',
    async (connection: Connection) => {
      const ended = new Promise<Error | null | undefined>(resolve =>
        connection.clientSocket.end(resolve),
      )
      // A client that does not read never sees the server's EOF, and so
      // never closes: drain it, as a real socket consumer does.
      connection.clientSocket.resume()
      // Called back without an error (Node passes null for "no error").
      assert.strictEqual(
        (await within(ended, 'the end() callback')) ?? null,
        null,
      )
    },
  ],
  [
    'destroy()',
    async (connection: Connection) => {
      connection.clientSocket.destroy()
    },
  ],
] as const

describe('virtual connection with a parked blocking command', () => {
  for (const kind of kinds) {
    for (const [how, hangUp] of hangUps) {
      test(`${kind.name} parked, then ${how}: the waiter is gone and a later push stays`, async () => {
        const state = new RedisServerState({ databaseCount: 16 })
        const parked = await connect(state)
        const pusher = await connect(state)
        const key = 'parked-key'
        assert.strictEqual(state.getConnectedClients().length, 2)

        parked.clientSocket.write(kind.park(key))
        await until(() => blockedCount(state) === 1, `${kind.name} to park`)

        const closed = once(parked.clientSocket, 'close')
        await hangUp(parked)
        await within(parked.done, 'the parked session to settle')
        await within(closed, 'the client socket to close')

        assert.strictEqual(state.getConnectedClients().length, 1)
        assert.strictEqual(blockedCount(state), 0)

        await roundTrip(pusher, kind.push(key) as string[], kind.pushReply)
        await roundTrip(pusher, [kind.length, key], ':1\r\n')

        pusher.close()
        await pusher.done
      })

      test(`two ${kind.name} waiters on one key: the first ends with ${how}, the push goes to the second`, async () => {
        const state = new RedisServerState({ databaseCount: 16 })
        const first = await connect(state)
        const second = await connect(state)
        const pusher = await connect(state)
        const key = 'parked-key'

        first.clientSocket.write(kind.park(key))
        await until(() => blockedCount(state) === 1, 'the first to park')
        second.clientSocket.write(kind.park(key))
        await until(() => blockedCount(state) === 2, 'the second to park')

        await hangUp(first)
        await within(first.done, 'the first session to settle')
        assert.strictEqual(blockedCount(state), 1)

        await roundTrip(pusher, kind.push(key) as string[], kind.pushReply)
        const wake = kind.wake(key)
        const reply = await within(
          readBytes(second.clientSocket, Buffer.byteLength(wake)),
          'the second waiter to wake',
        )
        assert.strictEqual(reply.toString(), wake)
        assert.strictEqual(blockedCount(state), 0)

        second.close()
        pusher.close()
        await Promise.all([second.done, pusher.done])
      })
    }
  }
})
