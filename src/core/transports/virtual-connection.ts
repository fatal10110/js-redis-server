import { duplexPair, type Duplex } from 'node:stream'
import type { CommandExecutor } from '../command-executor'
import type { Logger } from '../../logger'
import type { RedisClusterNodeRole, RedisServerState } from '../../state'
import { formatSocketAddressParts } from '../network-address'
import { attachSession } from './attach-session'
import { SocketConnectionTransport } from './socket-connection-transport'

export type CreateVirtualConnectionOptions = {
  state: RedisServerState
  executor: CommandExecutor
  nodeRole?: RedisClusterNodeRole
  logger?: Pick<Logger, 'error'>
  /** Synthetic remote address reported to the client lib. Default 127.0.0.1. */
  remoteAddress?: string
  /** Synthetic remote port reported to the client lib. Default 6379. */
  remotePort?: number
}

export type VirtualConnection = {
  /** The fake `net.Socket`-shaped stream handed to the client library. */
  clientSocket: VirtualClientSocket
  /** Resolves once the server-side adapter loop ends and the session is closed. */
  done: Promise<void>
  /** Tear down both ends of the wire and the server-side session. */
  close(): void
}

/**
 * The client-facing end of the virtual wire: one side of a
 * {@link duplexPair}, decorated with the `net.Socket` surface a client library
 * (ioredis) touches during setup — the `remoteAddress` / `remotePort` getters
 * and the socket-tuning no-ops. The `'connect'` event is emitted on
 * `nextTick`, like ioredis' `StandaloneConnector`.
 */
export type VirtualClientSocket = Duplex & {
  readonly remoteAddress: string
  readonly remotePort: number
  readonly localAddress: string
  readonly localPort: number
  setNoDelay(noDelay?: boolean): VirtualClientSocket
  setKeepAlive(enable?: boolean, initialDelay?: number): VirtualClientSocket
  setTimeout(timeout?: number, callback?: () => void): VirtualClientSocket
  ref(): VirtualClientSocket
  unref(): VirtualClientSocket
}

const DEFAULT_REMOTE_ADDRESS = '127.0.0.1'
const DEFAULT_REMOTE_PORT = 6379

let nextConnectionId = 0

/**
 * Build an in-process virtual connection: a fake client-facing `net.Socket`
 * already wired to a fresh `ClientSession`. The reusable primitive behind
 * `createIoredisMock` — no TCP socket, no port bind.
 *
 * The wire is a {@link duplexPair}: the client end is handed to the client
 * library, the server end is driven by a {@link SocketConnectionTransport} that
 * {@link attachSession} treats exactly like a real socket connection. Tearing
 * down either end (client `destroy()` or {@link VirtualConnection.close})
 * destroys the other, which aborts the adapter and closes the session.
 */
export function createVirtualConnection(
  opts: CreateVirtualConnectionOptions,
): VirtualConnection {
  const remoteAddress = opts.remoteAddress ?? DEFAULT_REMOTE_ADDRESS
  const remotePort = opts.remotePort ?? DEFAULT_REMOTE_PORT

  const [clientEnd, serverEnd] = duplexPair()
  const clientSocket = asVirtualClientSocket(
    clientEnd,
    remoteAddress,
    remotePort,
  )

  // Unlike a real socket pair, `duplexPair` does not propagate teardown between
  // its two ends — a destroyed side leaves the other waiting forever. Bridge it
  // both ways so closing either end ends the session.
  propagateDestroy(clientSocket, serverEnd)
  propagateDestroy(serverEnd, clientSocket)

  const transport = new SocketConnectionTransport(serverEnd, {
    id: `virtual-${++nextConnectionId}`,
  })

  const attached = attachSession(transport, {
    state: opts.state,
    executor: opts.executor,
    nodeRole: opts.nodeRole,
    logger: opts.logger,
    clientAddress: formatSocketAddressParts(remoteAddress, remotePort),
  })

  return {
    clientSocket,
    done: attached.done,
    close: () => attached.close(),
  }
}

function asVirtualClientSocket(
  stream: Duplex,
  remoteAddress: string,
  remotePort: number,
): VirtualClientSocket {
  const socket = stream as VirtualClientSocket
  const chainable = () => socket

  Object.assign(socket, {
    remoteAddress,
    remotePort,
    localAddress: DEFAULT_REMOTE_ADDRESS,
    localPort: 0,
    // net.Socket-shaped no-ops the client library calls during setup.
    setNoDelay: chainable,
    setKeepAlive: chainable,
    setTimeout: chainable,
    ref: chainable,
    unref: chainable,
  })

  // ioredis resolves the connector's promise and then waits for 'connect';
  // StandaloneConnector resolves on process.nextTick, so match that timing.
  process.nextTick(() => {
    if (!socket.destroyed) {
      socket.emit('connect')
      socket.emit('ready')
    }
  })

  return socket
}

function propagateDestroy(from: Duplex, to: Duplex): void {
  from.once('close', () => {
    if (!to.destroyed) {
      to.destroy()
    }
  })
}
