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

/**
 * The virtual wire is memory-to-memory: there is no kernel socket buffer, and
 * an in-process server must never block because the client has not read yet.
 * A `duplexPair` end parks its write callback until the peer's `_read()` runs,
 * so the default 16 KiB high-water mark would stall the adapter's write chain
 * mid-reply against a paused client. Raising it past any reply size restores
 * the unbounded queueing `createIoredisMock` has always had: `write()` always
 * returns `true`, so there is no `'drain'` to wait on.
 */
const UNBOUNDED_HIGH_WATER_MARK = Number.MAX_SAFE_INTEGER

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

  const [clientEnd, serverEnd] = duplexPair({
    highWaterMark: UNBOUNDED_HIGH_WATER_MARK,
  })
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

  // Non-writable, to match both the `readonly` on the type above and the
  // getter-backed originals on `net.Socket`.
  Object.defineProperties(socket, {
    remoteAddress: { value: remoteAddress, enumerable: true },
    remotePort: { value: remotePort, enumerable: true },
    localAddress: { value: DEFAULT_REMOTE_ADDRESS, enumerable: true },
    localPort: { value: 0, enumerable: true },
  })

  Object.assign(socket, {
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

/** Codes Node raises for ordinary stream teardown rather than a wire failure. */
const TEARDOWN_ERROR_CODES = new Set([
  'ABORT_ERR',
  'ERR_STREAM_DESTROYED',
  'ERR_STREAM_PREMATURE_CLOSE',
])

function propagateDestroy(from: Duplex, to: Duplex): void {
  let failure: Error | undefined

  // 'error' always precedes 'close', so by the time the bridge fires we know
  // whether this was a clean teardown or a failure. Carrying a genuine failure
  // across keeps an errored teardown from looking like a graceful close on the
  // far end — the distinction a real socket pair makes.
  //
  // Teardown artifacts must NOT cross, though: ending a `for await` early
  // destroys the stream with an AbortError, and a bridged destroy surfaces as a
  // premature close. Forwarding either would turn a clean QUIT into a
  // connection error for the client.
  from.on('error', (err: Error) => {
    const code = (err as NodeJS.ErrnoException).code
    if (!code || !TEARDOWN_ERROR_CODES.has(code)) {
      failure = err
    }
  })

  from.once('close', () => {
    if (!to.destroyed) {
      to.destroy(failure)
    }
  })
}
