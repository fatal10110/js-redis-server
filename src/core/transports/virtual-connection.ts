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
  /**
   * End the server-side session and half-close the wire. The client socket is
   * destroyed once it has read the remaining bytes and `'end'`.
   */
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
 * {@link attachSession} treats exactly like a real socket connection.
 *
 * Teardown mirrors TCP. A client `destroy()` destroys the server end, which
 * aborts the adapter and closes the session. A server-side close (
 * {@link VirtualConnection.close}, `QUIT`, a protocol error) half-closes: the
 * session ends at once, and the client receives any buffered reply bytes, then
 * `'end'`, then `'close'` — whenever it reads them. A client that never reads
 * stays half-open until its owner destroys it, as a real socket would.
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
  propagateDestroy(clientSocket, serverEnd, { awaitReaderEof: false })
  propagateDestroy(serverEnd, clientSocket, { awaitReaderEof: true })

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
  // getter-backed originals on `net.Socket`; configurable, so test doubles
  // (sinon-style `stub(socket, 'remoteAddress')`) can still redefine them.
  const address = (value: string | number) => ({
    value,
    enumerable: true,
    configurable: true,
  })
  Object.defineProperties(socket, {
    remoteAddress: address(remoteAddress),
    remotePort: address(remotePort),
    localAddress: address(DEFAULT_REMOTE_ADDRESS),
    localPort: address(0),
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

type PropagateDestroyOptions = {
  /**
   * After a clean half-close, let `to` drain to `'end'` before destroying it,
   * instead of destroying it straight away. Set for the server → client
   * direction only: the client may not be reading yet, and destroying a
   * paused stream strands whatever it has buffered (a destroyed stream never
   * starts emitting `'data'`).
   */
  awaitReaderEof: boolean
}

function propagateDestroy(
  from: Duplex,
  to: Duplex,
  { awaitReaderEof }: PropagateDestroyOptions,
): void {
  // Read the failure off `from.errored` at 'close' rather than holding an
  // 'error' listener: a permanent listener would silently swallow errors for a
  // consumer with no handler of its own, where a net.Socket (or a bare
  // duplexPair end) would surface them as an unhandled 'error'.
  from.once('close', () => {
    if (to.destroyed) {
      return
    }

    // Carrying a genuine failure across keeps an errored teardown from looking
    // like a graceful close on the far end — the distinction a real socket pair
    // makes. Teardown artifacts must NOT cross, though: ending a `for await`
    // early destroys the stream with an AbortError, and a bridged destroy
    // surfaces as a premature close. Forwarding either would turn a clean QUIT
    // into a connection error for the client.
    const failure = genuineFailure(from.errored)
    if (failure) {
      to.destroy(failure)
      return
    }

    if (awaitReaderEof && from.writableEnded) {
      // `from` half-closed, so its EOF is (or is about to be) queued on `to`
      // behind any reply bytes. Pushing it again is a no-op if it is already
      // there, and guarantees `to` can reach 'end' even if `from` was torn
      // down before its `_final` ran.
      to.push(null)

      // Like a real TCP socket, `to` stays half-open until its consumer reads
      // to EOF (or destroys it). The server side is already gone either way:
      // `from` is destroyed and the session has ended.
      if (to.readableEnded) {
        to.destroy()
      } else {
        to.once('end', () => to.destroy())
      }
      return
    }

    to.destroy()
  })
}

function genuineFailure(err: Error | null | undefined): Error | undefined {
  if (!err) {
    return undefined
  }

  const code = (err as NodeJS.ErrnoException).code
  return code && TEARDOWN_ERROR_CODES.has(code) ? undefined : err
}
