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
 * Teardown mirrors TCP, identically on Node 22 and 24. A client `destroy()`
 * destroys the server end, which aborts the adapter and closes the session; a
 * client `end()` makes the server close its side too. A server-side close (
 * {@link VirtualConnection.close}, `QUIT`, a protocol error) half-closes: the
 * session ends at once, and the client receives any buffered reply bytes, then
 * `'end'`, `'finish'` and `'close'` — whenever it reads them. A client that
 * never reads stays half-open until its owner destroys it, as a real socket
 * would; meanwhile its writes are accepted (and go nowhere) and `end()`
 * completes.
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

  // net.connect()'s default, which ioredis uses: once the server's EOF has
  // been read, the client ends its own writable ('finish'), then closes.
  clientSocket.allowHalfOpen = false

  settleWritesOnPeerClose(clientSocket, serverEnd)
  bridgeTeardown(clientSocket, serverEnd)

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

/**
 * Tear the two ends down together, the way a socket pair does. A `duplexPair`
 * does not do this itself on Node 22, and on Node 24 does it only partly (a
 * clean destroy just pushes EOF to the peer).
 *
 * No error crosses in either direction: the far end is torn down cleanly.
 * That is also what Node 24's own `duplexPair` does — it destroys the peer
 * without the error, so a consumer with no handler cannot crash on an
 * unhandled 'error' it did not cause — and on this wire nothing could observe
 * the difference anyway: the server end is never destroyed with an error, and
 * the transport does not surface a client-side one. No 'error' listener is
 * attached either, so a consumer's own errors surface as they would on a
 * net.Socket.
 */
function bridgeTeardown(clientEnd: Duplex, serverEnd: Duplex): void {
  // The client is gone — destroyed, or it read to EOF and closed itself — so
  // the server end has nobody to talk to. Dropping it ends the session.
  clientEnd.once('close', () => {
    if (!serverEnd.destroyed) {
      serverEnd.destroy()
    }
  })

  serverEnd.once('close', () => {
    if (clientEnd.destroyed) {
      return
    }

    if (!serverEnd.writableEnded) {
      clientEnd.destroy()
      return
    }

    // A clean server-side half-close (close(), QUIT, a protocol error). Leave
    // the client half-open, like TCP: it keeps any reply it has not read, then
    // EOF — pushing it again is a no-op if `_final` already queued it — and
    // closes itself once it reads that far (allowHalfOpen: false +
    // autoDestroy). A client that never reads stays half-open until its owner
    // destroys it; the session is already gone either way. Destroying it here
    // instead would strand the unread reply: a destroyed stream never starts
    // emitting 'data'.
    clientEnd.push(null)
  })
}

type WriteCallback = (error?: Error | null) => void

/**
 * A `duplexPair` end's `_write` completes only when its peer reads, and its
 * `_final` only when the peer emits 'end'. Once the server end is destroyed
 * neither can happen — and `Duplex.destroy()` does not flush a callback already
 * handed to `_write` — so client code awaiting a write or `end(cb)` would hang
 * for good. Answer them the way a TCP socket whose peer has closed does:
 * `end()` completes, and a write is accepted (the kernel buffers it; the data
 * goes nowhere) for as long as the client still has the unread reply and EOF
 * to take in. Failing it instead would destroy the client and discard that
 * reply, and crash a consumer with no 'error' handler. Only once the client
 * has read to EOF does a write fail with EPIPE — and by then a client with
 * allowHalfOpen: false has usually ended its own writable anyway.
 */
function settleWritesOnPeerClose(stream: Duplex, peer: Duplex): void {
  const writeToClosedPeer = () => (stream.readableEnded ? epipe() : undefined)

  const pending = new Map<WriteCallback, () => Error | undefined>()

  const track = (
    callback: WriteCallback,
    onPeerClose: () => Error | undefined,
  ): WriteCallback => {
    const settle: WriteCallback = error => {
      if (pending.delete(settle)) {
        callback(error)
      }
    }
    pending.set(settle, onPeerClose)
    return settle
  }

  const write = stream._write.bind(stream)
  const final = stream._final?.bind(stream)

  stream._write = (chunk, encoding, callback) => {
    if (peer.destroyed) {
      callback(writeToClosedPeer())
      return
    }
    write(chunk, encoding, track(callback, writeToClosedPeer))
  }

  if (final) {
    stream._final = callback => {
      if (peer.destroyed) {
        callback()
        return
      }
      final(track(callback, () => undefined))
    }
  }

  peer.once('close', () => {
    for (const [settle, onPeerClose] of [...pending]) {
      settle(onPeerClose())
    }
  })
}

/** Shaped like the error a net.Socket reports writing to a closed peer. */
function epipe(): NodeJS.ErrnoException {
  return Object.assign(new Error('write EPIPE'), {
    code: 'EPIPE',
    errno: -32,
    syscall: 'write',
  })
}
