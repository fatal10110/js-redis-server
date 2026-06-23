import { Duplex } from 'node:stream'
import type { CommandExecutor } from '../command-executor'
import type { RespEncodeOptions } from '../resp-encoder'
import type { Logger } from '../../logger'
import type { RedisClusterNodeRole, RedisServerState } from '../../state'
import { formatSocketAddressParts } from '../network-address'
import { attachSession } from './attach-session'
import {
  type ConnectionTransport,
  type ConnectionTransportEvent,
  type ConnectionTransportListener,
  type ConnectionTransportUnsubscribe,
} from './connection-transport'

export type CreateVirtualConnectionOptions = {
  state: RedisServerState
  executor: CommandExecutor
  nodeRole?: RedisClusterNodeRole
  logger?: Pick<Logger, 'error'>
  encoder?: RespEncodeOptions
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
 * A `net.Socket`-compatible {@link Duplex} handed to a client library (ioredis)
 * as its connection stream. Only the surface the client touches is implemented:
 * duplex read/write, the no-op socket tuning methods, the `remoteAddress` /
 * `remotePort` getters, and the `'connect'` event (emitted on `nextTick`, like
 * ioredis' `StandaloneConnector`). Bytes written by the client are forwarded to
 * the server side via {@link feedServer}; bytes from the server are pushed back
 * out as `'data'`.
 */
export class VirtualClientSocket extends Duplex {
  readonly remoteAddress: string
  readonly remotePort: number
  readonly localAddress = '127.0.0.1'
  readonly localPort = 0

  /** Called with each chunk the client writes; set by the wiring below. */
  private onClientWrite?: (chunk: Buffer) => void
  /** Called once when the client side initiates teardown. */
  private onClientDestroy?: () => void

  constructor(remoteAddress: string, remotePort: number) {
    super()
    this.remoteAddress = remoteAddress
    this.remotePort = remotePort
    // ioredis resolves the connector's promise and then waits for 'connect';
    // StandaloneConnector resolves on process.nextTick, so match that timing.
    process.nextTick(() => {
      if (!this.destroyed) {
        this.emit('connect')
        this.emit('ready')
      }
    })
  }

  /** @internal Wire the server-bound write + destroy hooks. */
  bindServer(hooks: {
    onClientWrite: (chunk: Buffer) => void
    onClientDestroy: () => void
  }): void {
    this.onClientWrite = hooks.onClientWrite
    this.onClientDestroy = hooks.onClientDestroy
  }

  /** @internal Push a chunk produced by the server out to the client reader. */
  feedClient(chunk: Buffer): void {
    if (!this.destroyed) {
      this.push(chunk)
    }
  }

  /** @internal Signal the client reader that the server closed the wire. */
  endClient(): void {
    if (!this.destroyed) {
      this.push(null)
    }
  }

  override _read(): void {
    // Backpressure is not modeled — the server pushes proactively via feedClient.
  }

  override _write(
    chunk: Buffer | string,
    encoding: BufferEncoding,
    callback: (error?: Error | null) => void,
  ): void {
    // Defensive copy of caller-owned Buffer input; the string branch already
    // allocates a fresh Buffer, so it needs no second copy.
    const owned = Buffer.isBuffer(chunk)
      ? Buffer.from(chunk)
      : Buffer.from(chunk, encoding)
    this.onClientWrite?.(owned)
    callback()
  }

  override _destroy(
    error: Error | null,
    callback: (error: Error | null) => void,
  ): void {
    this.onClientDestroy?.()
    callback(error)
  }

  // --- net.Socket-shaped no-ops the client library calls during setup ---

  setNoDelay(): this {
    return this
  }

  setKeepAlive(): this {
    return this
  }

  setTimeout(): this {
    return this
  }

  ref(): this {
    return this
  }

  unref(): this {
    return this
  }
}

/**
 * A server-side {@link ConnectionTransport} over the in-process virtual wire.
 * `read()` yields the bytes the client wrote; `write()` pushes bytes back to the
 * client socket; `close()` aborts the read loop and tears both ends down.
 */
class DuplexConnectionTransport implements ConnectionTransport {
  private static nextId = 0

  readonly id: string
  readonly signal: AbortSignal

  private readonly controller = new AbortController()
  private readonly incoming: Buffer[] = []
  private readonly waiters = new Set<() => void>()
  private readonly listeners = new Map<
    ConnectionTransportEvent,
    Set<ConnectionTransportListener>
  >()
  private readerActive = false
  private inputEnded = false
  private closed = false

  constructor(
    private readonly clientSocket: VirtualClientSocket,
    id?: string,
  ) {
    this.id = id ?? `virtual-${++DuplexConnectionTransport.nextId}`
    this.signal = this.controller.signal
  }

  /** Feed a chunk the client wrote into the server-side read loop. */
  feed(chunk: Buffer): void {
    // Deliberate divergence from InMemoryConnectionTransport.feed (a strict test
    // harness that throws on a closed transport): a real socket silently drops
    // writes after close, and this transport models a socket — so drop, not throw.
    if (this.inputEnded || this.closed) {
      return
    }
    this.incoming.push(Buffer.from(chunk))
    this.wakeReaders()
  }

  /** Mark the client→server direction finished (client ended its writable). */
  endInput(): void {
    this.inputEnded = true
    this.wakeReaders()
  }

  async *read(): AsyncIterable<Buffer> {
    if (this.readerActive) {
      throw new Error('ConnectionTransport only supports one reader')
    }
    this.readerActive = true

    try {
      while (!this.closed && !this.signal.aborted) {
        const chunk = this.incoming.shift()
        if (chunk) {
          yield Buffer.from(chunk)
          continue
        }
        if (this.inputEnded) {
          return
        }
        await this.waitForInput()
      }
    } finally {
      this.readerActive = false
    }
  }

  write(chunk: Buffer): void {
    if (this.closed) {
      return
    }
    this.clientSocket.feedClient(Buffer.from(chunk))
  }

  close(_reason?: string): void {
    if (this.closed) {
      return
    }
    this.closed = true
    this.inputEnded = true
    this.abort()
    this.wakeReaders()
    this.clientSocket.endClient()
    if (!this.clientSocket.destroyed) {
      this.clientSocket.destroy()
    }
    this.emit('close')
  }

  on(
    event: ConnectionTransportEvent,
    listener: ConnectionTransportListener,
  ): ConnectionTransportUnsubscribe {
    let bucket = this.listeners.get(event)
    if (!bucket) {
      bucket = new Set()
      this.listeners.set(event, bucket)
    }
    bucket.add(listener)
    return () => {
      bucket?.delete(listener)
    }
  }

  private waitForInput(): Promise<void> {
    return new Promise(resolve => {
      this.waiters.add(resolve)
    })
  }

  private wakeReaders(): void {
    const waiters = Array.from(this.waiters)
    this.waiters.clear()
    for (const waiter of waiters) {
      waiter()
    }
  }

  private abort(): void {
    if (!this.controller.signal.aborted) {
      this.controller.abort()
    }
  }

  private emit(event: ConnectionTransportEvent, error?: Error): void {
    const bucket = this.listeners.get(event)
    if (!bucket) {
      return
    }
    for (const listener of bucket) {
      listener(error)
    }
  }
}

/**
 * Build an in-process virtual connection: a fake client-facing `net.Socket`
 * already wired to a fresh server-side {@link ClientSession}. The reusable
 * primitive behind {@link createIoredisMock} — no TCP socket, no port bind.
 *
 * Bytes the client writes flow into a {@link DuplexConnectionTransport} that
 * {@link attachSession} drives exactly like a real socket connection; the
 * server's replies are pushed back out as `'data'` on the client socket. Tearing
 * down either end (client `destroy()` or {@link VirtualConnection.close}) closes
 * the transport, which aborts the adapter and the session.
 */
export function createVirtualConnection(
  opts: CreateVirtualConnectionOptions,
): VirtualConnection {
  const clientSocket = new VirtualClientSocket(
    opts.remoteAddress ?? '127.0.0.1',
    opts.remotePort ?? 6379,
  )
  const transport = new DuplexConnectionTransport(clientSocket)

  clientSocket.bindServer({
    onClientWrite: chunk => transport.feed(chunk),
    onClientDestroy: () => transport.endInput(),
  })

  const attached = attachSession(transport, {
    state: opts.state,
    executor: opts.executor,
    nodeRole: opts.nodeRole,
    logger: opts.logger,
    encoder: opts.encoder,
    clientAddress: formatSocketAddressParts(
      opts.remoteAddress ?? '127.0.0.1',
      opts.remotePort ?? 6379,
    ),
  })

  return {
    clientSocket,
    done: attached.done,
    close: () => attached.close(),
  }
}
