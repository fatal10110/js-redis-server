import type { Duplex } from 'node:stream'
import { type ConnectionTransport } from './connection-transport'

export type SocketConnectionTransportOptions = {
  id?: string
}

/** `net.Socket.destroySoon()` — absent on a plain {@link Duplex}. */
type MaybeSocket = Duplex & { destroySoon?: () => void }

/**
 * A {@link ConnectionTransport} over any {@link Duplex} stream: a real
 * `net.Socket` for the TCP server, or one end of a `stream.duplexPair()` for
 * the in-process virtual wire.
 */
export class SocketConnectionTransport implements ConnectionTransport {
  private static nextId = 0

  readonly id: string
  readonly signal: AbortSignal

  private readonly controller = new AbortController()
  /** Settle callbacks for writes whose stream callback has not fired yet. */
  private readonly pendingWrites = new Set<(err?: Error | null) => void>()
  private closed = false

  constructor(
    private readonly socket: Duplex,
    options?: SocketConnectionTransportOptions,
  ) {
    this.id = options?.id ?? `socket-${++SocketConnectionTransport.nextId}`
    this.signal = this.controller.signal

    socket
      .on('close', () => this.abort())
      .on('error', () => this.abort())
      .on('timeout', () => this.abort())
  }

  async *read(): AsyncIterable<Buffer> {
    // A real socket keeps Node's default and is destroyed when this loop exits,
    // as on main. On peer EOF that is Redis's freeClient — output the peer
    // never read is dropped — and it must happen here, not in the adapter's
    // finally: that awaits pending writes first, which never flush to a peer
    // that has stopped reading, so the socket would stay open for good.
    //
    // The in-process wire (a duplexPair, no destroySoon) opts out. The default
    // destroys with an AbortError, and from Node 24 a duplexPair answers an
    // errored destroy by destroying its peer — the client — which would strand
    // a reply it has not read yet (e.g. after QUIT). Its writes cannot back up
    // (unbounded high-water mark), so the adapter's close() is enough there.
    //
    // `readable.iterator()` and its `destroyOnReturn` option are documented as
    // experimental.
    const chunks = this.socket.iterator({
      destroyOnReturn:
        typeof (this.socket as MaybeSocket).destroySoon === 'function',
    })

    try {
      for await (const chunk of chunks) {
        if (this.signal.aborted) {
          return
        }

        yield Buffer.from(chunk as Buffer)
      }
    } catch (err) {
      // Destroying a stream mid-iteration rejects its async iterator with
      // ERR_STREAM_PREMATURE_CLOSE. That is an ordinary disconnect — either end
      // tearing the wire down — so the read loop just ends.
      //
      // Any other error is rethrown rather than passed off as a clean EOF. Note
      // it is not logged end to end: the stream's 'error' has already aborted
      // the transport, and the adapter only logs while it is not aborted. That
      // matches main, where e.g. a TCP ECONNRESET ends the session silently.
      if (!isPrematureClose(err)) {
        throw err
      }
    }
  }

  write(chunk: Buffer): Promise<void> {
    if (this.closed || this.socket.destroyed) {
      return Promise.resolve()
    }

    return new Promise((resolve, reject) => {
      const settle = (err?: Error | null) => {
        if (!this.pendingWrites.delete(settle)) {
          return
        }

        // A write that never made it out because the wire went away is not a
        // failure worth propagating — the session is already ending.
        if (err && !this.signal.aborted && !this.socket.destroyed) {
          reject(err)
          return
        }

        resolve()
      }

      this.pendingWrites.add(settle)
      this.socket.write(chunk, settle)
    })
  }

  close(_reason?: string): void {
    if (this.closed) {
      return
    }

    this.closed = true
    this.abort()
    this.destroySoon()
  }

  /**
   * Half-close, then destroy, so the peer sees `'end'` before `'close'` — the
   * sequence real Redis produces on `QUIT`. `net.Socket` has this built in as
   * `destroySoon()`; a plain {@link Duplex} (the virtual wire) does not.
   *
   * When a write is still in flight at `close()`, `end()` defers `_final` —
   * which is what hands the peer its EOF — until that write completes on a
   * later tick, so a synchronous or microtask destroy drops the `'end'`. Nor can
   * the destroy wait on `'finish'` alone: a `duplexPair` withholds `'finish'`
   * until the peer has drained, so a client that never reads would leave the
   * stream alive and the read loop suspended forever. Destroy on `'finish'`,
   * with an immediate as the bound.
   */
  private destroySoon(): void {
    const socket = this.socket as MaybeSocket

    if (typeof socket.destroySoon === 'function') {
      socket.destroySoon()
      return
    }

    if (socket.writable) {
      socket.end()
    }

    if (socket.writableFinished) {
      socket.destroy()
      return
    }

    socket.once('finish', () => socket.destroy())
    setImmediate(() => socket.destroy())
  }

  /**
   * Abort the transport and settle every in-flight write. `net.Socket` fails a
   * pending write callback with ECANCELED when it is destroyed, but a
   * `duplexPair` end leaves that callback pending forever; without this the
   * adapter's write chain would stall and the session would never tear down.
   * One listener per transport (installed in the constructor) rather than one
   * per write, so a burst of in-flight writes cannot trip MaxListeners.
   */
  private abort(): void {
    if (!this.controller.signal.aborted) {
      this.controller.abort()
    }

    for (const settle of [...this.pendingWrites]) {
      settle()
    }
  }
}

function isPrematureClose(err: unknown): boolean {
  return (
    err instanceof Error &&
    (err as NodeJS.ErrnoException).code === 'ERR_STREAM_PREMATURE_CLOSE'
  )
}
