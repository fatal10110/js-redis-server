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
    // `destroyOnReturn: false`: when the SERVER ends first (QUIT, a protocol
    // error, the owner's close()) the consumer leaves this loop early, and that
    // must not destroy the stream. The default destroys it with an AbortError,
    // and from Node 24 a duplexPair answers an errored destroy by destroying
    // its peer — the client — which strands a reply it has not read yet.
    // close() half-closes instead. `readable.iterator()` and `destroyOnReturn`
    // are documented as experimental.
    const chunks = this.socket.iterator({ destroyOnReturn: false })

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

    // Only reached when the CLIENT ended first: its EOF arrived, or its end
    // was destroyed. (An early exit by the consumer returns above instead.)
    this.dropOnPeerEof()
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
   * The client ended its side first. Tear the connection down promptly, as
   * Redis's freeClient does (and main did over TCP): output the client has not
   * read is dropped. Waiting on it instead — which close()'s half-close does —
   * would park the session, and leak the stream, behind a client that stopped
   * reading. `abort()` settles the in-flight writes, so the adapter's pending
   * drains finish; `end()` still hands EOF to a client that is reading.
   *
   * Deferred by one immediate: the adapter's finally runs first and flushes
   * output that is already queued and can go out (e.g. the confirmations for a
   * `SUBSCRIBE a b c` sent in the same tick as the EOF — on the in-process
   * wire those writes complete within the current turn). What the immediate
   * bounds is writes that cannot complete, to a peer that stopped reading.
   *
   * This only runs once the read loop has seen the EOF. A bounded stream whose
   * loop is blocked writing an ordinary request/response reply (not a
   * background drain) to a client that stopped reading never gets that far, so
   * that session stays parked until the stream closes. The shipped paths do
   * not hit this: the virtual wire cannot back up, and a TCP peer that goes
   * away errors or closes the socket, which aborts the transport.
   */
  private dropOnPeerEof(): void {
    setImmediate(() => {
      // Guard on the stream, not `closed`: a server-side close() that came
      // first (e.g. CLIENT KILL) only half-closes, and waits for output a paused
      // client will never read. The client's EOF must still tear it down.
      if (this.socket.destroyed) {
        return
      }

      this.closed = true
      this.abort()

      if (this.socket.writable) {
        this.socket.end()
      }
      this.socket.destroy()
    })
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
