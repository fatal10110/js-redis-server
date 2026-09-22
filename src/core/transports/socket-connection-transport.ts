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
    try {
      for await (const chunk of this.socket) {
        if (this.signal.aborted) {
          return
        }

        yield Buffer.from(chunk as Buffer)
      }
    } catch (err) {
      // Destroying a stream mid-iteration rejects its async iterator with
      // ERR_STREAM_PREMATURE_CLOSE. That is an ordinary disconnect — either end
      // tearing the wire down — so the read loop just ends. Every other error
      // (including a `destroy(err)` carrying a real failure) still propagates,
      // so the adapter can log it.
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
      let settled = false

      const settle = (err?: Error | null) => {
        if (settled) {
          return
        }
        settled = true
        this.signal.removeEventListener('abort', onTeardown)
        this.socket.off('close', onTeardown)

        // A write that never made it out because the wire went away is not a
        // failure worth propagating — the session is already ending.
        if (err && !this.signal.aborted && !this.socket.destroyed) {
          reject(err)
          return
        }

        resolve()
      }

      // `net.Socket` fails a pending write callback with ECANCELED when it is
      // destroyed, but a `duplexPair` end leaves that callback pending forever.
      // Without this the adapter's write chain would stall and the session
      // would never be torn down, so settle on teardown too.
      const onTeardown = () => settle()

      this.signal.addEventListener('abort', onTeardown, { once: true })
      this.socket.once('close', onTeardown)
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
   * The destroy cannot be synchronous or a microtask: `end()` only runs
   * `_final` — which is what hands the peer its EOF — on a following tick, so
   * destroying sooner drops the `'end'` entirely. Nor can it wait on `'finish'`
   * alone: a `duplexPair` withholds `'finish'` until the peer has drained, so a
   * client that never reads would leave the stream alive and the read loop
   * suspended forever. Destroy on `'finish'`, with an immediate as the bound.
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

  private abort(): void {
    if (!this.controller.signal.aborted) {
      this.controller.abort()
    }
  }
}

function isPrematureClose(err: unknown): boolean {
  return (
    err instanceof Error &&
    (err as NodeJS.ErrnoException).code === 'ERR_STREAM_PREMATURE_CLOSE'
  )
}
