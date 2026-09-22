import type { Duplex } from 'node:stream'
import { type ConnectionTransport } from './connection-transport'

export type SocketConnectionTransportOptions = {
  id?: string
}

/** `net.Socket.destroySoon()` — absent on a plain {@link Duplex}. */
type MaybeSocket = Duplex & { destroySoon?: () => void }

/**
 * A {@link ConnectionTransport} over any {@link Duplex} stream: a real
 * `net.Socket` for the TCP server, or one end of a {@link import('node:stream').duplexPair}
 * for the in-process virtual wire.
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
      // A destroyed stream surfaces on its async iterator as an error
      // (`ERR_STREAM_PREMATURE_CLOSE`). That is an ordinary disconnect — either
      // end tearing the wire down — not something the session should answer.
      if (!this.socket.destroyed && !this.signal.aborted) {
        throw err
      }
    }
  }

  write(chunk: Buffer): Promise<void> {
    if (this.closed || this.socket.destroyed) {
      return Promise.resolve()
    }

    return new Promise((resolve, reject) => {
      this.socket.write(chunk, err => {
        if (err) {
          reject(err)
          return
        }

        resolve()
      })
    })
  }

  close(_reason?: string): void {
    if (this.closed) {
      return
    }

    this.closed = true
    this.abort()

    const socket = this.socket as MaybeSocket
    if (typeof socket.destroySoon === 'function') {
      socket.destroySoon()
      return
    }

    socket.destroy()
  }

  private abort(): void {
    if (!this.controller.signal.aborted) {
      this.controller.abort()
    }
  }
}
