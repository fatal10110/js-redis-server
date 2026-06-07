import { Socket } from 'net'
import {
  type ConnectionTransport,
  type ConnectionTransportEvent,
  type ConnectionTransportListener,
  type ConnectionTransportUnsubscribe,
} from './connection-transport'

export type SocketConnectionTransportOptions = {
  id?: string
}

export class SocketConnectionTransport implements ConnectionTransport {
  private static nextId = 0

  readonly id: string
  readonly signal: AbortSignal

  private readonly controller = new AbortController()
  private closed = false

  constructor(
    private readonly socket: Socket,
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
    for await (const chunk of this.socket) {
      if (this.signal.aborted) {
        return
      }

      yield Buffer.isBuffer(chunk) ? Buffer.from(chunk) : Buffer.from(chunk)
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
    this.socket.destroySoon()
  }

  on(
    event: ConnectionTransportEvent,
    listener: ConnectionTransportListener,
  ): ConnectionTransportUnsubscribe {
    const wrapped = (error?: Error) => listener(error)
    this.socket.on(event, wrapped)
    return () => {
      this.socket.off(event, wrapped)
    }
  }

  private abort(): void {
    if (!this.controller.signal.aborted) {
      this.controller.abort()
    }
  }
}
