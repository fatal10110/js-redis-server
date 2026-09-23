import { duplexPair, type Duplex } from 'node:stream'
import { SocketConnectionTransport } from '../src/core/transports/socket-connection-transport'
import type { ConnectionTransport } from '../src/core/transports/connection-transport'

/**
 * A {@link ConnectionTransport} over an in-process {@link duplexPair}, with the
 * client end exposed as test controls: {@link feed} writes bytes the session
 * should read, {@link endRead} half-closes that direction, and
 * {@link getWrittenBuffer} returns everything the session wrote back.
 *
 * The server side is the production {@link SocketConnectionTransport}, so these
 * tests exercise the same framing/teardown path as a real socket connection.
 */
export class InMemoryTransport implements ConnectionTransport {
  private readonly clientEnd: Duplex
  private readonly transport: SocketConnectionTransport
  private readonly written: Buffer[] = []
  private readerActive = false

  constructor(id?: string) {
    const [clientEnd, serverEnd] = duplexPair()
    this.clientEnd = clientEnd
    this.transport = new SocketConnectionTransport(serverEnd, { id })
    clientEnd.on('data', (chunk: Buffer) => {
      this.written.push(Buffer.from(chunk))
    })
  }

  get id(): string {
    return this.transport.id
  }

  get signal(): AbortSignal {
    return this.transport.signal
  }

  async *read(): AsyncIterable<Buffer> {
    if (this.readerActive) {
      throw new Error('ConnectionTransport only supports one reader')
    }
    this.readerActive = true

    try {
      yield* this.transport.read()
    } finally {
      this.readerActive = false
    }
  }

  write(chunk: Buffer): Promise<void> {
    return this.transport.write(chunk)
  }

  close(reason?: string): void {
    this.transport.close(reason)
  }

  /** Push bytes onto the wire as if the client had sent them. */
  feed(chunk: Buffer): void {
    if (this.clientEnd.writableEnded || this.signal.aborted) {
      throw new Error('Cannot feed a closed transport')
    }

    this.clientEnd.write(Buffer.from(chunk))
  }

  /** Signal end-of-input on the client→server direction. */
  endRead(): void {
    this.clientEnd.end()
  }

  /** Everything the session has written back to the client so far. */
  getWrittenBuffer(): Buffer {
    return Buffer.concat(this.written)
  }
}
