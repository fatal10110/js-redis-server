import {
  type ConnectionTransport,
  type ConnectionTransportEvent,
  type ConnectionTransportListener,
  type ConnectionTransportUnsubscribe,
} from './connection-transport'

export class InMemoryConnectionTransport implements ConnectionTransport {
  private static nextId = 0

  readonly id: string
  readonly signal: AbortSignal

  private readonly controller = new AbortController()
  private readonly incoming: Buffer[] = []
  private readonly written: Buffer[] = []
  private readonly listeners = new Map<
    ConnectionTransportEvent,
    Set<ConnectionTransportListener>
  >()
  private readonly waiters: Array<() => void> = []
  private inputEnded = false
  private readerActive = false
  private closed = false

  constructor(id?: string) {
    this.id = id ?? `memory-${++InMemoryConnectionTransport.nextId}`
    this.signal = this.controller.signal
  }

  feed(chunk: Buffer): void {
    if (this.inputEnded || this.closed) {
      throw new Error('Cannot feed a closed transport')
    }

    this.incoming.push(Buffer.from(chunk))
    this.wakeReaders()
  }

  endRead(): void {
    this.inputEnded = true
    this.wakeReaders()
  }

  async *read(): AsyncIterable<Buffer> {
    if (this.readerActive) {
      throw new Error('ConnectionTransport only supports one reader')
    }

    this.readerActive = true

    try {
      while (!this.closed) {
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

    this.written.push(Buffer.from(chunk))
  }

  getWritten(): Buffer[] {
    return this.written.map(chunk => Buffer.from(chunk))
  }

  getWrittenBuffer(): Buffer {
    return Buffer.concat(this.written)
  }

  clearWritten(): void {
    this.written.length = 0
  }

  close(_reason?: string): void {
    if (this.closed) {
      return
    }

    this.closed = true
    this.inputEnded = true
    this.controller.abort()
    this.wakeReaders()
    this.emit('close')
  }

  on(
    event: ConnectionTransportEvent,
    listener: ConnectionTransportListener,
  ): ConnectionTransportUnsubscribe {
    let eventListeners = this.listeners.get(event)
    if (!eventListeners) {
      eventListeners = new Set()
      this.listeners.set(event, eventListeners)
    }

    eventListeners.add(listener)
    return () => {
      eventListeners?.delete(listener)
    }
  }

  private waitForInput(): Promise<void> {
    return new Promise(resolve => {
      this.waiters.push(resolve)
    })
  }

  private wakeReaders(): void {
    const waiters = this.waiters.splice(0)
    for (const waiter of waiters) {
      waiter()
    }
  }

  private emit(event: ConnectionTransportEvent, error?: Error): void {
    const listeners = this.listeners.get(event)
    if (!listeners) {
      return
    }

    for (const listener of listeners) {
      listener(error)
    }
  }
}
