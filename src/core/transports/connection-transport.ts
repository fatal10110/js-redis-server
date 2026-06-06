export type ConnectionTransportEvent = 'close' | 'drain' | 'error'

export type ConnectionTransportListener = (error?: Error) => void

export type ConnectionTransportUnsubscribe = () => void

export interface ConnectionTransport {
  readonly id: string
  readonly signal: AbortSignal
  read(): AsyncIterable<Buffer>
  write(chunk: Buffer): void | Promise<void>
  close(reason?: string): void
  on(
    event: ConnectionTransportEvent,
    listener: ConnectionTransportListener,
  ): ConnectionTransportUnsubscribe
}
