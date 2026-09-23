export interface ConnectionTransport {
  readonly id: string
  readonly signal: AbortSignal
  read(): AsyncIterable<Buffer>
  write(chunk: Buffer): void | Promise<void>
  close(reason?: string): void
}
