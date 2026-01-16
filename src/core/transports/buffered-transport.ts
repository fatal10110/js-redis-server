import { Transport } from '../../types'

export class BufferedTransport implements Transport {
  private buffer: unknown[] = []
  private closeRequested = false

  constructor(private readonly inner: Transport) {}

  write(responseData: unknown): void {
    this.buffer.push(responseData)
  }

  flush(options?: { close?: boolean }): void {
    const close = options?.close ?? this.closeRequested

    for (const responseData of this.buffer) {
      this.inner.write(responseData)
    }

    this.buffer = []
    this.closeRequested = false
    this.inner.flush({ close })
  }

  closeAfterFlush(): void {
    this.closeRequested = true
  }
}
