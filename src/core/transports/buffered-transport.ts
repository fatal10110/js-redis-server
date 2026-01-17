import { CommandResult, Transport } from '../../types'

export class BufferedTransport implements Transport {
  private result: CommandResult | null = null
  private hasWritten = false
  private closeRequested = false

  constructor(private readonly inner: Transport) {}

  write(responseData: CommandResult): void {
    if (this.hasWritten) {
      throw new Error('BufferedTransport only allows a single write')
    }
    if (responseData instanceof Error) {
      throw responseData
    }
    this.hasWritten = true
    this.result = responseData as CommandResult
  }

  flush(options?: { close?: boolean }): void {
    const close = options?.close ?? this.closeRequested
    if (this.hasWritten) {
      this.inner.write(this.result)
    }
    this.result = null
    this.hasWritten = false
    this.closeRequested = false
    this.inner.flush({ close })
  }

  closeAfterFlush(): void {
    this.closeRequested = true
  }
}
