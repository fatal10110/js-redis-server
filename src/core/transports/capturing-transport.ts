import { CommandResult, Transport } from '../../types'

/**
 * CapturingTransport captures responses instead of writing them to a socket.
 * Used during EXEC to collect all command results for the final array response.
 */
export class CapturingTransport implements Transport {
  private result: CommandResult | null = null
  private hasWritten = false
  private closed = false
  private closeRequested = false

  write(responseData: CommandResult): void {
    if (this.hasWritten) {
      throw new Error('CapturingTransport only allows a single write')
    }
    if (responseData instanceof Error) {
      throw responseData
    }
    this.hasWritten = true
    this.result = responseData
  }

  flush(options?: { close?: boolean }): void {
    const close = options?.close ?? this.closeRequested
    if (close) {
      this.closed = true
    }
    this.closeRequested = false
  }

  closeAfterFlush(): void {
    this.closeRequested = true
  }

  getResults(): CommandResult {
    return this.result
  }

  isClosed(): boolean {
    return this.closed
  }
}
