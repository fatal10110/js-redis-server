import { Transport } from '../../types'

/**
 * CapturingTransport captures responses instead of writing them to a socket.
 * Used during EXEC to collect all command results for the final array response.
 */
export class CapturingTransport implements Transport {
  private results: unknown[] = []
  private closed = false
  private closeRequested = false

  write(responseData: unknown): void {
    this.results.push(responseData)
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

  getResults(): unknown[] {
    return this.results
  }

  isClosed(): boolean {
    return this.closed
  }
}
