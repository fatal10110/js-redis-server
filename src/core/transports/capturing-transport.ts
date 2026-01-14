import { Transport } from '../../types'

/**
 * CapturingTransport captures responses instead of writing them to a socket.
 * Used during EXEC to collect all command results for the final array response.
 */
export class CapturingTransport implements Transport {
  private results: unknown[] = []
  private closed = false

  write(responseData: unknown, close?: boolean): void {
    this.results.push(responseData)
    if (close) {
      this.closed = true
    }
  }

  getResults(): unknown[] {
    return this.results
  }

  isClosed(): boolean {
    return this.closed
  }
}
