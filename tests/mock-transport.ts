import { Transport } from '../src/types'

export interface MockTransportCall {
  responseData: unknown
  close?: boolean
  timestamp: number
}

export class MockTransport implements Transport {
  private calls: MockTransportCall[] = []
  private closed = false

  write(responseData: unknown, close?: boolean): void {
    this.calls.push({
      responseData,
      close,
      timestamp: Date.now(),
    })

    if (close) {
      this.closed = true
    }
  }

  // Test utility methods
  getCalls(): MockTransportCall[] {
    return [...this.calls]
  }

  getLastCall(): MockTransportCall | undefined {
    return this.calls[this.calls.length - 1]
  }

  getCallCount(): number {
    return this.calls.length
  }

  getResponseData(): unknown[] {
    return this.calls.map(call => call.responseData)
  }

  getLastResponse(): unknown {
    const lastCall = this.getLastCall()
    return lastCall?.responseData
  }

  wasCloseCalled(): boolean {
    return this.calls.some(call => call.close === true)
  }

  isClosed(): boolean {
    return this.closed
  }

  reset(): void {
    this.calls = []
    this.closed = false
  }

  // Assertion helpers
  assertCallCount(expected: number): void {
    if (this.calls.length !== expected) {
      throw new Error(
        `Expected ${expected} calls, but got ${this.calls.length}`,
      )
    }
  }

  assertLastResponse(expected: unknown): void {
    const lastResponse = this.getLastResponse()
    if (lastResponse !== expected) {
      throw new Error(
        `Expected last response to be ${expected}, but got ${lastResponse}`,
      )
    }
  }

  assertCloseCalled(): void {
    if (!this.wasCloseCalled()) {
      throw new Error('Expected close to be called, but it was not')
    }
  }

  assertCloseNotCalled(): void {
    if (this.wasCloseCalled()) {
      throw new Error('Expected close not to be called, but it was')
    }
  }
}

// Factory function for creating mock transports
export function createMockTransport(): MockTransport {
  return new MockTransport()
}
