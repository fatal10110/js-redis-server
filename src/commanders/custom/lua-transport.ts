import type { Transport } from '../../types'

/**
 * Transport for capturing command responses during Lua script execution
 * instead of writing to socket
 */
export class LuaTransport implements Transport {
  private lastResponse: unknown = null

  write(responseData: unknown, close?: boolean): void {
    // Capture response for Lua script
    this.lastResponse = responseData

    // Ignore close flag - Lua scripts don't close connections
  }

  /**
   * Get the captured response
   */
  getResponse(): unknown {
    return this.lastResponse
  }

  /**
   * Reset for next command
   */
  reset(): void {
    this.lastResponse = null
  }
}
