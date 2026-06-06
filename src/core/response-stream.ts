import { RedisResult } from './redis-result'

/**
 * Single-reader stream of server-initiated Redis responses.
 *
 * Phase 1 intentionally keeps backpressure out of this interface. A transport
 * owns the single reader and may pause iteration if its underlying connection
 * applies backpressure. Implementations should treat a second `frames()` reader
 * as unsupported unless they document stronger behavior.
 */
export interface ResponseStream {
  readonly kind: 'response-stream'
  readonly closed: Promise<void>
  frames(signal: AbortSignal): AsyncIterable<RedisResult>
  close(reason?: string): void
}

export function isResponseStream(value: unknown): value is ResponseStream {
  return (
    typeof value === 'object' &&
    value !== null &&
    'kind' in value &&
    value.kind === 'response-stream'
  )
}
