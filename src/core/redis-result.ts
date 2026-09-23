import { errorReplyBody, type RedisCommandError } from './redis-error'
import { RedisValue } from './redis-value'

export type RedisResultOptions = {
  close?: boolean
  disconnect?: boolean
  omitReply?: boolean
  afterReply?: () => void
  /**
   * Frames `encoded` already carries after `value` — the 2nd..Nth confirmation
   * of a multi-target SUBSCRIBE. The wire path writes them with the reply; a
   * front end that reads `value` instead of bytes delivers them as pushes.
   */
  trailingFrames?: readonly RedisValue[]
}

export class RedisResult {
  constructor(
    public readonly value: RedisValue,
    public readonly options?: RedisResultOptions,
    public readonly encoded?: Buffer,
  ) {}

  static create(value: RedisValue, options?: RedisResultOptions): RedisResult {
    return new RedisResult(value, options)
  }

  static preEncoded(
    value: RedisValue,
    encoded: Buffer,
    options?: RedisResultOptions,
  ): RedisResult {
    return new RedisResult(value, options, Buffer.from(encoded))
  }

  static nil(): RedisResult {
    return new RedisResult(RedisValue.null())
  }

  static ok(): RedisResult {
    return new RedisResult(RedisValue.simpleString('OK'))
  }

  static error(message: string | Buffer, code?: string): RedisResult {
    return new RedisResult(RedisValue.error(message, code))
  }

  /**
   * The reply for a caught {@link RedisCommandError}. Prefer this over
   * `RedisResult.error(err.message, err.code)`: `Error.message` is a `string`,
   * so that form silently drops the byte-exact body of an error that echoes
   * raw client bytes.
   */
  static fromError(error: RedisCommandError): RedisResult {
    return new RedisResult(RedisValue.error(errorReplyBody(error), error.code))
  }
}
