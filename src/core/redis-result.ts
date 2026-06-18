import { RedisValue } from './redis-value'

export type RedisResultOptions = {
  close?: boolean
  disconnect?: boolean
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

  static error(message: string, code?: string): RedisResult {
    return new RedisResult(RedisValue.error(message, code))
  }
}
