import { CommandResult } from '../../../../../types'

export type ReplyValue =
  | null
  | number
  | bigint
  | Buffer
  | { ok: Buffer }
  | { err: Buffer }
  | ReplyValue[]

export function replyValueToResponse(
  value: ReplyValue,
  sha: string,
  makeError: (message: string) => Error,
): CommandResult {
  if (value === null || value === undefined) {
    return null
  }

  if (typeof value === 'number' || typeof value === 'bigint') {
    return value
  }

  if (Buffer.isBuffer(value)) {
    return value
  }

  if (Array.isArray(value)) {
    return value.map(item => replyValueToResponse(item, sha, makeError))
  }

  if (typeof value === 'object' && value) {
    if ('ok' in value) {
      const okValue = value.ok
      return Buffer.isBuffer(okValue) ? okValue.toString() : String(okValue)
    }
    if ('err' in value) {
      const errValue = value.err
      const rawMessage = Buffer.isBuffer(errValue)
        ? errValue.toString()
        : String(errValue)
      const err = normalizeRedisError(makeError(rawMessage))
      return err
    }
  }

  return Buffer.from(String(value))
}

function normalizeRedisError(err: unknown): Error {
  const base = err instanceof Error ? err : new Error(String(err))
  const message = base.message ?? ''
  const match = message.match(/^([A-Z]+)\\s+(.*)$/)

  if (match) {
    const normalized = new Error(match[2])
    normalized.name = match[1]
    return normalized
  }

  if (!base.name || base.name === 'Error') {
    base.name = 'ERR'
  }

  return base
}
