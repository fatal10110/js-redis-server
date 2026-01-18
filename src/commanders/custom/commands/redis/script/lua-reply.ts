import { LuaReplyError } from '../../../../../core/errors'
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
    return value.map(item => replyValueToResponse(item, sha))
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
      return new LuaReplyError(rawMessage)
    }
  }

  return Buffer.from(String(value))
}
