import {
  ExpectedIntegerError,
  InvalidStreamIdError,
} from '../../core/redis-error'
import type { StreamId } from '../../state/data-types'
import { MIN_ID } from '../../state/stream-ids'

// StreamId arithmetic now lives in the state layer; re-export it here so the
// streams command modules keep importing it from a single place.
export {
  MIN_ID,
  formatStreamId,
  streamIdKey,
  compareStreamId,
  cloneStreamId,
  maxStreamId,
} from '../../state/stream-ids'

export const MAX_UINT64 = (1n << 64n) - 1n
export const MAX_ID: StreamId = { ms: MAX_UINT64, seq: MAX_UINT64 }

export function bufferId(value: Buffer): string {
  return value.toString('hex')
}

export function parseUint64(token: string): bigint | null {
  if (!/^\d+$/.test(token)) return null
  const value = BigInt(token)
  return value > MAX_UINT64 ? null : value
}

// XDEL ids are exact: "<ms>-<seq>", or "<ms>" meaning "<ms>-0".
export function parseExactId(token: string): StreamId {
  const dash = token.indexOf('-')
  if (dash === -1) {
    const ms = parseUint64(token)
    if (ms === null) throw new InvalidStreamIdError()
    return { ms, seq: 0n }
  }

  const ms = parseUint64(token.slice(0, dash))
  const seq = parseUint64(token.slice(dash + 1))
  if (ms === null || seq === null) throw new InvalidStreamIdError()
  return { ms, seq }
}

// XRANGE/XREVRANGE bound. `-`/`+` are the open ends; a leading `(` is exclusive;
// a bare ms defaults its seq to 0 (start) or the max (end).
export type RangeBound = { id: StreamId; exclusive: boolean }

export function parseRangeId(token: string, isStart: boolean): RangeBound {
  let exclusive = false
  let body = token
  if (body.startsWith('(')) {
    exclusive = true
    body = body.slice(1)
  }

  if (body === '-') return { id: MIN_ID, exclusive }
  if (body === '+') return { id: MAX_ID, exclusive }

  const dash = body.indexOf('-')
  if (dash === -1) {
    const ms = parseUint64(body)
    if (ms === null) throw new InvalidStreamIdError()
    return { id: { ms, seq: isStart ? 0n : MAX_UINT64 }, exclusive }
  }

  const ms = parseUint64(body.slice(0, dash))
  const seq = parseUint64(body.slice(dash + 1))
  if (ms === null || seq === null) throw new InvalidStreamIdError()
  return { id: { ms, seq }, exclusive }
}

// cmp = compareStreamId(entry, bound). For the lower bound we need entry >= bound
// (or > when exclusive); for the upper bound entry <= bound (or < when exclusive).
export function exclusiveAware(
  cmp: number,
  exclusive: boolean,
  isLowerBound: boolean,
): boolean {
  if (isLowerBound) {
    return exclusive ? cmp > 0 : cmp >= 0
  }
  return exclusive ? cmp < 0 : cmp <= 0
}

export function parseNonNegativeInteger(token: Buffer): number {
  const raw = token.toString()
  if (!/^\d+$/.test(raw)) {
    throw new ExpectedIntegerError()
  }

  const value = Number(raw)
  if (!Number.isSafeInteger(value)) {
    throw new ExpectedIntegerError()
  }

  return value
}
