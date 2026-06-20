import type { StreamId } from './data-types'

// Pure StreamId arithmetic. Lives in the state layer (next to the StreamId type)
// so state-layer code such as TrackedStreamData can use it without importing from
// the command layer. The command-layer ids module re-exports these.

export const MIN_ID: StreamId = { ms: 0n, seq: 0n }

export function formatStreamId(id: StreamId): string {
  return `${id.ms}-${id.seq}`
}

export function streamIdKey(id: StreamId): string {
  return formatStreamId(id)
}

export function compareStreamId(a: StreamId, b: StreamId): number {
  if (a.ms !== b.ms) return a.ms < b.ms ? -1 : 1
  if (a.seq !== b.seq) return a.seq < b.seq ? -1 : 1
  return 0
}

export function cloneStreamId(id: StreamId): StreamId {
  return { ms: id.ms, seq: id.seq }
}

export function maxStreamId(a: StreamId, b: StreamId): StreamId {
  return compareStreamId(a, b) >= 0 ? a : b
}
