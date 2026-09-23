// Data model for a database's keyspace. The storage and all of the behavior
// live on `RedisDatabase` (src/state/database.ts); these are just the shapes it
// stores. `KeyspaceMutationTracker` is internal to its private `update` — code
// outside goes through `updateHash`/`updateList`/..., which hand the mutator a
// `Tracked*` wrapper and do the marking for it.

import type { RedisDataValue } from './data-types'

export type ExpirationState =
  | { kind: 'missing' }
  | { kind: 'persistent' }
  | { kind: 'expires'; expiresAt: number }

export type KeyspaceEntry = {
  key: Buffer
  value: RedisDataValue
  expiresAt?: number
}

export type SetOptions = {
  expiresAt?: number
  keepTtl?: boolean
}

export type KeyspaceMutationTracker = {
  // A WATCH-dirtying write: persists the value AND dirties a WATCH on the key.
  markChanged(): void
  // Persist the (possibly brand-new) value and announce it as a keyspace
  // notification without, on its own, dirtying a WATCH. Used for stream
  // consumer-group / last-id metadata changes, which real Redis notifies
  // (`notifyKeyspaceEvent`) but does not treat as touching a WATCH on the
  // stream key (`signalModifiedKey`). A brand-new key still dirties — coming
  // into existence is itself a write — so `RedisDatabase.update` emits a
  // notification-only `notify` event only for in-place changes to an
  // already-existing key.
  markCommitted(): void
}
