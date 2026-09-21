// Data model for a database's keyspace. The storage itself lives on
// `RedisDatabase` (src/state/database.ts) — these are the shapes it stores and
// the contract its `update` mutators are handed.

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
  // Persist the (possibly brand-new) value without, on its own, dirtying a
  // WATCH. Used for stream consumer-group / pending-entry metadata changes,
  // which real Redis does not treat as touching a WATCH on the stream key. A
  // brand-new key still dirties — coming into existence is itself a write — so
  // `RedisDatabase.update` only suppresses the dirty signal for in-place
  // changes to an already-existing key.
  markCommitted(): void
}
