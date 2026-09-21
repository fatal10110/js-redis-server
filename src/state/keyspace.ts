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
  // Persist the (possibly brand-new) value without, on its own, dirtying a
  // WATCH. Used for stream consumer-group / pending-entry metadata changes,
  // which real Redis does not treat as touching a WATCH on the stream key. A
  // brand-new key still dirties — coming into existence is itself a write — so
  // `RedisDatabase.update` only suppresses the dirty signal for in-place
  // changes to an already-existing key.
  //
  // Caveat: WATCH-faithful, but not notification-faithful. In that in-place
  // case the dirty signal is suppressed by dropping the mutation event
  // outright, and the same bus drives keyspace notifications, so the
  // notification real Redis would still fire is lost with it. Real Redis keeps
  // the two signals independent (`signalModifiedKey` vs `notifyKeyspaceEvent`);
  // splitting them here is tracked in #379.
  markCommitted(): void
}
