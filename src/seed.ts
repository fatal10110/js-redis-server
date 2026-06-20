import type { RedisCluster } from './cluster'
import type { RedisDatabase, RedisServerState } from './state'

/**
 * Public seeding contract for {@link RedisMock}. Callers describe data with
 * plain keys/types/values (plus optional `ttlMs` and `db`); the mock owns the
 * conversion into the internal {@link RedisDataValue} representation and the
 * placement onto the right database (and, in cluster mode, the right node).
 *
 * Streams are intentionally not seedable yet — their public entry shape (entry
 * ids, fields, consumer groups) is not finalized, and exposing the internal
 * stream state would leak implementation details.
 */
export type SeedEntry =
  | {
      key: string
      type: 'string'
      value: string | number
      ttlMs?: number
      db?: number
    }
  | {
      key: string
      type: 'hash'
      value: Record<string, string | number>
      ttlMs?: number
      db?: number
    }
  | {
      key: string
      type: 'list'
      value: (string | number)[]
      ttlMs?: number
      db?: number
    }
  | {
      key: string
      type: 'set'
      value: (string | number)[]
      ttlMs?: number
      db?: number
    }
  | {
      key: string
      type: 'zset'
      value: Record<string, number>
      ttlMs?: number
      db?: number
    }

function toBuffer(value: string | number): Buffer {
  return Buffer.from(typeof value === 'number' ? String(value) : value)
}

function writeEntry(db: RedisDatabase, entry: SeedEntry): void {
  const key = Buffer.from(entry.key)

  switch (entry.type) {
    case 'string':
      db.setString(key, toBuffer(entry.value))
      break
    case 'hash':
      db.updateHash(key, hash => {
        for (const [field, value] of Object.entries(entry.value)) {
          hash.setField(Buffer.from(field), toBuffer(value))
        }
      })
      break
    case 'list':
      db.updateList(key, list => {
        list.pushRight(entry.value.map(toBuffer))
      })
      break
    case 'set':
      db.updateSet(key, set => {
        for (const member of entry.value) {
          set.addMember(toBuffer(member))
        }
      })
      break
    case 'zset':
      db.updateSortedSet(key, zset => {
        for (const [member, score] of Object.entries(entry.value)) {
          zset.setScore(Buffer.from(member), score)
        }
      })
      break
  }

  if (entry.ttlMs !== undefined) {
    if (!Number.isFinite(entry.ttlMs) || entry.ttlMs <= 0) {
      throw new Error(`Invalid ttlMs ${entry.ttlMs} for key "${entry.key}"`)
    }
    db.expire(key, Date.now() + entry.ttlMs)
  }
}

async function writeToDatabase(
  db: RedisDatabase,
  entry: SeedEntry,
): Promise<void> {
  // Respect the per-database serialization turn so seeding never interleaves
  // with an in-flight command on the same database.
  const turn = await db.turnQueue.waitTurn()
  try {
    writeEntry(db, entry)
  } finally {
    turn.release()
  }
}

export async function seedStandalone(
  state: RedisServerState,
  entries: readonly SeedEntry[],
): Promise<void> {
  for (const entry of entries) {
    await writeToDatabase(state.getDatabase(entry.db ?? 0), entry)
  }
}

export async function seedCluster(
  cluster: RedisCluster,
  entries: readonly SeedEntry[],
): Promise<void> {
  for (const entry of entries) {
    const key = Buffer.from(entry.key)
    const slot = cluster.topology.calculateSlot(key)
    const owner = cluster.topology.getSlotOwner(slot)
    if (!owner) {
      throw new Error(
        `No master owns slot ${slot} for key "${entry.key}"; cannot seed`,
      )
    }

    const handle = cluster.nodes.find(node => node.id === owner.id)
    if (!handle) {
      throw new Error(`Missing node handle for slot owner ${owner.id}`)
    }

    // Writing into the master's keyspace fires mutation events that the
    // existing replication links propagate to replicas — no extra work here.
    await writeToDatabase(handle.server.getDatabase(entry.db ?? 0), entry)
  }
}
