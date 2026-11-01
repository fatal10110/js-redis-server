import type { RedisClientSession } from './redis-context'

const clientIds = new WeakMap<RedisClientSession, number>()
let nextClientId = 1

/**
 * The connection's CLIENT ID: a server-wide counter, assigned the first time
 * anything asks for it, never reused.
 */
export function getClientId(session: RedisClientSession): number {
  const existing = clientIds.get(session)
  if (existing !== undefined) {
    return existing
  }

  const id = nextClientId++
  clientIds.set(session, id)
  return id
}

/** The connected client with CLIENT ID `id`, if any (`lookupClientByID`). */
export function findClientById(
  clients: readonly RedisClientSession[],
  id: number,
): RedisClientSession | undefined {
  return clients.find(client => getClientId(client) === id)
}
