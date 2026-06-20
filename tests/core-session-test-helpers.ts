import {
  ClientSession,
  RedisServerState,
  createRedisCommandExecutor,
  createRedisCommandRegistry,
} from '../src/internal'
import type { CommandDefinition } from '../src/internal'

export function createRedisSessionHarness(options?: {
  databaseCount?: number
  extraCommands?: readonly CommandDefinition[]
  requirepass?: string
}) {
  const server = new RedisServerState({
    databaseCount: options?.databaseCount ?? 1,
    requirepass: options?.requirepass,
  })
  const registry = createRedisCommandRegistry(options?.extraCommands)
  const executor = createRedisCommandExecutor({
    extraCommands: options?.extraCommands,
  })
  const session = new ClientSession({ server, executor })

  return { server, registry, executor, session }
}
