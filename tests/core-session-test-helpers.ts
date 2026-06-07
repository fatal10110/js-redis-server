import {
  ClientSession,
  RedisServerState,
  createRedisCommandExecutor,
  createRedisCommandRegistry,
} from '../src'
import type { CommandDefinition } from '../src'

export function createRedisSessionHarness(options?: {
  databaseCount?: number
  extraCommands?: readonly CommandDefinition[]
}) {
  const server = new RedisServerState({
    databaseCount: options?.databaseCount ?? 1,
  })
  const registry = createRedisCommandRegistry(options?.extraCommands)
  const executor = createRedisCommandExecutor({
    extraCommands: options?.extraCommands,
  })
  const session = new ClientSession({ server, executor })

  return { server, registry, executor, session }
}
