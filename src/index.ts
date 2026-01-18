// Core types
export type {
  Command,
  CommandResult,
  DBCommandExecutor,
  ExecutionContext,
  Logger,
  Transport,
  DiscoveryNode,
  DiscoveryService,
  ClusterCommanderFactory,
  SlotRange,
} from './types'

// Errors
export { UserFacedError, UnknownCommand } from './core/errors'

// Transport
export { Resp2Transport } from './core/transports/resp2'

// Commander factories
export {
  createCustomCommander,
  CustomCommanderFactory,
} from './commanders/custom/commander'

export { createCustomClusterCommander } from './commanders/custom/clusterCommander'

// Cluster utilities
export { ClusterNetwork, computeSlotRange } from './core/cluster/network'

// Database (for advanced usage)
export { DB } from './commanders/custom/db'
