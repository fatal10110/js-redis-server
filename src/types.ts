export interface DBCommandExecutor {
  execute(rawCmd: Buffer, args: Buffer[]): Promise<CommandResult>
  shutdown(): Promise<void>
}

export interface Logger {
  info(msg: unknown, metadata?: Record<string, unknown>): void
  error(msg: unknown, metadata?: Record<string, unknown>): void
}

export type CommandResult = {
  close?: boolean
  response: unknown
}

export interface Command {
  getKeys(rawCmd: Buffer, args: Buffer[]): Buffer[]
  run(rawCmd: Buffer, args: Buffer[]): Promise<CommandResult>
}

export type SlotRange = [number, number]
export type DiscoveryNode = {
  host: string
  port: number
  slots: SlotRange[]
  id: string
}

export interface ClusterCommanderFactory {
  createCommander(mySelfId: string): DBCommandExecutor
  createReadOnlyCommander(mySelfId: string): DBCommandExecutor
  shutdown(): Promise<void>
}

export interface DiscoveryService {
  getAll(): DiscoveryNode[]
  isMaster(id: string): boolean
  getMaster(id: string): DiscoveryNode
  getById(id: string): DiscoveryNode
  getBySlot(slot: number): DiscoveryNode
}
