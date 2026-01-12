export interface DBCommandExecutor {
  execute(
    transport: Transport,
    rawCmd: Buffer,
    args: Buffer[],
    signal: AbortSignal,
  ): Promise<void>
  shutdown(): Promise<void>
}

export interface ExecutionContext {
  execute(
    transport: Transport,
    rawCmd: Buffer,
    args: Buffer[],
    signal: AbortSignal,
  ): Promise<ExecutionContext>
  shutdown(): Promise<void>
}

export type CommandResult = {
  close?: boolean
  response: unknown
}

export interface Logger {
  info(msg: unknown, metadata?: Record<string, unknown>): void
  error(msg: unknown, metadata?: Record<string, unknown>): void
}

export interface Command {
  getKeys(rawCmd: Buffer, args: Buffer[]): Buffer[]
  run(
    rawCmd: Buffer,
    args: Buffer[],
    signal: AbortSignal,
  ): Promise<CommandResult>
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
export interface Transport {
  write(responseData: unknown, close?: boolean): void
}
