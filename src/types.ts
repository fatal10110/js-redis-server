import { CommandMetadata } from './commanders/custom/commands/metadata'

export interface DBCommandExecutor {
  shutdown(): Promise<void>
}

export interface ExecutionContext {
  execute(
    transport: Transport,
    rawCmd: Buffer,
    args: Buffer[],
    signal: AbortSignal,
  ): ExecutionContext
  shutdown(): Promise<void>
}

export type CommandResult =
  | string
  | number
  | bigint
  | null
  | Buffer
  | CommandResult[]

export interface Logger {
  info(msg: unknown, metadata?: Record<string, unknown>): void
  error(msg: unknown, metadata?: Record<string, unknown>): void
}

export interface Command {
  /** Command metadata (arity, flags, key positions) */
  readonly metadata: CommandMetadata

  /**
   * Extract keys from command arguments
   * Used for cluster slot routing and WATCH tracking
   */
  getKeys(rawCmd: Buffer, args: Buffer[]): Buffer[]

  /**
   * Execute command
   */
  run(
    rawCmd: Buffer,
    args: Buffer[],
    signal: AbortSignal,
    transport: Transport,
  ): CommandResult | void
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
  write(responseData: unknown): void
  flush(options?: { close?: boolean }): void
  closeAfterFlush(): void
}
