import { DiscoveryService } from '../cluster/network'
import { DB } from '../db'
import { Node } from '../node'
import clusterNodes from './clusterNodes'
import del from './del'
import get from './get'
import set from './set'

export interface DataCommand {
  getKeys(args: Buffer[]): Buffer[]
  run(db: DB, args: Buffer[]): unknown
}

export interface NodeCommand {
  handle(
    discoveryService: DiscoveryService,
    node: Node,
    args: unknown[],
  ): string | Buffer
}

export type CommandResult = number | null | Buffer | Iterable<unknown> | string
export type Command = (db: DB, args: Buffer[]) => CommandResult

const stringCommands = {
  get,
}

const dataCommands: Record<string, DataCommand> = {
  del,
  set,
  ...stringCommands,
}

const clusterCommands = {
  nodes: clusterNodes,
}

export default {
  data: dataCommands,
  cluster: clusterCommands,
}
