import { DiscoveryService } from '../../cluster/network'
import { DB } from '../../db'
import { Node } from '../../node'
import clusterNodes from './cluster/clusterNodes'
import clusterInfo from './cluster/clusterInfo'
import clusterSlots from './cluster/clusterSlots'
import clusterShards from './cluster/clusterShards'
import del from './data/del'
import get from './data/get'
import set from './data/set'

export interface DataCommand {
  getKeys(args: Buffer[]): Buffer[]
  run(db: DB, args: Buffer[]): unknown
}

export interface NodeCommand {
  handle(
    discoveryService: DiscoveryService,
    node: Node,
    args: unknown[],
  ): string | Buffer | Iterable<unknown>
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
  info: clusterInfo,
  slots: clusterSlots,
  shards: clusterShards,
}

export default {
  data: dataCommands,
  cluster: clusterCommands,
}
