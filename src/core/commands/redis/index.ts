import { DiscoveryService } from '../../cluster/network'
import { DB } from '../../db'
import { HandlingResult, Node } from '../../node'
import quitCommand from './quit'
import clientSetName from './client/clientSetName'
import clusterNodes from './cluster/clusterNodes'
import clusterInfo from './cluster/clusterInfo'
import clusterSlots from './cluster/clusterSlots'
import clusterShards from './cluster/clusterShards'
import del from './data/del'
import get from './data/get'
import set from './data/set'
import { Socket } from 'node:net'

export interface DataCommand {
  getKeys(args: Buffer[]): Buffer[]
  run(db: DB, args: Buffer[]): unknown
}

export interface NodeCommand {
  handle(
    discoveryService: DiscoveryService,
    node: Node,
    args: unknown[],
  ): string | Buffer | Iterable<unknown> | void
}

export interface NodeClientCommand {
  handle(node: Node, args: unknown[]): HandlingResult
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

const clientCommands = {
  setname: clientSetName,
}

const nodeCommands = {
  quit: quitCommand,
}

export default {
  data: dataCommands,
  cluster: clusterCommands,
  client: clientCommands,
  node: nodeCommands,
}
