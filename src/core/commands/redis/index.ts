import { DiscoveryService } from '../../cluster/network'
import { CommandsInput, HandlingResult, Node } from '../../node'
import quitCommand from './quit'

import clientSetName from './client/clientSetName'
import clusterNodes from './cluster/clusterNodes'
import clusterInfo from './cluster/clusterInfo'
import clusterSlots from './cluster/clusterSlots'
import ping from './ping'
import mget from './data/mget'
import clusterShards from './cluster/clusterShards'
import del from './data/del'
import get from './data/get'
import set from './data/set'
import evalCommand from './eval'

export interface DataCommand {
  getKeys(args: Buffer[]): Buffer[]
  run(node: Node, args: Buffer[]): unknown
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
export type Command = (node: Node, args: Buffer[]) => CommandResult

const stringCommands = {
  get,
}

const dataCommands: Record<string, DataCommand> = {
  del,
  set,
  mget,
  eval: evalCommand,
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
  ping,
}

const commands: CommandsInput = {
  data: dataCommands,
  cluster: clusterCommands,
  client: clientCommands,
  node: nodeCommands,
}

export default commands
