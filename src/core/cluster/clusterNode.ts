import { Socket } from 'net'
import { RedisNode } from '../node'
import { AddressInfo } from 'net'
import { ClusterCommander } from './clusterCommander'
import { DB } from '../db'
import { CommandProvider, CommandsInput, Logger, Node } from '../../types'

export type SlotRange = {
  max: number
  min: number
}

export type ClusterNodeAddress = { host: string; port: number }

export interface ClusterNode extends Node {
  readonly id: string
  readonly masterNodeId?: string
  readonly slotRange: SlotRange
  getNodeBySlot(slot: number): ClusterNode
  getClusterNodes(): IterableIterator<ClusterNode>
  getAddress(): ClusterNodeAddress
  listen(): Promise<void>
  close(): Promise<void>
}

export interface DiscoveryService {
  getById(id: string): ClusterNode
  getAll(): IterableIterator<ClusterNode>
  getBySlot(slot: number): ClusterNode
}

export class RedisClusterNode implements ClusterNode {
  private readonly node: RedisNode

  // TODO move to utility
  public readonly id = Math.random().toString(36).substring(2, 10)

  constructor(
    logger: Logger,
    public readonly db: DB,
    public readonly slotRange: SlotRange,
    private readonly discoveryService: DiscoveryService,
    createCommandInput: (node: ClusterNode) => CommandsInput,
    public readonly masterNodeId?: string,
  ) {
    const commands = createCommandInput(this)
    const commandsProvider = new ClusterCommander(db, this, commands)
    this.node = new RedisNode(logger, db, commandsProvider)
  }

  get commandExecutor(): CommandProvider {
    return this.node.commandExecutor
  }

  set commandExecutor(executor: CommandProvider) {
    this.node.commandExecutor = executor
  }

  getNodeBySlot(slot: number): ClusterNode {
    return this.discoveryService.getBySlot(slot)
  }

  getClusterNodes(): IterableIterator<ClusterNode> {
    return this.discoveryService.getAll()
  }

  write(
    socket: Socket,
    responseData: unknown,
    close?: boolean | undefined,
  ): void {
    this.node.write(socket, responseData, close)
  }

  getAddress(): ClusterNodeAddress {
    const address = this.node.server.address()

    if (!address) {
      throw new Error(`Failed to fetch node ${this.id} address`)
    }

    if (address instanceof String) {
      throw new Error(`Could not fetch address from ${address}`)
    }

    return {
      host: '127.0.0.1', // TODO
      port: (address as AddressInfo).port,
    }
  }

  listen(port?: number): Promise<void> {
    return this.node.listen(port)
  }

  close(): Promise<void> {
    return this.node.close()
  }
}
