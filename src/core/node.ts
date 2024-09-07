import { DB } from './db'
import { DataCommand, NodeClientCommand, NodeCommand } from './commands/redis'
import {
  CorssSlot,
  MovedError,
  UnknownCommand,
  UnknwonClientSubCommand,
  UnknwonClusterSubCommand,
  WrongNumberOfArguments,
} from './errors'
import clusterKeySlot from 'cluster-key-slot'
import { DiscoveryService } from './cluster/network'
import { Socket } from 'node:net'

export type SlotRange = {
  max: number
  min: number
}

export type HandlingResult = {
  close?: boolean
  response: unknown
}

export class Node {
  // TODO move to utility
  public readonly id = Math.random().toString(36).substring(2, 10)
  private readonly replicas: Node[] = []

  constructor(
    private readonly db: DB,
    private readonly commands: {
      data: Record<string, DataCommand>
      cluster: Record<string, NodeCommand>
      client: Record<string, NodeClientCommand>
      node: Record<string, NodeClientCommand>
    },
    public slotRange?: SlotRange,
    private readonly discoveryService?: DiscoveryService,
    public readonly master?: Node,
  ) {}

  createReplica(): Node {
    if (this.master) {
      throw new Error(`Can not create replica from replica`)
    }

    // TODO pass replica commands
    const replica = new Node(
      this.db,
      this.commands,
      this.slotRange,
      this.discoveryService,
      this,
    )
    this.replicas.push(replica)
    return replica
  }

  request(rawCmd: Buffer, args: Buffer[]): HandlingResult {
    const cmd = rawCmd.toString().toLowerCase()

    switch (cmd) {
      case 'cluster':
        // Move to cluster commands handler
        const subCommand = args.shift()?.toString().toLowerCase()

        if (!subCommand) {
          throw new WrongNumberOfArguments('cluster')
        }

        if (!(subCommand in this.commands.cluster)) {
          throw new UnknwonClusterSubCommand(subCommand)
        }

        if (subCommand) {
          const response = this.commands.cluster[subCommand].handle(
            this.discoveryService!,
            this,
            args,
          )

          return { response }
        }
      case 'client':
        // Move to cluitn commands handler
        const subClientCommand = args.shift()?.toString().toLowerCase()

        if (!subClientCommand) {
          throw new WrongNumberOfArguments('client')
        }

        if (!(subClientCommand in this.commands.client)) {
          throw new UnknwonClientSubCommand(subClientCommand)
        }

        if (subClientCommand) {
          const response = this.commands.client[subClientCommand].handle(
            this,
            args,
          )
          return { response }
        }
      case 'quit':
        return this.commands.node.quit.handle(this, args)
      case 'command':
        // TODO implement real command docs
        return { response: 'mock command' }
      case 'info':
        // TODO implement real command info
        return { response: 'mock info' }
      case 'ping':
        return { response: 'PONG' }
    }

    if (!(cmd in this.commands.data)) {
      throw new UnknownCommand(rawCmd, args)
    }

    if (!this.slotRange) {
      const response = this.commands.data[cmd].run(this.db, args)
      return { response }
    }

    const keys = this.commands.data[cmd].getKeys(args)

    const slot = clusterKeySlot.generateMulti(keys)

    if (slot === -1) {
      throw new CorssSlot()
    }

    if (slot >= this.slotRange.min && slot <= this.slotRange.max) {
      const response = this.commands.data[cmd].run(this.db, args)
      return { response }
    }

    if (this.discoveryService) {
      const destination = this.discoveryService.getNodeAndAdressBySlot(slot)

      throw new MovedError(destination, slot)
    }

    throw new Error(`unknown slot ${slot}`)
  }
}
