import { DB } from './db'
import { DataCommand, NodeCommand } from './commands'
import {
  CorssSlot,
  MovedError,
  UnknownCommand,
  WrongNumberOfArguments,
} from './errors'
import clusterKeySlot from 'cluster-key-slot'
import { DiscoveryService } from './cluster/network'

export type SlotRange = {
  max: number
  min: number
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

  request(rawCmd: Buffer, args: Buffer[]) {
    const cmd = rawCmd.toString().toLowerCase()

    switch (cmd) {
      case 'cluster':
        // Move to cluster commands handler
        const subCommands = args.shift()?.toString()

        if (!subCommands) {
          throw new WrongNumberOfArguments('cluster')
        }

        if (subCommands) {
          return this.commands.cluster[subCommands].handle(
            this.discoveryService!,
            this,
            args,
          )
        }
      case 'command':
        // TODO implement real command docs
        return 'mock command'
      case 'info':
        // TODO implement real command info
        return 'mock info'
      case 'ping':
        return 'PONG'
    }

    if (!(cmd in this.commands.data)) {
      throw new UnknownCommand(rawCmd, args)
    }

    if (!this.slotRange) {
      return this.commands.data[cmd].run(this.db, args)
    }

    const keys = this.commands.data[cmd].getKeys(args)

    const slot = clusterKeySlot.generateMulti(keys)

    if (slot === -1) {
      throw new CorssSlot()
    }

    if (slot >= this.slotRange.min && slot <= this.slotRange.max) {
      return this.commands.data[cmd].run(this.db, args)
    }

    if (this.discoveryService) {
      const destination = this.discoveryService.getNodeAndAdressBySlot(slot)

      throw new MovedError(destination, slot)
    }

    throw new Error(`unknown slot ${slot}`)
  }
}
