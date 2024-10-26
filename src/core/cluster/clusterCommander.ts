import { DB } from '../db'
import { CorssSlot, MovedError } from '../errors'
import clusterKeySlot from 'cluster-key-slot'
import { ClusterNode } from './clusterNode'
import { Socket } from 'net'
import { Command, CommandProvider, CommandsInput } from '../../types'
import { Commander } from '../commander'

export class ClusterCommander implements CommandProvider {
  private readonly nodeCommandProvider: CommandProvider

  constructor(
    private readonly db: DB,
    private readonly clusterNode: ClusterNode,
    commands: CommandsInput,
  ) {
    this.nodeCommandProvider = new Commander(db, commands)
  }

  getOrCreateCommand(socket: Socket, rawCmd: Buffer, args: Buffer[]): Command {
    const cmd = this.nodeCommandProvider.getOrCreateCommand(
      socket,
      rawCmd,
      args,
    )
    const keys = cmd.getKeys(rawCmd, args)

    if (!keys.length) {
      return cmd
    }

    const slot = clusterKeySlot.generateMulti(keys)

    if (slot === -1) {
      throw new CorssSlot()
    }

    if (
      slot >= this.clusterNode.slotRange.min &&
      slot <= this.clusterNode.slotRange.max
    ) {
      return cmd
    }

    const clusterNode = this.clusterNode.getNodeBySlot(slot)

    throw new MovedError(clusterNode, slot)
  }
}
