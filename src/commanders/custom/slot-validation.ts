import { CorssSlot, MovedError } from '../../core/errors'
import { Command, DiscoveryNode, DiscoveryService } from '../../types'
import clusterKeySlot from 'cluster-key-slot'

export interface Validator {
  validate(cmd: Command, rawCmd: Buffer, args: Buffer[]): void
}

export class SlotValidator implements Validator {
  constructor(
    private readonly discoveryService: DiscoveryService,
    private readonly myself: DiscoveryNode,
  ) {}

  validate(cmd: Command, rawCmd: Buffer, args: Buffer[]): void {
    const keys = cmd.getKeys(rawCmd, args)

    if (!keys.length) {
      return
    }

    const slot = clusterKeySlot.generateMulti(keys)

    if (slot === -1) {
      throw new CorssSlot()
    }

    for (const [min, max] of this.myself.slots) {
      if (slot >= min && slot <= max) {
        return
      }
    }

    const clusterNode = this.discoveryService.getBySlot(slot)

    throw new MovedError(clusterNode.host, clusterNode.port, slot)
  }
}
