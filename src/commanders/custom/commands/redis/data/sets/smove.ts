import { WrongType } from '../../../../../../core/errors'
import { SetDataType } from '../../../../data-structures/set'
import { defineCommand, CommandCategory } from '../../../metadata'
import {
  SchemaCommand,
  CommandContext,
} from '../../../../schema/schema-command'
import { t } from '../../../../schema'
import { DB } from '../../../../db'

export class SmoveCommand extends SchemaCommand<[Buffer, Buffer, Buffer]> {
  constructor(private readonly db: DB) {
    super()
  }

  metadata = defineCommand('smove', {
    arity: 4, // SMOVE source destination member
    flags: {
      write: true,
      fast: true,
    },
    firstKey: 0,
    lastKey: 1,
    keyStep: 1,
    categories: [CommandCategory.SET],
  })

  protected schema = t.tuple([t.key(), t.key(), t.string()])

  protected execute(
    [sourceKey, destinationKey, member]: [Buffer, Buffer, Buffer],
    { transport }: CommandContext,
  ) {
    const sourceExisting = this.db.get(sourceKey)
    if (sourceExisting === null) {
      transport.write(0)
      return
    }
    if (!(sourceExisting instanceof SetDataType)) {
      throw new WrongType()
    }
    const destinationExisting = this.db.get(destinationKey)
    if (
      destinationExisting !== null &&
      !(destinationExisting instanceof SetDataType)
    ) {
      throw new WrongType()
    }
    const destinationSet =
      destinationExisting instanceof SetDataType
        ? destinationExisting
        : new SetDataType()
    if (!(destinationExisting instanceof SetDataType)) {
      this.db.set(destinationKey, destinationSet)
    }
    const moved = sourceExisting.smove(destinationSet, member)
    if (sourceExisting.scard() === 0) {
      this.db.del(sourceKey)
    }
    transport.write(moved ? 1 : 0)
  }
}
