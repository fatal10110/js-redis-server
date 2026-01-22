import { WrongType } from '../../../../../../core/errors'
import { ListDataType } from '../../../../data-structures/list'
import { defineCommand, CommandCategory } from '../../../metadata'
import {
  SchemaCommand,
  CommandContext,
} from '../../../../schema/schema-command'
import { t } from '../../../../schema'

export class RpoplpushCommand extends SchemaCommand<[Buffer, Buffer]> {
  metadata = defineCommand('rpoplpush', {
    arity: 3, // RPOPLPUSH source destination
    flags: {
      write: true,
      denyoom: true,
    },
    firstKey: 0,
    lastKey: 1,
    keyStep: 1,
    categories: [CommandCategory.LIST],
  })

  protected schema = t.tuple([t.key(), t.key()])

  protected execute(
    [source, destination]: [Buffer, Buffer],
    { db, transport }: CommandContext,
  ) {
    const sourceData = db.get(source)

    if (sourceData === null) {
      transport.write(null)
      return
    }

    if (!(sourceData instanceof ListDataType)) {
      throw new WrongType()
    }

    const value = sourceData.rpop()

    if (value === null) {
      transport.write(null)
      return
    }

    // Get or create destination list
    let destData = db.get(destination)

    if (destData === null) {
      const newList = new ListDataType()
      db.set(destination, newList)
      newList.lpush(value)
    } else if (!(destData instanceof ListDataType)) {
      throw new WrongType()
    } else {
      destData.lpush(value)
    }

    transport.write(value)
  }
}
