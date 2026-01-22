import { WrongType } from '../../../../../../core/errors'
import { ListDataType } from '../../../../data-structures/list'
import { defineCommand, CommandCategory } from '../../../metadata'
import {
  SchemaCommand,
  CommandContext,
} from '../../../../schema/schema-command'
import { t } from '../../../../schema'

export class RpushxCommand extends SchemaCommand<[Buffer, Buffer, Buffer[]]> {
  metadata = defineCommand('rpushx', {
    arity: -3, // RPUSHX key element [element ...]
    flags: {
      write: true,
      denyoom: true,
      fast: true,
    },
    firstKey: 0,
    lastKey: 0,
    keyStep: 1,
    categories: [CommandCategory.LIST],
  })

  protected schema = t.tuple([t.key(), t.string(), t.variadic(t.string())])

  protected execute(
    [key, firstValue, restValues]: [Buffer, Buffer, Buffer[]],
    { db, transport }: CommandContext,
  ) {
    const data = db.get(key)

    // Only push if key exists
    if (data === null) {
      transport.write(0)
      return
    }

    if (!(data instanceof ListDataType)) {
      throw new WrongType()
    }

    // Push elements in order (left to right)
    data.rpush(firstValue)
    for (const value of restValues) {
      data.rpush(value)
    }

    transport.write(data.llen())
  }
}
