import { WrongType } from '../../../../../../core/errors'
import { ListDataType } from '../../../../data-structures/list'
import { defineCommand, CommandCategory } from '../../../metadata'
import {
  SchemaCommand,
  CommandContext,
} from '../../../../schema/schema-command'
import { t } from '../../../../schema'
import { DB } from '../../../../db'

export class LpushxCommand extends SchemaCommand<[Buffer, Buffer, Buffer[]]> {
  constructor(private readonly db: DB) {
    super()
  }

  metadata = defineCommand('lpushx', {
    arity: -3, // LPUSHX key element [element ...]
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
    { transport }: CommandContext,
  ) {
    const data = this.db.get(key)

    // Only push if key exists
    if (data === null) {
      transport.write(0)
      return
    }

    if (!(data instanceof ListDataType)) {
      throw new WrongType()
    }

    // Push elements in order (left to right)
    data.lpush(firstValue)
    for (const value of restValues) {
      data.lpush(value)
    }

    transport.write(data.llen())
  }
}
