import { WrongType } from '../../../../../../core/errors'
import { ListDataType } from '../../../../data-structures/list'
import { defineCommand, CommandCategory } from '../../../metadata'
import {
  SchemaCommand,
  CommandContext,
} from '../../../../schema/schema-command'
import { t } from '../../../../schema'
import { DB } from '../../../../db'

export class LindexCommand extends SchemaCommand<[Buffer, number]> {
  constructor(private readonly db: DB) {
    super()
  }

  metadata = defineCommand('lindex', {
    arity: 3, // LINDEX key index
    flags: {
      readonly: true,
      fast: true,
    },
    firstKey: 0,
    lastKey: 0,
    keyStep: 1,
    categories: [CommandCategory.LIST],
  })

  protected schema = t.tuple([t.key(), t.integer()])

  protected execute(
    [key, index]: [Buffer, number],
    { transport }: CommandContext,
  ) {
    const existing = this.db.get(key)
    if (existing === null) {
      transport.write(null)
      return
    }
    if (!(existing instanceof ListDataType)) {
      throw new WrongType()
    }
    transport.write(existing.lindex(index))
  }
}
