import { WrongType } from '../../../../../../core/errors'
import { ListDataType } from '../../../../data-structures/list'
import { defineCommand, CommandCategory } from '../../../metadata'
import {
  SchemaCommand,
  CommandContext,
} from '../../../../schema/schema-command'
import { t } from '../../../../schema'
import { DB } from '../../../../db'

export class LtrimCommand extends SchemaCommand<[Buffer, number, number]> {
  constructor(private readonly db: DB) {
    super()
  }

  metadata = defineCommand('ltrim', {
    arity: 4, // LTRIM key start stop
    flags: {
      write: true,
    },
    firstKey: 0,
    lastKey: 0,
    keyStep: 1,
    categories: [CommandCategory.LIST],
  })

  protected schema = t.tuple([t.key(), t.integer(), t.integer()])

  protected execute(
    [key, start, stop]: [Buffer, number, number],
    { transport }: CommandContext,
  ) {
    const existing = this.db.get(key)
    if (existing === null) {
      transport.write('OK')
      return
    }
    if (!(existing instanceof ListDataType)) {
      throw new WrongType()
    }
    existing.ltrim(start, stop)
    if (existing.llen() === 0) {
      this.db.del(key)
    }
    transport.write('OK')
  }
}
