import { WrongType } from '../../../../../../core/errors'
import { ListDataType } from '../../../../data-structures/list'
import { defineCommand, CommandCategory } from '../../../metadata'
import {
  SchemaCommand,
  CommandContext,
} from '../../../../schema/schema-command'
import { t } from '../../../../schema'

export class LrangeCommand extends SchemaCommand<[Buffer, number, number]> {
  metadata = defineCommand('lrange', {
    arity: 4, // LRANGE key start stop
    flags: {
      readonly: true,
      fast: true,
    },
    firstKey: 0,
    lastKey: 0,
    keyStep: 1,
    categories: [CommandCategory.LIST],
  })

  protected schema = t.tuple([t.key(), t.integer(), t.integer()])

  protected execute(
    [key, start, stop]: [Buffer, number, number],
    { db, transport }: CommandContext,
  ) {
    const existing = db.get(key)
    if (existing === null) {
      transport.write([])
      return
    }
    if (!(existing instanceof ListDataType)) {
      throw new WrongType()
    }
    transport.write(existing.lrange(start, stop))
  }
}
