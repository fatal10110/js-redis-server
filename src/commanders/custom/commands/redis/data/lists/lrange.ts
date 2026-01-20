import { WrongType } from '../../../../../../core/errors'
import { ListDataType } from '../../../../data-structures/list'
import { DB } from '../../../../db'
import { defineCommand, CommandCategory } from '../../../metadata'
import {
  createSchemaCommand,
  SchemaCommandRegistration,
  SchemaCommandContext,
  t,
} from '../../../../schema'

export class LrangeCommandDefinition
  implements SchemaCommandRegistration<[Buffer, number, number]>
{
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

  schema = t.tuple([t.key(), t.integer(), t.integer()])

  handler(
    [key, start, stop]: [Buffer, number, number],
    { db, transport }: SchemaCommandContext,
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

export default function (db: DB) {
  return createSchemaCommand(new LrangeCommandDefinition(), { db })
}
