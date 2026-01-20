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

export class LtrimCommandDefinition
  implements SchemaCommandRegistration<[Buffer, number, number]>
{
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

  schema = t.tuple([t.key(), t.integer(), t.integer()])

  handler(
    [key, start, stop]: [Buffer, number, number],
    { db, transport }: SchemaCommandContext,
  ) {
    const existing = db.get(key)
    if (existing === null) {
      transport.write('OK')
      return
    }
    if (!(existing instanceof ListDataType)) {
      throw new WrongType()
    }
    existing.ltrim(start, stop)
    if (existing.llen() === 0) {
      db.del(key)
    }
    transport.write('OK')
  }
}

export default function (db: DB) {
  return createSchemaCommand(new LtrimCommandDefinition(), { db })
}
