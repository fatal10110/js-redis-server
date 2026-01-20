import { WrongType } from '../../../../../../core/errors'
import { StringDataType } from '../../../../data-structures/string'
import { DB } from '../../../../db'
import { defineCommand, CommandCategory } from '../../../metadata'
import {
  createSchemaCommand,
  SchemaCommandRegistration,
  SchemaCommandContext,
  t,
} from '../../../../schema'

export class GetdelCommandDefinition
  implements SchemaCommandRegistration<[Buffer]>
{
  metadata = defineCommand('getdel', {
    arity: 2, // GETDEL key
    flags: {
      write: true,
      fast: true,
    },
    firstKey: 0,
    lastKey: 0,
    keyStep: 1,
    categories: [CommandCategory.STRING],
  })

  schema = t.tuple([t.key()])

  handler([key]: [Buffer], { db, transport }: SchemaCommandContext) {
    const existing = db.get(key)

    if (existing === null) {
      transport.write(null)
      return
    }

    if (!(existing instanceof StringDataType)) {
      throw new WrongType()
    }

    const value = existing.data
    db.del(key)
    transport.write(value)
  }
}

export default function (db: DB) {
  return createSchemaCommand(new GetdelCommandDefinition(), { db })
}
