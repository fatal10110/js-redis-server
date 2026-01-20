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

export class StrlenCommandDefinition
  implements SchemaCommandRegistration<[Buffer]>
{
  metadata = defineCommand('strlen', {
    arity: 2, // STRLEN key
    flags: {
      readonly: true,
      fast: true,
    },
    firstKey: 0,
    lastKey: 0,
    keyStep: 1,
    categories: [CommandCategory.STRING],
  })

  schema = t.tuple([t.key()])

  handler([key]: [Buffer], { db, transport }: SchemaCommandContext) {
    const val = db.get(key)
    if (val === null) {
      transport.write(0)
      return
    }
    if (!(val instanceof StringDataType)) {
      throw new WrongType()
    }
    transport.write(val.data.length)
  }
}

export default function (db: DB) {
  return createSchemaCommand(new StrlenCommandDefinition(), { db })
}
