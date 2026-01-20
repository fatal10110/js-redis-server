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

export class SetnxCommandDefinition
  implements SchemaCommandRegistration<[Buffer, string]>
{
  metadata = defineCommand('setnx', {
    arity: 3, // SETNX key value
    flags: {
      write: true,
      denyoom: true,
      fast: true,
    },
    firstKey: 0,
    lastKey: 0,
    keyStep: 1,
    categories: [CommandCategory.STRING],
  })

  schema = t.tuple([t.key(), t.string()])

  handler(
    [key, value]: [Buffer, string],
    { db, transport }: SchemaCommandContext,
  ) {
    const existing = db.get(key)

    if (existing !== null) {
      transport.write(0)
      return
    }

    db.set(key, new StringDataType(Buffer.from(value)))
    transport.write(1)
  }
}

export default function (db: DB) {
  return createSchemaCommand(new SetnxCommandDefinition(), { db })
}
