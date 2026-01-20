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

export class GetsetCommandDefinition
  implements SchemaCommandRegistration<[Buffer, string]>
{
  metadata = defineCommand('getset', {
    arity: 3, // GETSET key value
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
    let oldValue: Buffer | null = null
    if (existing !== null) {
      if (!(existing instanceof StringDataType)) {
        throw new WrongType()
      }
      oldValue = existing.data
    }
    db.set(key, new StringDataType(Buffer.from(value)))
    transport.write(oldValue)
  }
}

export default function (db: DB) {
  return createSchemaCommand(new GetsetCommandDefinition(), { db })
}
