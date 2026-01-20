import { WrongType } from '../../../../../../core/errors'
import { SetDataType } from '../../../../data-structures/set'
import { DB } from '../../../../db'
import { defineCommand, CommandCategory } from '../../../metadata'
import {
  createSchemaCommand,
  SchemaCommandContext,
  SchemaCommandRegistration,
  t,
} from '../../../../schema'

export class SmembersCommandDefinition
  implements SchemaCommandRegistration<[Buffer]>
{
  metadata = defineCommand('smembers', {
    arity: 2, // SMEMBERS key
    flags: {
      readonly: true,
      fast: true,
    },
    firstKey: 0,
    lastKey: 0,
    keyStep: 1,
    categories: [CommandCategory.SET],
  })

  schema = t.tuple([t.key()])

  handler([key]: [Buffer], { db, transport }: SchemaCommandContext) {
    const existing = db.get(key)
    if (existing === null) {
      transport.write([])
      return
    }
    if (!(existing instanceof SetDataType)) {
      throw new WrongType()
    }
    transport.write(existing.smembers())
  }
}

export default function (db: DB) {
  return createSchemaCommand(new SmembersCommandDefinition(), { db })
}
