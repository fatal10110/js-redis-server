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

export class LlenCommandDefinition
  implements SchemaCommandRegistration<[Buffer]>
{
  metadata = defineCommand('llen', {
    arity: 2, // LLEN key
    flags: {
      readonly: true,
      fast: true,
    },
    firstKey: 0,
    lastKey: 0,
    keyStep: 1,
    categories: [CommandCategory.LIST],
  })

  schema = t.tuple([t.key()])

  handler([key]: [Buffer], { db, transport }: SchemaCommandContext) {
    const existing = db.get(key)
    if (existing === null) {
      transport.write(0)
      return
    }
    if (!(existing instanceof ListDataType)) {
      throw new WrongType()
    }
    transport.write(existing.llen())
  }
}

export default function (db: DB) {
  return createSchemaCommand(new LlenCommandDefinition(), { db })
}
