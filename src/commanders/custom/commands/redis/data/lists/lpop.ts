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

export class LpopCommandDefinition
  implements SchemaCommandRegistration<[Buffer]>
{
  metadata = defineCommand('lpop', {
    arity: 2, // LPOP key
    flags: {
      write: true,
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
      transport.write(null)
      return
    }
    if (!(existing instanceof ListDataType)) {
      throw new WrongType()
    }
    const value = existing.lpop()
    if (existing.llen() === 0) {
      db.del(key)
    }
    transport.write(value)
  }
}

export default function (db: DB) {
  return createSchemaCommand(new LpopCommandDefinition(), { db })
}
