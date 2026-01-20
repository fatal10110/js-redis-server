import { WrongType } from '../../../../../../core/errors'
import { HashDataType } from '../../../../data-structures/hash'
import { DB } from '../../../../db'
import { defineCommand, CommandCategory } from '../../../metadata'
import {
  createSchemaCommand,
  SchemaCommandRegistration,
  SchemaCommandContext,
  t,
} from '../../../../schema'

export class HkeysCommandDefinition
  implements SchemaCommandRegistration<[Buffer]>
{
  metadata = defineCommand('hkeys', {
    arity: 2, // HKEYS key
    flags: {
      readonly: true,
      fast: true,
    },
    firstKey: 0,
    lastKey: 0,
    keyStep: 1,
    categories: [CommandCategory.HASH],
  })

  schema = t.tuple([t.key()])

  handler([key]: [Buffer], { db, transport }: SchemaCommandContext) {
    const existing = db.get(key)
    if (existing === null) {
      transport.write([])
      return
    }
    if (!(existing instanceof HashDataType)) {
      throw new WrongType()
    }
    transport.write(existing.hkeys())
  }
}

export default function (db: DB) {
  return createSchemaCommand(new HkeysCommandDefinition(), { db })
}
