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

export class HlenCommandDefinition
  implements SchemaCommandRegistration<[Buffer]>
{
  metadata = defineCommand('hlen', {
    arity: 2, // HLEN key
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
      transport.write(0)
      return
    }
    if (!(existing instanceof HashDataType)) {
      throw new WrongType()
    }
    transport.write(existing.hlen())
  }
}

export default function (db: DB) {
  return createSchemaCommand(new HlenCommandDefinition(), { db })
}
