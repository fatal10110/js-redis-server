import { WrongType } from '../../../../../../core/errors'
import { HashDataType } from '../../../../data-structures/hash'
import { DB } from '../../../../db'
import { defineCommand, CommandCategory } from '../../../metadata'
import {
  createSchemaCommand,
  SchemaCommandRegistration,
  t,
} from '../../../../schema'

const metadata = defineCommand('hexists', {
  arity: 3, // HEXISTS key field
  flags: {
    readonly: true,
    fast: true,
  },
  firstKey: 0,
  lastKey: 0,
  keyStep: 1,
  categories: [CommandCategory.HASH],
})

export const HexistsCommandDefinition: SchemaCommandRegistration<
  [Buffer, Buffer]
> = {
  metadata,
  schema: t.tuple([t.key(), t.string()]),
  handler: ([key, field], { db }) => {
    const existing = db.get(key)

    if (existing === null) {
      return 0
    }

    if (!(existing instanceof HashDataType)) {
      throw new WrongType()
    }

    return existing.hexists(field) ? 1 : 0
  },
}

export default function (db: DB) {
  return createSchemaCommand(HexistsCommandDefinition, { db })
}
