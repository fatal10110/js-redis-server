import { WrongType } from '../../../../../../core/errors'
import { ListDataType } from '../../../../data-structures/list'
import { DB } from '../../../../db'
import { defineCommand, CommandCategory } from '../../../metadata'
import {
  createSchemaCommand,
  SchemaCommandRegistration,
  t,
} from '../../../../schema'

const metadata = defineCommand('lindex', {
  arity: 3, // LINDEX key index
  flags: {
    readonly: true,
    fast: true,
  },
  firstKey: 0,
  lastKey: 0,
  keyStep: 1,
  categories: [CommandCategory.LIST],
})

export const LindexCommandDefinition: SchemaCommandRegistration<
  [Buffer, number]
> = {
  metadata,
  schema: t.tuple([t.key(), t.integer()]),
  handler: async ([key, index], { db }) => {
    const existing = db.get(key)

    if (existing === null) {
      return { response: null }
    }

    if (!(existing instanceof ListDataType)) {
      throw new WrongType()
    }

    return { response: existing.lindex(index) }
  },
}

export default function (db: DB) {
  return createSchemaCommand(LindexCommandDefinition, { db })
}
