import { WrongType } from '../../../../../../core/errors'
import { ListDataType } from '../../../../data-structures/list'
import { DB } from '../../../../db'
import { defineCommand, CommandCategory } from '../../../metadata'
import {
  createSchemaCommand,
  SchemaCommandRegistration,
  t,
} from '../../../../schema'

const metadata = defineCommand('llen', {
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

export const LlenCommandDefinition: SchemaCommandRegistration<[Buffer]> = {
  metadata,
  schema: t.tuple([t.key()]),
  handler: async ([key], { db }) => {
    const existing = db.get(key)

    if (existing === null) {
      return { response: 0 }
    }

    if (!(existing instanceof ListDataType)) {
      throw new WrongType()
    }

    return { response: existing.llen() }
  },
}

export default function (db: DB) {
  return createSchemaCommand(LlenCommandDefinition, { db })
}
