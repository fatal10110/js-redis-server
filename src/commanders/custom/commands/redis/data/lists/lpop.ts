import { WrongType } from '../../../../../../core/errors'
import { ListDataType } from '../../../../data-structures/list'
import { DB } from '../../../../db'
import { defineCommand, CommandCategory } from '../../../metadata'
import {
  createSchemaCommand,
  SchemaCommandRegistration,
  t,
} from '../../../../schema'

const metadata = defineCommand('lpop', {
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

export const LpopCommandDefinition: SchemaCommandRegistration<[Buffer]> = {
  metadata,
  schema: t.tuple([t.key()]),
  handler: async ([key], { db }) => {
    const existing = db.get(key)

    if (existing === null) {
      return { response: null }
    }

    if (!(existing instanceof ListDataType)) {
      throw new WrongType()
    }

    const value = existing.lpop()

    if (existing.llen() === 0) {
      db.del(key)
    }

    return { response: value }
  },
}

export default function (db: DB) {
  return createSchemaCommand(LpopCommandDefinition, { db })
}
