import { WrongType } from '../../../../../../core/errors'
import { HashDataType } from '../../../../data-structures/hash'
import { DB } from '../../../../db'
import { defineCommand, CommandCategory } from '../../../metadata'
import {
  createSchemaCommand,
  SchemaCommandRegistration,
  t,
} from '../../../../schema'

const metadata = defineCommand('hkeys', {
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

export const HkeysCommandDefinition: SchemaCommandRegistration<[Buffer]> = {
  metadata,
  schema: t.tuple([t.key()]),
  handler: async ([key], { db }) => {
    const existing = db.get(key)

    if (existing === null) {
      return { response: [] }
    }

    if (!(existing instanceof HashDataType)) {
      throw new WrongType()
    }

    return { response: existing.hkeys() }
  },
}

export default function (db: DB) {
  return createSchemaCommand(HkeysCommandDefinition, { db })
}
