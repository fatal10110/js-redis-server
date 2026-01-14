import { WrongType } from '../../../../../../core/errors'
import { SetDataType } from '../../../../data-structures/set'
import { DB } from '../../../../db'
import { defineCommand, CommandCategory } from '../../../metadata'
import {
  createSchemaCommand,
  SchemaCommandRegistration,
  t,
} from '../../../../schema'

const metadata = defineCommand('scard', {
  arity: 2, // SCARD key
  flags: {
    readonly: true,
    fast: true,
  },
  firstKey: 0,
  lastKey: 0,
  keyStep: 1,
  categories: [CommandCategory.SET],
})

export const ScardCommandDefinition: SchemaCommandRegistration<[Buffer]> = {
  metadata,
  schema: t.tuple([t.key()]),
  handler: async ([key], { db }) => {
    const existing = db.get(key)

    if (existing === null) {
      return { response: 0 }
    }

    if (!(existing instanceof SetDataType)) {
      throw new WrongType()
    }

    return { response: existing.scard() }
  },
}

export default function (db: DB) {
  return createSchemaCommand(ScardCommandDefinition, { db })
}
