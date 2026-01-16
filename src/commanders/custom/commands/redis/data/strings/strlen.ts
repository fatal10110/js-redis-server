import { WrongType } from '../../../../../../core/errors'
import { StringDataType } from '../../../../data-structures/string'
import { DB } from '../../../../db'
import { defineCommand, CommandCategory } from '../../../metadata'
import {
  createSchemaCommand,
  SchemaCommandRegistration,
  t,
} from '../../../../schema'

const metadata = defineCommand('strlen', {
  arity: 2, // STRLEN key
  flags: {
    readonly: true,
    fast: true,
  },
  firstKey: 0,
  lastKey: 0,
  keyStep: 1,
  categories: [CommandCategory.STRING],
})

export const StrlenCommandDefinition: SchemaCommandRegistration<[Buffer]> = {
  metadata,
  schema: t.tuple([t.key()]),
  handler: ([key], { db }) => {
    const val = db.get(key)

    if (val === null) {
      return { response: 0 }
    }

    if (!(val instanceof StringDataType)) {
      throw new WrongType()
    }

    return { response: val.data.length }
  },
}

export default function (db: DB) {
  return createSchemaCommand(StrlenCommandDefinition, { db })
}
