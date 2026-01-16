import { WrongType } from '../../../../../../core/errors'
import { StringDataType } from '../../../../data-structures/string'
import { DB } from '../../../../db'
import { defineCommand, CommandCategory } from '../../../metadata'
import {
  createSchemaCommand,
  SchemaCommandRegistration,
  t,
} from '../../../../schema'

const metadata = defineCommand('get', {
  arity: 2, // GET key
  flags: {
    readonly: true,
    fast: true,
  },
  firstKey: 0,
  lastKey: 0,
  keyStep: 1,
  categories: [CommandCategory.STRING, CommandCategory.GENERIC],
})

export const GetCommandDefinition: SchemaCommandRegistration<[Buffer]> = {
  metadata,
  schema: t.tuple([t.key()]),
  handler: ([key], { db }) => {
    const val = db.get(key)

    if (val === null) {
      return null
    }

    if (!(val instanceof StringDataType)) {
      throw new WrongType()
    }

    return val.data
  },
}

export default function (db: DB) {
  return createSchemaCommand(GetCommandDefinition, { db })
}
