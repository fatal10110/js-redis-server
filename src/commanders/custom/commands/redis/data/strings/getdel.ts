import { WrongType } from '../../../../../../core/errors'
import { StringDataType } from '../../../../data-structures/string'
import { DB } from '../../../../db'
import { defineCommand, CommandCategory } from '../../../metadata'
import {
  createSchemaCommand,
  SchemaCommandRegistration,
  t,
} from '../../../../schema'

const metadata = defineCommand('getdel', {
  arity: 2, // GETDEL key
  flags: {
    write: true,
    fast: true,
  },
  firstKey: 0,
  lastKey: 0,
  keyStep: 1,
  categories: [CommandCategory.STRING],
})

export const GetdelCommandDefinition: SchemaCommandRegistration<[Buffer]> = {
  metadata,
  schema: t.tuple([t.key()]),
  handler: ([key], { db, transport }) => {
    const existing = db.get(key)

    if (existing === null) {
      transport.write(null)
      return
    }

    if (!(existing instanceof StringDataType)) {
      throw new WrongType()
    }

    const value = existing.data
    db.del(key)
    transport.write(value)
  },
}

export default function (db: DB) {
  return createSchemaCommand(GetdelCommandDefinition, { db })
}
