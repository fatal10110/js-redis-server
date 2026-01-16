import { WrongType } from '../../../../../../core/errors'
import { SetDataType } from '../../../../data-structures/set'
import { DB } from '../../../../db'
import { defineCommand, CommandCategory } from '../../../metadata'
import {
  createSchemaCommand,
  SchemaCommandRegistration,
  t,
} from '../../../../schema'

const metadata = defineCommand('smembers', {
  arity: 2, // SMEMBERS key
  flags: {
    readonly: true,
    fast: true,
  },
  firstKey: 0,
  lastKey: 0,
  keyStep: 1,
  categories: [CommandCategory.SET],
})

export const SmembersCommandDefinition: SchemaCommandRegistration<[Buffer]> = {
  metadata,
  schema: t.tuple([t.key()]),
  handler: ([key], { db }) => {
    const existing = db.get(key)

    if (existing === null) {
      return { response: [] }
    }

    if (!(existing instanceof SetDataType)) {
      throw new WrongType()
    }

    return { response: existing.smembers() }
  },
}

export default function (db: DB) {
  return createSchemaCommand(SmembersCommandDefinition, { db })
}
