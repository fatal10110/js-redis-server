import { WrongType } from '../../../../../../core/errors'
import { HashDataType } from '../../../../data-structures/hash'
import { DB } from '../../../../db'
import { defineCommand, CommandCategory } from '../../../metadata'
import {
  createSchemaCommand,
  SchemaCommandRegistration,
  t,
} from '../../../../schema'

const metadata = defineCommand('hgetall', {
  arity: 2, // HGETALL key
  flags: {
    readonly: true,
    fast: true,
  },
  firstKey: 0,
  lastKey: 0,
  keyStep: 1,
  categories: [CommandCategory.HASH],
})

export const HgetallCommandDefinition: SchemaCommandRegistration<[Buffer]> = {
  metadata,
  schema: t.tuple([t.key()]),
  handler: ([key], { db }) => {
    const existing = db.get(key)

    if (existing === null) {
      return []
    }

    if (!(existing instanceof HashDataType)) {
      throw new WrongType()
    }

    return existing.hgetall()
  },
}

export default function (db: DB) {
  return createSchemaCommand(HgetallCommandDefinition, { db })
}
