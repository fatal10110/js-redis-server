import { WrongType } from '../../../../../../core/errors'
import { HashDataType } from '../../../../data-structures/hash'
import { DB } from '../../../../db'
import { defineCommand, CommandCategory } from '../../../metadata'
import {
  createSchemaCommand,
  SchemaCommandRegistration,
  t,
} from '../../../../schema'

const metadata = defineCommand('hget', {
  arity: 3, // HGET key field
  flags: {
    readonly: true,
    fast: true,
  },
  firstKey: 0,
  lastKey: 0,
  keyStep: 1,
  categories: [CommandCategory.HASH],
})

export const HgetCommandDefinition: SchemaCommandRegistration<
  [Buffer, Buffer]
> = {
  metadata,
  schema: t.tuple([t.key(), t.string()]),
  handler: ([key, field], { db }) => {
    const existing = db.get(key)

    if (existing === null) {
      return null
    }

    if (!(existing instanceof HashDataType)) {
      throw new WrongType()
    }

    const value = existing.hget(field)
    return value
  },
}

export default function (db: DB) {
  return createSchemaCommand(HgetCommandDefinition, { db })
}
