import { WrongType } from '../../../../../../core/errors'
import { HashDataType } from '../../../../data-structures/hash'
import { DB } from '../../../../db'
import { defineCommand, CommandCategory } from '../../../metadata'
import {
  createSchemaCommand,
  SchemaCommandRegistration,
  t,
} from '../../../../schema'

const metadata = defineCommand('hincrby', {
  arity: 4, // HINCRBY key field increment
  flags: {
    write: true,
    fast: true,
  },
  firstKey: 0,
  lastKey: 0,
  keyStep: 1,
  categories: [CommandCategory.HASH],
})

export const HincrbyCommandDefinition: SchemaCommandRegistration<
  [Buffer, Buffer, number]
> = {
  metadata,
  schema: t.tuple([t.key(), t.string(), t.integer()]),
  handler: ([key, field, increment], { db }) => {
    const existing = db.get(key)

    if (existing !== null && !(existing instanceof HashDataType)) {
      throw new WrongType()
    }

    const hash =
      existing instanceof HashDataType ? existing : new HashDataType()

    if (!(existing instanceof HashDataType)) {
      db.set(key, hash)
    }

    const result = hash.hincrby(field, increment)
    return result
  },
}

export default function (db: DB) {
  return createSchemaCommand(HincrbyCommandDefinition, { db })
}
