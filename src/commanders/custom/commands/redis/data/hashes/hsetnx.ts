import { WrongType } from '../../../../../../core/errors'
import { HashDataType } from '../../../../data-structures/hash'
import { DB } from '../../../../db'
import { defineCommand, CommandCategory } from '../../../metadata'
import {
  createSchemaCommand,
  SchemaCommandRegistration,
  t,
} from '../../../../schema'

const metadata = defineCommand('hsetnx', {
  arity: 4, // HSETNX key field value
  flags: {
    write: true,
    denyoom: true,
    fast: true,
  },
  firstKey: 0,
  lastKey: 0,
  keyStep: 1,
  categories: [CommandCategory.HASH],
})

export const HsetnxCommandDefinition: SchemaCommandRegistration<
  [Buffer, Buffer, Buffer]
> = {
  metadata,
  schema: t.tuple([t.key(), t.string(), t.string()]),
  handler: ([key, field, value], { db, transport }) => {
    const existing = db.get(key)
    if (existing !== null && !(existing instanceof HashDataType)) {
      throw new WrongType()
    }
    const hash =
      existing instanceof HashDataType ? existing : new HashDataType()
    if (!(existing instanceof HashDataType)) {
      db.set(key, hash)
    }
    const result = hash.hsetnx(field, value)
    transport.write(result)
  },
}

export default function (db: DB) {
  return createSchemaCommand(HsetnxCommandDefinition, { db })
}
