import { ExpectedFloat, WrongType } from '../../../../../../core/errors'
import { HashDataType } from '../../../../data-structures/hash'
import { DB } from '../../../../db'
import { defineCommand, CommandCategory } from '../../../metadata'
import {
  createSchemaCommand,
  SchemaCommandRegistration,
  t,
} from '../../../../schema'

const metadata = defineCommand('hincrbyfloat', {
  arity: 4, // HINCRBYFLOAT key field increment
  flags: {
    write: true,
    fast: true,
  },
  firstKey: 0,
  lastKey: 0,
  keyStep: 1,
  categories: [CommandCategory.HASH],
})

export const HincrbyfloatCommandDefinition: SchemaCommandRegistration<
  [Buffer, Buffer, string]
> = {
  metadata,
  schema: t.tuple([t.key(), t.string(), t.string()]),
  handler: ([key, field, incrementStr], { db }) => {
    const increment = parseFloat(incrementStr)
    if (Number.isNaN(increment)) {
      throw new ExpectedFloat()
    }

    const existing = db.get(key)

    if (existing !== null && !(existing instanceof HashDataType)) {
      throw new WrongType()
    }

    const hash =
      existing instanceof HashDataType ? existing : new HashDataType()

    if (!(existing instanceof HashDataType)) {
      db.set(key, hash)
    }

    const result = hash.hincrbyfloat(field, increment)
    return { response: Buffer.from(result.toString()) }
  },
}

export default function (db: DB) {
  return createSchemaCommand(HincrbyfloatCommandDefinition, { db })
}
