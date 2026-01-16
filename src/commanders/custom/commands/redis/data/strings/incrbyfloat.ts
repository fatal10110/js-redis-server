import { WrongType, ExpectedFloat } from '../../../../../../core/errors'
import { StringDataType } from '../../../../data-structures/string'
import { DB } from '../../../../db'
import { defineCommand, CommandCategory } from '../../../metadata'
import {
  createSchemaCommand,
  SchemaCommandRegistration,
  t,
} from '../../../../schema'

const metadata = defineCommand('incrbyfloat', {
  arity: 3, // INCRBYFLOAT key increment
  flags: {
    write: true,
    fast: true,
  },
  firstKey: 0,
  lastKey: 0,
  keyStep: 1,
  categories: [CommandCategory.STRING],
})

export const IncrbyfloatCommandDefinition: SchemaCommandRegistration<
  [Buffer, string]
> = {
  metadata,
  schema: t.tuple([t.key(), t.string()]),
  handler: ([key, incrementStr], { db }) => {
    const increment = parseFloat(incrementStr)
    if (isNaN(increment)) {
      throw new ExpectedFloat()
    }

    const existing = db.get(key)
    let currentValue = 0

    if (existing !== null) {
      if (!(existing instanceof StringDataType)) {
        throw new WrongType()
      }

      currentValue = parseFloat(existing.data.toString())
      if (isNaN(currentValue)) {
        throw new ExpectedFloat()
      }
    }

    const newValue = currentValue + increment
    db.set(key, new StringDataType(Buffer.from(newValue.toString())))

    return Buffer.from(newValue.toString())
  },
}

export default function (db: DB) {
  return createSchemaCommand(IncrbyfloatCommandDefinition, { db })
}
