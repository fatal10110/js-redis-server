import { WrongType, ExpectedInteger } from '../../../../../../core/errors'
import { StringDataType } from '../../../../data-structures/string'
import { DB } from '../../../../db'
import { defineCommand, CommandCategory } from '../../../metadata'
import {
  createSchemaCommand,
  SchemaCommandRegistration,
  t,
} from '../../../../schema'

const metadata = defineCommand('incrby', {
  arity: 3, // INCRBY key increment
  flags: {
    write: true,
    fast: true,
  },
  firstKey: 0,
  lastKey: 0,
  keyStep: 1,
  categories: [CommandCategory.STRING],
})

export const IncrbyCommandDefinition: SchemaCommandRegistration<
  [Buffer, string]
> = {
  metadata,
  schema: t.tuple([t.key(), t.string()]),
  handler: async ([key, incrementStr], { db }) => {
    const increment = parseInt(incrementStr)
    if (isNaN(increment)) {
      throw new ExpectedInteger()
    }

    const existing = db.get(key)
    let currentValue = 0

    if (existing !== null) {
      if (!(existing instanceof StringDataType)) {
        throw new WrongType()
      }

      currentValue = parseInt(existing.data.toString())
      if (isNaN(currentValue)) {
        throw new ExpectedInteger()
      }
    }

    const newValue = currentValue + increment
    db.set(key, new StringDataType(Buffer.from(newValue.toString())))

    return { response: newValue }
  },
}

export default function (db: DB) {
  return createSchemaCommand(IncrbyCommandDefinition, { db })
}
