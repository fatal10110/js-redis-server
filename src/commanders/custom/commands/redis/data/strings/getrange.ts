import { WrongType } from '../../../../../../core/errors'
import { StringDataType } from '../../../../data-structures/string'
import { DB } from '../../../../db'
import { defineCommand, CommandCategory } from '../../../metadata'
import {
  createSchemaCommand,
  SchemaCommandRegistration,
  t,
} from '../../../../schema'

const metadata = defineCommand('getrange', {
  arity: 4, // GETRANGE key start end
  flags: {
    readonly: true,
  },
  firstKey: 0,
  lastKey: 0,
  keyStep: 1,
  categories: [CommandCategory.STRING],
})

export const GetrangeCommandDefinition: SchemaCommandRegistration<
  [Buffer, number, number]
> = {
  metadata,
  schema: t.tuple([t.key(), t.integer(), t.integer()]),
  handler: ([key, start, end], { db, transport }) => {
    const existing = db.get(key)

    if (existing === null) {
      transport.write(Buffer.from(''))
      return
    }

    if (!(existing instanceof StringDataType)) {
      throw new WrongType()
    }

    const buffer = existing.data
    const length = buffer.length

    // Handle negative indices
    if (start < 0) {
      start = length + start
    }
    if (end < 0) {
      end = length + end
    }

    // Clamp to valid range
    if (start < 0) start = 0
    if (end >= length) end = length - 1

    // If start > end or start >= length, return empty string
    if (start > end || start >= length) {
      transport.write(Buffer.from(''))
      return
    }

    // Extract substring (end is inclusive)
    const result = buffer.slice(start, end + 1)
    transport.write(result)
  },
}

export default function (db: DB) {
  return createSchemaCommand(GetrangeCommandDefinition, { db })
}
