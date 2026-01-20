import { WrongType } from '../../../../../../core/errors'
import { StringDataType } from '../../../../data-structures/string'
import { DB } from '../../../../db'
import { defineCommand, CommandCategory } from '../../../metadata'
import {
  createSchemaCommand,
  SchemaCommandRegistration,
  SchemaCommandContext,
  t,
} from '../../../../schema'

export class GetrangeCommandDefinition
  implements SchemaCommandRegistration<[Buffer, number, number]>
{
  metadata = defineCommand('getrange', {
    arity: 4, // GETRANGE key start end
    flags: {
      readonly: true,
    },
    firstKey: 0,
    lastKey: 0,
    keyStep: 1,
    categories: [CommandCategory.STRING],
  })

  schema = t.tuple([t.key(), t.integer(), t.integer()])

  handler(
    [key, start, end]: [Buffer, number, number],
    { db, transport }: SchemaCommandContext,
  ) {
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
    let startIdx = start
    let endIdx = end
    if (startIdx < 0) {
      startIdx = length + startIdx
    }
    if (endIdx < 0) {
      endIdx = length + endIdx
    }

    // Clamp to valid range
    if (startIdx < 0) startIdx = 0
    if (endIdx >= length) endIdx = length - 1

    // If start > end or start >= length, return empty string
    if (startIdx > endIdx || startIdx >= length) {
      transport.write(Buffer.from(''))
      return
    }

    // Extract substring (end is inclusive)
    const result = buffer.slice(startIdx, endIdx + 1)
    transport.write(result)
  }
}

export default function (db: DB) {
  return createSchemaCommand(new GetrangeCommandDefinition(), { db })
}
