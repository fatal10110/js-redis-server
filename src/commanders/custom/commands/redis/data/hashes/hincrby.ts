import { WrongType } from '../../../../../../core/errors'
import { HashDataType } from '../../../../data-structures/hash'
import { DB } from '../../../../db'
import { defineCommand, CommandCategory } from '../../../metadata'
import {
  createSchemaCommand,
  SchemaCommandRegistration,
  SchemaCommandContext,
  t,
} from '../../../../schema'

export class HincrbyCommandDefinition
  implements SchemaCommandRegistration<[Buffer, Buffer, number]>
{
  metadata = defineCommand('hincrby', {
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

  schema = t.tuple([t.key(), t.string(), t.integer()])

  handler(
    [key, field, increment]: [Buffer, Buffer, number],
    { db, transport }: SchemaCommandContext,
  ) {
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
    transport.write(result)
  }
}

export default function (db: DB) {
  return createSchemaCommand(new HincrbyCommandDefinition(), { db })
}
