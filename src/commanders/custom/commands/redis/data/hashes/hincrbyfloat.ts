import { ExpectedFloat, WrongType } from '../../../../../../core/errors'
import { HashDataType } from '../../../../data-structures/hash'
import { DB } from '../../../../db'
import { defineCommand, CommandCategory } from '../../../metadata'
import {
  createSchemaCommand,
  SchemaCommandRegistration,
  SchemaCommandContext,
  t,
} from '../../../../schema'

export class HincrbyfloatCommandDefinition
  implements SchemaCommandRegistration<[Buffer, Buffer, string]>
{
  metadata = defineCommand('hincrbyfloat', {
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

  schema = t.tuple([t.key(), t.string(), t.string()])

  handler(
    [key, field, incrementStr]: [Buffer, Buffer, string],
    { db, transport }: SchemaCommandContext,
  ) {
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
    transport.write(Buffer.from(result.toString()))
  }
}

export default function (db: DB) {
  return createSchemaCommand(new HincrbyfloatCommandDefinition(), { db })
}
