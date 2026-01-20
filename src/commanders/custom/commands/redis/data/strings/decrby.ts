import { WrongType, ExpectedInteger } from '../../../../../../core/errors'
import { StringDataType } from '../../../../data-structures/string'
import { DB } from '../../../../db'
import { defineCommand, CommandCategory } from '../../../metadata'
import {
  createSchemaCommand,
  SchemaCommandRegistration,
  SchemaCommandContext,
  t,
} from '../../../../schema'

export class DecrbyCommandDefinition
  implements SchemaCommandRegistration<[Buffer, string]>
{
  metadata = defineCommand('decrby', {
    arity: 3, // DECRBY key decrement
    flags: {
      write: true,
      fast: true,
    },
    firstKey: 0,
    lastKey: 0,
    keyStep: 1,
    categories: [CommandCategory.STRING],
  })

  schema = t.tuple([t.key(), t.string()])

  handler(
    [key, decrementStr]: [Buffer, string],
    { db, transport }: SchemaCommandContext,
  ) {
    const decrement = parseInt(decrementStr)
    if (isNaN(decrement)) {
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
    const newValue = currentValue - decrement
    db.set(key, new StringDataType(Buffer.from(newValue.toString())))
    transport.write(newValue)
  }
}

export default function (db: DB) {
  return createSchemaCommand(new DecrbyCommandDefinition(), { db })
}
