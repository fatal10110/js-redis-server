import { WrongType, OutOfRangeIndex } from '../../../../../../core/errors'
import { ListDataType } from '../../../../data-structures/list'
import { DB } from '../../../../db'
import { defineCommand, CommandCategory } from '../../../metadata'
import {
  createSchemaCommand,
  SchemaCommandRegistration,
  SchemaCommandContext,
  t,
} from '../../../../schema'

export class LsetCommandDefinition
  implements SchemaCommandRegistration<[Buffer, number, Buffer]>
{
  metadata = defineCommand('lset', {
    arity: 4, // LSET key index value
    flags: {
      write: true,
      fast: true,
    },
    firstKey: 0,
    lastKey: 0,
    keyStep: 1,
    categories: [CommandCategory.LIST],
  })

  schema = t.tuple([t.key(), t.integer(), t.string()])

  handler(
    [key, index, value]: [Buffer, number, Buffer],
    { db, transport }: SchemaCommandContext,
  ) {
    const existing = db.get(key)
    if (existing === null) {
      throw new OutOfRangeIndex()
    }
    if (!(existing instanceof ListDataType)) {
      throw new WrongType()
    }
    const success = existing.lset(index, value)
    if (!success) {
      throw new OutOfRangeIndex()
    }
    transport.write('OK')
  }
}

export default function (db: DB) {
  return createSchemaCommand(new LsetCommandDefinition(), { db })
}
