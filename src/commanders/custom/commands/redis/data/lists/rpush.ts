import { WrongType } from '../../../../../../core/errors'
import { ListDataType } from '../../../../data-structures/list'
import { DB } from '../../../../db'
import { defineCommand, CommandCategory } from '../../../metadata'
import {
  createSchemaCommand,
  SchemaCommandRegistration,
  SchemaCommandContext,
  t,
} from '../../../../schema'

export class RpushCommandDefinition
  implements SchemaCommandRegistration<[Buffer, Buffer, Buffer[]]>
{
  metadata = defineCommand('rpush', {
    arity: -3, // RPUSH key element [element ...]
    flags: {
      write: true,
      denyoom: true,
      fast: true,
    },
    firstKey: 0,
    lastKey: 0,
    keyStep: 1,
    categories: [CommandCategory.LIST],
  })

  schema = t.tuple([t.key(), t.string(), t.variadic(t.string())])

  handler(
    [key, firstValue, restValues]: [Buffer, Buffer, Buffer[]],
    { db, transport }: SchemaCommandContext,
  ) {
    const existing = db.get(key)
    if (existing !== null && !(existing instanceof ListDataType)) {
      throw new WrongType()
    }
    const list =
      existing instanceof ListDataType ? existing : new ListDataType()
    if (!(existing instanceof ListDataType)) {
      db.set(key, list)
    }
    list.rpush(firstValue)
    for (const value of restValues) {
      list.rpush(value)
    }
    transport.write(list.llen())
  }
}

export default function (db: DB) {
  return createSchemaCommand(new RpushCommandDefinition(), { db })
}
