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

export class LpushxCommandDefinition
  implements SchemaCommandRegistration<[Buffer, Buffer, Buffer[]]>
{
  metadata = defineCommand('lpushx', {
    arity: -3, // LPUSHX key element [element ...]
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
    const data = db.get(key)

    // Only push if key exists
    if (data === null) {
      transport.write(0)
      return
    }

    if (!(data instanceof ListDataType)) {
      throw new WrongType()
    }

    // Push elements in order (left to right)
    data.lpush(firstValue)
    for (const value of restValues) {
      data.lpush(value)
    }

    transport.write(data.llen())
  }
}

export default function (db: DB) {
  return createSchemaCommand(new LpushxCommandDefinition(), { db })
}
