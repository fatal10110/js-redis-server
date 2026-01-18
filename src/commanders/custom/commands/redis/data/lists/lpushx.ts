import { WrongType } from '../../../../../../core/errors'
import { ListDataType } from '../../../../data-structures/list'
import { DB } from '../../../../db'
import { defineCommand, CommandCategory } from '../../../metadata'
import {
  createSchemaCommand,
  SchemaCommandRegistration,
  t,
} from '../../../../schema'

const metadata = defineCommand('lpushx', {
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

export const LpushxCommandDefinition: SchemaCommandRegistration<
  [Buffer, Buffer, Buffer[]]
> = {
  metadata,
  schema: t.tuple([t.key(), t.string(), t.variadic(t.string())]),
  handler: ([key, firstValue, restValues], { db, transport }) => {
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
  },
}

export default function (db: DB) {
  return createSchemaCommand(LpushxCommandDefinition, { db })
}
