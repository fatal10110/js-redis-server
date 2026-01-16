import { WrongType } from '../../../../../../core/errors'
import { ListDataType } from '../../../../data-structures/list'
import { DB } from '../../../../db'
import { defineCommand, CommandCategory } from '../../../metadata'
import {
  createSchemaCommand,
  SchemaCommandRegistration,
  t,
} from '../../../../schema'

const metadata = defineCommand('lrem', {
  arity: 4, // LREM key count element
  flags: {
    write: true,
  },
  firstKey: 0,
  lastKey: 0,
  keyStep: 1,
  categories: [CommandCategory.LIST],
})

export const LremCommandDefinition: SchemaCommandRegistration<
  [Buffer, number, Buffer]
> = {
  metadata,
  schema: t.tuple([t.key(), t.integer(), t.string()]),
  handler: ([key, count, value], { db }) => {
    const existing = db.get(key)

    if (existing === null) {
      return 0
    }

    if (!(existing instanceof ListDataType)) {
      throw new WrongType()
    }

    const removed = existing.lrem(count, value)

    if (existing.llen() === 0) {
      db.del(key)
    }

    return removed
  },
}

export default function (db: DB) {
  return createSchemaCommand(LremCommandDefinition, { db })
}
