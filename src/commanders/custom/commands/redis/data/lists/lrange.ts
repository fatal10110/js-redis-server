import { WrongType } from '../../../../../../core/errors'
import { ListDataType } from '../../../../data-structures/list'
import { DB } from '../../../../db'
import { defineCommand, CommandCategory } from '../../../metadata'
import {
  createSchemaCommand,
  SchemaCommandRegistration,
  t,
} from '../../../../schema'

const metadata = defineCommand('lrange', {
  arity: 4, // LRANGE key start stop
  flags: {
    readonly: true,
    fast: true,
  },
  firstKey: 0,
  lastKey: 0,
  keyStep: 1,
  categories: [CommandCategory.LIST],
})

export const LrangeCommandDefinition: SchemaCommandRegistration<
  [Buffer, number, number]
> = {
  metadata,
  schema: t.tuple([t.key(), t.integer(), t.integer()]),
  handler: async ([key, start, stop], { db }) => {
    const existing = db.get(key)

    if (existing === null) {
      return { response: [] }
    }

    if (!(existing instanceof ListDataType)) {
      throw new WrongType()
    }

    return { response: existing.lrange(start, stop) }
  },
}

export default function (db: DB) {
  return createSchemaCommand(LrangeCommandDefinition, { db })
}
