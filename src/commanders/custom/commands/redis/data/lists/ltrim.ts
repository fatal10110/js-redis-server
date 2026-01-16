import { WrongType } from '../../../../../../core/errors'
import { ListDataType } from '../../../../data-structures/list'
import { DB } from '../../../../db'
import { defineCommand, CommandCategory } from '../../../metadata'
import {
  createSchemaCommand,
  SchemaCommandRegistration,
  t,
} from '../../../../schema'

const metadata = defineCommand('ltrim', {
  arity: 4, // LTRIM key start stop
  flags: {
    write: true,
  },
  firstKey: 0,
  lastKey: 0,
  keyStep: 1,
  categories: [CommandCategory.LIST],
})

export const LtrimCommandDefinition: SchemaCommandRegistration<
  [Buffer, number, number]
> = {
  metadata,
  schema: t.tuple([t.key(), t.integer(), t.integer()]),
  handler: ([key, start, stop], { db }) => {
    const existing = db.get(key)

    if (existing === null) {
      return 'OK'
    }

    if (!(existing instanceof ListDataType)) {
      throw new WrongType()
    }

    existing.ltrim(start, stop)

    if (existing.llen() === 0) {
      db.del(key)
    }

    return 'OK'
  },
}

export default function (db: DB) {
  return createSchemaCommand(LtrimCommandDefinition, { db })
}
