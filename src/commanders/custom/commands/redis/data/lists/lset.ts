import { WrongType, OutOfRangeIndex } from '../../../../../../core/errors'
import { ListDataType } from '../../../../data-structures/list'
import { DB } from '../../../../db'
import { defineCommand, CommandCategory } from '../../../metadata'
import {
  createSchemaCommand,
  SchemaCommandRegistration,
  t,
} from '../../../../schema'

const metadata = defineCommand('lset', {
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

export const LsetCommandDefinition: SchemaCommandRegistration<
  [Buffer, number, Buffer]
> = {
  metadata,
  schema: t.tuple([t.key(), t.integer(), t.string()]),
  handler: ([key, index, value], { db }) => {
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

    return 'OK'
  },
}

export default function (db: DB) {
  return createSchemaCommand(LsetCommandDefinition, { db })
}
