import { WrongType } from '../../../../../../core/errors'
import { SortedSetDataType } from '../../../../data-structures/zset'
import { DB } from '../../../../db'
import { defineCommand, CommandCategory } from '../../../metadata'
import {
  createSchemaCommand,
  SchemaCommandRegistration,
  t,
} from '../../../../schema'

const metadata = defineCommand('zcard', {
  arity: 2, // ZCARD key
  flags: {
    readonly: true,
    fast: true,
  },
  firstKey: 0,
  lastKey: 0,
  keyStep: 1,
  categories: [CommandCategory.ZSET],
})

export const ZcardCommandDefinition: SchemaCommandRegistration<[Buffer]> = {
  metadata,
  schema: t.tuple([t.key()]),
  handler: ([key], { db }) => {
    const existing = db.get(key)

    if (existing === null) {
      return 0
    }

    if (!(existing instanceof SortedSetDataType)) {
      throw new WrongType()
    }

    return existing.zcard()
  },
}

export default function (db: DB) {
  return createSchemaCommand(ZcardCommandDefinition, { db })
}
