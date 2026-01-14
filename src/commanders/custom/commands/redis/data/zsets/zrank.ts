import { WrongType } from '../../../../../../core/errors'
import { SortedSetDataType } from '../../../../data-structures/zset'
import { DB } from '../../../../db'
import { defineCommand, CommandCategory } from '../../../metadata'
import {
  createSchemaCommand,
  SchemaCommandRegistration,
  t,
} from '../../../../schema'

const metadata = defineCommand('zrank', {
  arity: 3, // ZRANK key member
  flags: {
    readonly: true,
    fast: true,
  },
  firstKey: 0,
  lastKey: 0,
  keyStep: 1,
  categories: [CommandCategory.ZSET],
})

export const ZrankCommandDefinition: SchemaCommandRegistration<
  [Buffer, Buffer]
> = {
  metadata,
  schema: t.tuple([t.key(), t.key()]),
  handler: async ([key, member], { db }) => {
    const existing = db.get(key)

    if (existing === null) {
      return { response: null }
    }

    if (!(existing instanceof SortedSetDataType)) {
      throw new WrongType()
    }

    return { response: existing.zrank(member) }
  },
}

export default function (db: DB) {
  return createSchemaCommand(ZrankCommandDefinition, { db })
}
