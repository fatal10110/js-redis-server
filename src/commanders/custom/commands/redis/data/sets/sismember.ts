import { WrongType } from '../../../../../../core/errors'
import { SetDataType } from '../../../../data-structures/set'
import { DB } from '../../../../db'
import { defineCommand, CommandCategory } from '../../../metadata'
import {
  createSchemaCommand,
  SchemaCommandRegistration,
  t,
} from '../../../../schema'

const metadata = defineCommand('sismember', {
  arity: 3, // SISMEMBER key member
  flags: {
    readonly: true,
    fast: true,
  },
  firstKey: 0,
  lastKey: 0,
  keyStep: 1,
  categories: [CommandCategory.SET],
})

export const SismemberCommandDefinition: SchemaCommandRegistration<
  [Buffer, Buffer]
> = {
  metadata,
  schema: t.tuple([t.key(), t.string()]),
  handler: async ([key, member], { db }) => {
    const existing = db.get(key)

    if (existing === null) {
      return { response: 0 }
    }

    if (!(existing instanceof SetDataType)) {
      throw new WrongType()
    }

    return { response: existing.sismember(member) ? 1 : 0 }
  },
}

export default function (db: DB) {
  return createSchemaCommand(SismemberCommandDefinition, { db })
}
