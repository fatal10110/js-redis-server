import { WrongType } from '../../../../../../core/errors'
import { SetDataType } from '../../../../data-structures/set'
import { DB } from '../../../../db'
import { defineCommand, CommandCategory } from '../../../metadata'
import {
  createSchemaCommand,
  SchemaCommandRegistration,
  t,
} from '../../../../schema'

const metadata = defineCommand('spop', {
  arity: 2, // SPOP key
  flags: {
    write: true,
    random: true,
    fast: true,
    noscript: true,
  },
  firstKey: 0,
  lastKey: 0,
  keyStep: 1,
  categories: [CommandCategory.SET],
})

export const SpopCommandDefinition: SchemaCommandRegistration<[Buffer]> = {
  metadata,
  schema: t.tuple([t.key()]),
  handler: async ([key], { db }) => {
    const existing = db.get(key)

    if (existing === null) {
      return { response: null }
    }

    if (!(existing instanceof SetDataType)) {
      throw new WrongType()
    }

    const member = existing.spop()

    if (existing.scard() === 0) {
      db.del(key)
    }

    return { response: member }
  },
}

export default function (db: DB) {
  return createSchemaCommand(SpopCommandDefinition, { db })
}
