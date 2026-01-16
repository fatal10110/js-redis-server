import { WrongType } from '../../../../../../core/errors'
import { SetDataType } from '../../../../data-structures/set'
import { DB } from '../../../../db'
import { defineCommand, CommandCategory } from '../../../metadata'
import {
  createSchemaCommand,
  SchemaCommandRegistration,
  t,
} from '../../../../schema'

const metadata = defineCommand('srandmember', {
  arity: -2, // SRANDMEMBER key [count]
  flags: {
    readonly: true,
    random: true,
    noscript: true,
  },
  firstKey: 0,
  lastKey: 0,
  keyStep: 1,
  categories: [CommandCategory.SET],
})

export const SrandmemberCommandDefinition: SchemaCommandRegistration<
  [Buffer, number | undefined]
> = {
  metadata,
  schema: t.tuple([t.key(), t.optional(t.integer())]),
  handler: ([key, count], { db }) => {
    const existing = db.get(key)

    if (existing === null) {
      if (count !== undefined) {
        return { response: [] }
      }
      return { response: null }
    }

    if (!(existing instanceof SetDataType)) {
      throw new WrongType()
    }

    if (count === undefined) {
      const member = existing.srandmember()
      return { response: member }
    }

    const members = existing.srandmember(count)
    return { response: members }
  },
}

export default function (db: DB) {
  return createSchemaCommand(SrandmemberCommandDefinition, { db })
}
