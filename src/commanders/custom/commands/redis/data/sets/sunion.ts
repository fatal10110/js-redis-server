import { WrongType } from '../../../../../../core/errors'
import { SetDataType } from '../../../../data-structures/set'
import { DB } from '../../../../db'
import { defineCommand, CommandCategory } from '../../../metadata'
import {
  createSchemaCommand,
  SchemaCommandRegistration,
  t,
} from '../../../../schema'

const metadata = defineCommand('sunion', {
  arity: -2, // SUNION key [key ...]
  flags: {
    readonly: true,
    fast: true,
  },
  firstKey: 0,
  lastKey: -1,
  keyStep: 1,
  categories: [CommandCategory.SET],
})

export const SunionCommandDefinition: SchemaCommandRegistration<
  [Buffer, Buffer[]]
> = {
  metadata,
  schema: t.tuple([t.key(), t.variadic(t.key())]),
  handler: ([firstKey, restKeys], { db }) => {
    const keys = [firstKey, ...restKeys]

    const sets: SetDataType[] = []
    for (const key of keys) {
      const existing = db.get(key)
      if (existing === null) {
        sets.push(new SetDataType())
        continue
      }

      if (!(existing instanceof SetDataType)) {
        throw new WrongType()
      }

      sets.push(existing)
    }

    const [firstSet, ...otherSets] = sets
    return { response: firstSet.sunion(otherSets) }
  },
}

export default function (db: DB) {
  return createSchemaCommand(SunionCommandDefinition, { db })
}
