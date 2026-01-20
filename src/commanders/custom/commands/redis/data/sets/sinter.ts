import { WrongType } from '../../../../../../core/errors'
import { SetDataType } from '../../../../data-structures/set'
import { DB } from '../../../../db'
import { defineCommand, CommandCategory } from '../../../metadata'
import {
  createSchemaCommand,
  SchemaCommandContext,
  SchemaCommandRegistration,
  t,
} from '../../../../schema'

export class SinterCommandDefinition
  implements SchemaCommandRegistration<[Buffer, Buffer[]]>
{
  metadata = defineCommand('sinter', {
    arity: -2, // SINTER key [key ...]
    flags: {
      readonly: true,
      fast: true,
    },
    firstKey: 0,
    lastKey: -1,
    keyStep: 1,
    categories: [CommandCategory.SET],
  })

  schema = t.tuple([t.key(), t.variadic(t.key())])

  handler(
    [firstKey, restKeys]: [Buffer, Buffer[]],
    { db, transport }: SchemaCommandContext,
  ) {
    const keys = [firstKey, ...restKeys]
    const sets: SetDataType[] = []
    for (const key of keys) {
      const existing = db.get(key)
      if (existing === null) {
        transport.write([])
        return
      }
      if (!(existing instanceof SetDataType)) {
        throw new WrongType()
      }
      sets.push(existing)
    }
    const [firstSet, ...otherSets] = sets
    transport.write(firstSet.sinter(otherSets))
  }
}

export default function (db: DB) {
  return createSchemaCommand(new SinterCommandDefinition(), { db })
}
