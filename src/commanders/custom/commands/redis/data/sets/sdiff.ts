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

export class SdiffCommandDefinition
  implements SchemaCommandRegistration<[Buffer, Buffer[]]>
{
  metadata = defineCommand('sdiff', {
    arity: -2, // SDIFF key [key ...]
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
        sets.push(new SetDataType())
        continue
      }
      if (!(existing instanceof SetDataType)) {
        throw new WrongType()
      }
      sets.push(existing)
    }
    const [firstSet, ...otherSets] = sets
    transport.write(firstSet.sdiff(otherSets))
  }
}

export default function (db: DB) {
  return createSchemaCommand(new SdiffCommandDefinition(), { db })
}
