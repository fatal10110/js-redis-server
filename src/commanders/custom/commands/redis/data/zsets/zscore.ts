import { WrongType } from '../../../../../../core/errors'
import { SortedSetDataType } from '../../../../data-structures/zset'
import { DB } from '../../../../db'
import { defineCommand, CommandCategory } from '../../../metadata'
import {
  createSchemaCommand,
  SchemaCommandRegistration,
  SchemaCommandContext,
  t,
} from '../../../../schema'

export class ZscoreCommandDefinition
  implements SchemaCommandRegistration<[Buffer, Buffer]>
{
  metadata = defineCommand('zscore', {
    arity: 3, // ZSCORE key member
    flags: {
      readonly: true,
      fast: true,
    },
    firstKey: 0,
    lastKey: 0,
    keyStep: 1,
    categories: [CommandCategory.ZSET],
  })

  schema = t.tuple([t.key(), t.string()])

  handler(
    [key, member]: [Buffer, Buffer],
    { db, transport }: SchemaCommandContext,
  ) {
    const existing = db.get(key)
    if (existing === null) {
      transport.write(null)
      return
    }
    if (!(existing instanceof SortedSetDataType)) {
      throw new WrongType()
    }
    const score = existing.zscore(member)
    const response = score !== null ? Buffer.from(score.toString()) : null
    transport.write(response)
  }
}

export default function (db: DB) {
  return createSchemaCommand(new ZscoreCommandDefinition(), { db })
}
