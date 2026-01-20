import { ExpectedFloat, WrongType } from '../../../../../../core/errors'
import { SortedSetDataType } from '../../../../data-structures/zset'
import { DB } from '../../../../db'
import { defineCommand, CommandCategory } from '../../../metadata'
import {
  createSchemaCommand,
  SchemaCommandRegistration,
  SchemaCommandContext,
  t,
} from '../../../../schema'

export class ZrangebyscoreCommandDefinition
  implements SchemaCommandRegistration<[Buffer, string, string]>
{
  metadata = defineCommand('zrangebyscore', {
    arity: 4, // ZRANGEBYSCORE key min max
    flags: {
      readonly: true,
    },
    firstKey: 0,
    lastKey: 0,
    keyStep: 1,
    categories: [CommandCategory.ZSET],
  })

  schema = t.tuple([t.key(), t.string(), t.string()])

  handler(
    [key, minStr, maxStr]: [Buffer, string, string],
    { db, transport }: SchemaCommandContext,
  ) {
    const min = parseFloat(minStr)
    const max = parseFloat(maxStr)
    if (Number.isNaN(min) || Number.isNaN(max)) {
      throw new ExpectedFloat()
    }
    const existing = db.get(key)
    if (existing === null) {
      transport.write([])
      return
    }
    if (!(existing instanceof SortedSetDataType)) {
      throw new WrongType()
    }
    const result = existing.zrangebyscore(min, max)
    transport.write(result)
  }
}

export default function createZrangebyscore(db: DB) {
  return createSchemaCommand(new ZrangebyscoreCommandDefinition(), { db })
}
