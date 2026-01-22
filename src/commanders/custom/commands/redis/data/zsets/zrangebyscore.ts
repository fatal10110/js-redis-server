import { ExpectedFloat, WrongType } from '../../../../../../core/errors'
import { SortedSetDataType } from '../../../../data-structures/zset'
import { defineCommand, CommandCategory } from '../../../metadata'
import {
  SchemaCommand,
  CommandContext,
} from '../../../../schema/schema-command'
import { t } from '../../../../schema'
import { DB } from '../../../../db'

export class ZrangebyscoreCommand extends SchemaCommand<
  [Buffer, string, string]
> {
  constructor(private readonly db: DB) {
    super()
  }

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

  protected schema = t.tuple([t.key(), t.string(), t.string()])

  protected execute(
    [key, minStr, maxStr]: [Buffer, string, string],
    { transport }: CommandContext,
  ) {
    const min = parseFloat(minStr)
    const max = parseFloat(maxStr)
    if (Number.isNaN(min) || Number.isNaN(max)) {
      throw new ExpectedFloat()
    }
    const existing = this.db.get(key)
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
