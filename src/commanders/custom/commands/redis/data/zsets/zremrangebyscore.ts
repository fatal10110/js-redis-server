import { ExpectedFloat, WrongType } from '../../../../../../core/errors'
import { SortedSetDataType } from '../../../../data-structures/zset'
import { defineCommand, CommandCategory } from '../../../metadata'
import {
  SchemaCommand,
  CommandContext,
} from '../../../../schema/schema-command'
import { t } from '../../../../schema'
import { DB } from '../../../../db'

export class ZremrangebyscoreCommand extends SchemaCommand<
  [Buffer, string, string]
> {
  constructor(private readonly db: DB) {
    super()
  }

  metadata = defineCommand('zremrangebyscore', {
    arity: 4, // ZREMRANGEBYSCORE key min max
    flags: {
      write: true,
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
      transport.write(0)
      return
    }
    if (!(existing instanceof SortedSetDataType)) {
      throw new WrongType()
    }
    const removed = existing.zremrangebyscore(min, max)
    transport.write(removed)
  }
}
