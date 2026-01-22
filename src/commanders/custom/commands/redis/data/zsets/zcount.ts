import { WrongType, ExpectedFloat } from '../../../../../../core/errors'
import { SortedSetDataType } from '../../../../data-structures/zset'
import { defineCommand, CommandCategory } from '../../../metadata'
import {
  SchemaCommand,
  CommandContext,
} from '../../../../schema/schema-command'
import { t } from '../../../../schema'

export class ZcountCommand extends SchemaCommand<[Buffer, string, string]> {
  metadata = defineCommand('zcount', {
    arity: 4, // ZCOUNT key min max
    flags: {
      readonly: true,
      fast: true,
    },
    firstKey: 0,
    lastKey: 0,
    keyStep: 1,
    categories: [CommandCategory.ZSET],
  })

  protected schema = t.tuple([t.key(), t.string(), t.string()])

  protected execute(
    [key, minStr, maxStr]: [Buffer, string, string],
    { db, transport }: CommandContext,
  ) {
    const min = parseFloat(minStr)
    const max = parseFloat(maxStr)

    if (Number.isNaN(min) || Number.isNaN(max)) {
      throw new ExpectedFloat()
    }

    const data = db.get(key)

    if (data === null) {
      transport.write(0)
      return
    }

    if (!(data instanceof SortedSetDataType)) {
      throw new WrongType()
    }

    transport.write(data.zcount(min, max))
  }
}
