import { WrongType } from '../../../../../../core/errors'
import { SetDataType } from '../../../../data-structures/set'
import { defineCommand, CommandCategory } from '../../../metadata'
import {
  SchemaCommand,
  CommandContext,
} from '../../../../schema/schema-command'
import { t } from '../../../../schema'

export class SdiffCommand extends SchemaCommand<[Buffer, Buffer[]]> {
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

  protected schema = t.tuple([t.key(), t.variadic(t.key())])

  protected execute(
    [firstKey, restKeys]: [Buffer, Buffer[]],
    { db, transport }: CommandContext,
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
