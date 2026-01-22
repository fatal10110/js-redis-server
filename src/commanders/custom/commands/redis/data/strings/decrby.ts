import { WrongType, ExpectedInteger } from '../../../../../../core/errors'
import { StringDataType } from '../../../../data-structures/string'
import { defineCommand, CommandCategory } from '../../../metadata'
import {
  SchemaCommand,
  CommandContext,
} from '../../../../schema/schema-command'
import { t } from '../../../../schema'
import { DB } from '../../../../db'

export class DecrbyCommand extends SchemaCommand<[Buffer, string]> {
  constructor(private readonly db: DB) {
    super()
  }

  metadata = defineCommand('decrby', {
    arity: 3, // DECRBY key decrement
    flags: {
      write: true,
      fast: true,
    },
    firstKey: 0,
    lastKey: 0,
    keyStep: 1,
    categories: [CommandCategory.STRING],
  })

  protected schema = t.tuple([t.key(), t.string()])

  protected execute(
    [key, decrementStr]: [Buffer, string],
    { transport }: CommandContext,
  ) {
    const decrement = parseInt(decrementStr)
    if (isNaN(decrement)) {
      throw new ExpectedInteger()
    }
    const existing = this.db.get(key)
    let currentValue = 0
    if (existing !== null) {
      if (!(existing instanceof StringDataType)) {
        throw new WrongType()
      }
      currentValue = parseInt(existing.data.toString())
      if (isNaN(currentValue)) {
        throw new ExpectedInteger()
      }
    }
    const newValue = currentValue - decrement
    this.db.set(key, new StringDataType(Buffer.from(newValue.toString())))
    transport.write(newValue)
  }
}
