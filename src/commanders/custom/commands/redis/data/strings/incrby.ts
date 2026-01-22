import { WrongType, ExpectedInteger } from '../../../../../../core/errors'
import { StringDataType } from '../../../../data-structures/string'
import { defineCommand, CommandCategory } from '../../../metadata'
import {
  SchemaCommand,
  CommandContext,
} from '../../../../schema/schema-command'
import { t } from '../../../../schema'

export class IncrbyCommand extends SchemaCommand<[Buffer, string]> {
  metadata = defineCommand('incrby', {
    arity: 3, // INCRBY key increment
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
    [key, incrementStr]: [Buffer, string],
    { db, transport }: CommandContext,
  ) {
    const increment = parseInt(incrementStr)
    if (isNaN(increment)) {
      throw new ExpectedInteger()
    }
    const existing = db.get(key)
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
    const newValue = currentValue + increment
    db.set(key, new StringDataType(Buffer.from(newValue.toString())))
    transport.write(newValue)
  }
}
