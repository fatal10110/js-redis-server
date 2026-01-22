import { WrongType, ExpectedFloat } from '../../../../../../core/errors'
import { StringDataType } from '../../../../data-structures/string'
import { defineCommand, CommandCategory } from '../../../metadata'
import {
  SchemaCommand,
  CommandContext,
} from '../../../../schema/schema-command'
import { t } from '../../../../schema'

export class IncrbyfloatCommand extends SchemaCommand<[Buffer, string]> {
  metadata = defineCommand('incrbyfloat', {
    arity: 3, // INCRBYFLOAT key increment
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
    const increment = parseFloat(incrementStr)
    if (isNaN(increment)) {
      throw new ExpectedFloat()
    }
    const existing = db.get(key)
    let currentValue = 0
    if (existing !== null) {
      if (!(existing instanceof StringDataType)) {
        throw new WrongType()
      }
      currentValue = parseFloat(existing.data.toString())
      if (isNaN(currentValue)) {
        throw new ExpectedFloat()
      }
    }
    const newValue = currentValue + increment
    db.set(key, new StringDataType(Buffer.from(newValue.toString())))
    transport.write(Buffer.from(newValue.toString()))
  }
}
