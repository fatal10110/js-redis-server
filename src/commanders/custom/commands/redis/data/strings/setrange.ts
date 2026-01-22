import { OffsetOutOfRange, WrongType } from '../../../../../../core/errors'
import { StringDataType } from '../../../../data-structures/string'
import { defineCommand, CommandCategory } from '../../../metadata'
import {
  SchemaCommand,
  CommandContext,
} from '../../../../schema/schema-command'
import { t } from '../../../../schema'
import { DB } from '../../../../db'

export class SetrangeCommand extends SchemaCommand<[Buffer, number, string]> {
  constructor(private readonly db: DB) {
    super()
  }

  metadata = defineCommand('setrange', {
    arity: 4, // SETRANGE key offset value
    flags: {
      write: true,
      denyoom: true,
    },
    firstKey: 0,
    lastKey: 0,
    keyStep: 1,
    categories: [CommandCategory.STRING],
  })

  protected schema = t.tuple([t.key(), t.integer({ min: 0 }), t.string()])

  protected execute(
    [key, offset, value]: [Buffer, number, string],
    { transport }: CommandContext,
  ) {
    if (offset < 0) {
      throw new OffsetOutOfRange()
    }

    // Maximum offset is 536870911 (512MB - 1)
    const MAX_OFFSET = 536870911
    if (offset > MAX_OFFSET) {
      throw new OffsetOutOfRange()
    }

    const existing = this.db.get(key)

    if (existing !== null && !(existing instanceof StringDataType)) {
      throw new WrongType()
    }

    const valueBuffer = Buffer.from(value)
    let currentBuffer =
      existing instanceof StringDataType ? existing.data : Buffer.alloc(0)

    // Calculate new buffer size
    const requiredSize = offset + valueBuffer.length

    // If the buffer needs to be extended
    if (requiredSize > currentBuffer.length) {
      const newBuffer = Buffer.alloc(requiredSize)
      currentBuffer.copy(newBuffer, 0)
      currentBuffer = newBuffer
    }

    // Write the value at the offset
    valueBuffer.copy(currentBuffer, offset)

    this.db.set(key, new StringDataType(currentBuffer))
    transport.write(currentBuffer.length)
  }
}
