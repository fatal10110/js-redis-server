import { WrongType } from '../../../../../../core/errors'
import { ListDataType } from '../../../../data-structures/list'
import { DB } from '../../../../db'
import { defineCommand, CommandCategory } from '../../../metadata'
import {
  createSchemaCommand,
  SchemaCommandRegistration,
  t,
} from '../../../../schema'
const metadata = defineCommand('lrange', {
  arity: 4, // LRANGE key start stop
  flags: {
    readonly: true,
    fast: true,
  },
  firstKey: 0,
  lastKey: 0,
  keyStep: 1,
  categories: [CommandCategory.LIST],
})
export const LrangeCommandDefinition: SchemaCommandRegistration<
  [Buffer, number, number]
> = {
  metadata,
  schema: t.tuple([t.key(), t.integer(), t.integer()]),
  handler: ([key, start, stop], { db, transport }) => {
    const existing = db.get(key)
    if (existing === null) {
      transport.write([])
      return
    }
    if (!(existing instanceof ListDataType)) {
      throw new WrongType()
    }
    transport.write(existing.lrange(start, stop))
  },
}
export default function (db: DB) {
  return createSchemaCommand(LrangeCommandDefinition, { db })
}
