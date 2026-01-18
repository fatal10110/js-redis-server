import { WrongType } from '../../../../../../core/errors'
import { HashDataType } from '../../../../data-structures/hash'
import { DB } from '../../../../db'
import { defineCommand, CommandCategory } from '../../../metadata'
import {
  createSchemaCommand,
  SchemaCommandRegistration,
  t,
} from '../../../../schema'

const metadata = defineCommand('hstrlen', {
  arity: 3, // HSTRLEN key field
  flags: {
    readonly: true,
    fast: true,
  },
  firstKey: 0,
  lastKey: 0,
  keyStep: 1,
  categories: [CommandCategory.HASH],
})

export const HstrlenCommandDefinition: SchemaCommandRegistration<
  [Buffer, string]
> = {
  metadata,
  schema: t.tuple([t.key(), t.string()]),
  handler: ([key, field], { db, transport }) => {
    const data = db.get(key)

    if (data === null) {
      transport.write(0)
      return
    }

    if (!(data instanceof HashDataType)) {
      throw new WrongType()
    }

    const value = data.hget(Buffer.from(field))
    transport.write(value ? value.length : 0)
  },
}

export default function (db: DB) {
  return createSchemaCommand(HstrlenCommandDefinition, { db })
}
