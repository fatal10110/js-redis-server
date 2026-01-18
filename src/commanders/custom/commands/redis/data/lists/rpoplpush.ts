import { WrongType } from '../../../../../../core/errors'
import { ListDataType } from '../../../../data-structures/list'
import { DB } from '../../../../db'
import { defineCommand, CommandCategory } from '../../../metadata'
import {
  createSchemaCommand,
  SchemaCommandRegistration,
  t,
} from '../../../../schema'

const metadata = defineCommand('rpoplpush', {
  arity: 3, // RPOPLPUSH source destination
  flags: {
    write: true,
    denyoom: true,
  },
  firstKey: 0,
  lastKey: 1,
  keyStep: 1,
  categories: [CommandCategory.LIST],
})

export const RpoplpushCommandDefinition: SchemaCommandRegistration<
  [Buffer, Buffer]
> = {
  metadata,
  schema: t.tuple([t.key(), t.key()]),
  handler: ([source, destination], { db, transport }) => {
    const sourceData = db.get(source)

    if (sourceData === null) {
      transport.write(null)
      return
    }

    if (!(sourceData instanceof ListDataType)) {
      throw new WrongType()
    }

    const value = sourceData.rpop()

    if (value === null) {
      transport.write(null)
      return
    }

    // Get or create destination list
    let destData = db.get(destination)

    if (destData === null) {
      const newList = new ListDataType()
      db.set(destination, newList)
      newList.lpush(value)
    } else if (!(destData instanceof ListDataType)) {
      throw new WrongType()
    } else {
      destData.lpush(value)
    }

    transport.write(value)
  },
}

export default function (db: DB) {
  return createSchemaCommand(RpoplpushCommandDefinition, { db })
}
