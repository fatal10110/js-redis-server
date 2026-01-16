import { WrongType } from '../../../../../../core/errors'
import { SetDataType } from '../../../../data-structures/set'
import { DB } from '../../../../db'
import { defineCommand, CommandCategory } from '../../../metadata'
import {
  createSchemaCommand,
  SchemaCommandRegistration,
  t,
} from '../../../../schema'
const metadata = defineCommand('smove', {
  arity: 4, // SMOVE source destination member
  flags: {
    write: true,
    fast: true,
  },
  firstKey: 0,
  lastKey: 1,
  keyStep: 1,
  categories: [CommandCategory.SET],
})
export const SmoveCommandDefinition: SchemaCommandRegistration<
  [Buffer, Buffer, Buffer]
> = {
  metadata,
  schema: t.tuple([t.key(), t.key(), t.string()]),
  handler: ([sourceKey, destinationKey, member], { db, transport }) => {
    const sourceExisting = db.get(sourceKey)
    if (sourceExisting === null) {
      transport.write(0)
      return
    }
    if (!(sourceExisting instanceof SetDataType)) {
      throw new WrongType()
    }
    const destinationExisting = db.get(destinationKey)
    if (
      destinationExisting !== null &&
      !(destinationExisting instanceof SetDataType)
    ) {
      throw new WrongType()
    }
    const destinationSet =
      destinationExisting instanceof SetDataType
        ? destinationExisting
        : new SetDataType()
    if (!(destinationExisting instanceof SetDataType)) {
      db.set(destinationKey, destinationSet)
    }
    const moved = sourceExisting.smove(destinationSet, member)
    if (sourceExisting.scard() === 0) {
      db.del(sourceKey)
    }
    transport.write(moved ? 1 : 0)
  },
}
export default function (db: DB) {
  return createSchemaCommand(SmoveCommandDefinition, { db })
}
