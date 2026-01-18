import { WrongType } from '../../../../../../core/errors'
import { SetDataType } from '../../../../data-structures/set'
import { DB } from '../../../../db'
import { defineCommand, CommandCategory } from '../../../metadata'
import {
  createSchemaCommand,
  SchemaCommandRegistration,
  t,
} from '../../../../schema'

const metadata = defineCommand('sinterstore', {
  arity: -3, // SINTERSTORE destination key [key ...]
  flags: {
    write: true,
    denyoom: true,
  },
  firstKey: 0,
  lastKey: -1,
  keyStep: 1,
  categories: [CommandCategory.SET],
})

export const SinterstoreCommandDefinition: SchemaCommandRegistration<
  [Buffer, Buffer, Buffer[]]
> = {
  metadata,
  schema: t.tuple([t.key(), t.key(), t.variadic(t.key())]),
  handler: ([destination, firstKey, otherKeys], { db, transport }) => {
    const firstSet = db.get(firstKey)

    if (firstSet === null) {
      // If first set doesn't exist, result is empty
      db.del(destination)
      transport.write(0)
      return
    }

    if (!(firstSet instanceof SetDataType)) {
      throw new WrongType()
    }

    // Get other sets
    const otherSets: SetDataType[] = []
    for (const key of otherKeys) {
      const data = db.get(key)
      if (data === null) {
        // If any set doesn't exist, result is empty
        db.del(destination)
        transport.write(0)
        return
      }
      if (!(data instanceof SetDataType)) {
        throw new WrongType()
      }
      otherSets.push(data)
    }

    // Calculate intersection
    const resultMembers = firstSet.sinter(otherSets)

    // Store result
    if (resultMembers.length === 0) {
      db.del(destination)
      transport.write(0)
    } else {
      const resultSet = new SetDataType()
      for (const member of resultMembers) {
        resultSet.sadd(member)
      }
      db.set(destination, resultSet)
      transport.write(resultMembers.length)
    }
  },
}

export default function (db: DB) {
  return createSchemaCommand(SinterstoreCommandDefinition, { db })
}
