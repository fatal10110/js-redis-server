import { WrongType } from '../../../../../../core/errors'
import { SetDataType } from '../../../../data-structures/set'
import { DB } from '../../../../db'
import { defineCommand, CommandCategory } from '../../../metadata'
import {
  createSchemaCommand,
  SchemaCommandRegistration,
  t,
} from '../../../../schema'

const metadata = defineCommand('sunionstore', {
  arity: -3, // SUNIONSTORE destination key [key ...]
  flags: {
    write: true,
    denyoom: true,
  },
  firstKey: 0,
  lastKey: -1,
  keyStep: 1,
  categories: [CommandCategory.SET],
})

export const SunionstoreCommandDefinition: SchemaCommandRegistration<
  [Buffer, Buffer, Buffer[]]
> = {
  metadata,
  schema: t.tuple([t.key(), t.key(), t.variadic(t.key())]),
  handler: ([destination, firstKey, otherKeys], { db, transport }) => {
    const firstSet = db.get(firstKey)

    if (firstSet === null) {
      // Start with empty set
      const otherSets: SetDataType[] = []

      // Collect all other sets
      for (const key of otherKeys) {
        const data = db.get(key)
        if (data !== null) {
          if (!(data instanceof SetDataType)) {
            throw new WrongType()
          }
          otherSets.push(data)
        }
      }

      // If all sets are empty, delete destination
      if (otherSets.length === 0) {
        db.del(destination)
        transport.write(0)
        return
      }

      // Calculate union starting from first non-null set
      const resultSet = new SetDataType()
      for (const set of otherSets) {
        const members = set.smembers()
        for (const member of members) {
          resultSet.sadd(member)
        }
      }

      db.set(destination, resultSet)
      transport.write(resultSet.scard())
      return
    }

    if (!(firstSet instanceof SetDataType)) {
      throw new WrongType()
    }

    // Get other sets
    const otherSets: SetDataType[] = []
    for (const key of otherKeys) {
      const data = db.get(key)
      if (data !== null) {
        if (!(data instanceof SetDataType)) {
          throw new WrongType()
        }
        otherSets.push(data)
      }
    }

    // Calculate union
    const resultMembers = firstSet.sunion(otherSets)

    // Store result
    const resultSet = new SetDataType()
    for (const member of resultMembers) {
      resultSet.sadd(member)
    }
    db.set(destination, resultSet)
    transport.write(resultMembers.length)
  },
}

export default function (db: DB) {
  return createSchemaCommand(SunionstoreCommandDefinition, { db })
}
