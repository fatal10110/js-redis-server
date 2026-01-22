import { WrongType } from '../../../../../../core/errors'
import { SetDataType } from '../../../../data-structures/set'
import { defineCommand, CommandCategory } from '../../../metadata'
import {
  SchemaCommand,
  CommandContext,
} from '../../../../schema/schema-command'
import { t } from '../../../../schema'

export class SinterstoreCommand extends SchemaCommand<
  [Buffer, Buffer, Buffer[]]
> {
  metadata = defineCommand('sinterstore', {
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

  protected schema = t.tuple([t.key(), t.key(), t.variadic(t.key())])

  protected execute(
    [destination, firstKey, otherKeys]: [Buffer, Buffer, Buffer[]],
    { db, transport }: CommandContext,
  ) {
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
  }
}
