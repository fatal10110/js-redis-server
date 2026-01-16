import { ExpectedFloat, WrongType } from '../../../../../../core/errors'
import { SortedSetDataType } from '../../../../data-structures/zset'
import { DB } from '../../../../db'
import { defineCommand, CommandCategory } from '../../../metadata'
import {
  createSchemaCommand,
  SchemaCommandRegistration,
  t,
} from '../../../../schema'
const metadata = defineCommand('zadd', {
  arity: -4, // ZADD key score member [score member ...]
  flags: {
    write: true,
    denyoom: true,
    fast: true,
  },
  firstKey: 0,
  lastKey: 0,
  keyStep: 1,
  categories: [CommandCategory.ZSET],
})
export const ZaddCommandDefinition: SchemaCommandRegistration<
  [Buffer, string, Buffer, Array<[string, Buffer]>]
> = {
  metadata,
  schema: t.tuple([
    t.key(),
    t.string(),
    t.string(),
    t.variadic(t.tuple([t.string(), t.string()])),
  ]),
  handler: (
    [key, firstScoreStr, firstMember, restPairs],
    { db, transport },
  ) => {
    const firstScore = parseFloat(firstScoreStr)
    if (Number.isNaN(firstScore)) {
      throw new ExpectedFloat()
    }
    const existing = db.get(key)
    if (existing !== null && !(existing instanceof SortedSetDataType)) {
      throw new WrongType()
    }
    const zset =
      existing instanceof SortedSetDataType ? existing : new SortedSetDataType()
    if (!(existing instanceof SortedSetDataType)) {
      db.set(key, zset)
    }
    let addedCount = 0
    addedCount += zset.zadd(firstScore, firstMember)
    for (const [scoreStr, member] of restPairs) {
      const score = parseFloat(scoreStr)
      if (Number.isNaN(score)) {
        throw new ExpectedFloat()
      }
      addedCount += zset.zadd(score, member)
    }
    transport.write(addedCount)
  },
}
export default function (db: DB) {
  return createSchemaCommand(ZaddCommandDefinition, { db })
}
