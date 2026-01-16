import { ExpectedFloat, WrongType } from '../../../../../../core/errors'
import { SortedSetDataType } from '../../../../data-structures/zset'
import { DB } from '../../../../db'
import { defineCommand, CommandCategory } from '../../../metadata'
import {
  createSchemaCommand,
  SchemaCommandRegistration,
  t,
} from '../../../../schema'

const metadata = defineCommand('zincrby', {
  arity: 4, // ZINCRBY key increment member
  flags: {
    write: true,
    fast: true,
  },
  firstKey: 0,
  lastKey: 0,
  keyStep: 1,
  categories: [CommandCategory.ZSET],
})

export const ZincrbyCommandDefinition: SchemaCommandRegistration<
  [Buffer, string, Buffer]
> = {
  metadata,
  schema: t.tuple([t.key(), t.string(), t.string()]),
  handler: ([key, incrementStr, member], { db }) => {
    const increment = parseFloat(incrementStr)
    if (Number.isNaN(increment)) {
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

    const newScore = zset.zincrby(member, increment)
    return Buffer.from(newScore.toString())
  },
}

export default function (db: DB) {
  return createSchemaCommand(ZincrbyCommandDefinition, { db })
}
