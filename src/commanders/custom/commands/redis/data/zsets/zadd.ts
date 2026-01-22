import { ExpectedFloat, WrongType } from '../../../../../../core/errors'
import { SortedSetDataType } from '../../../../data-structures/zset'
import { defineCommand, CommandCategory } from '../../../metadata'
import {
  SchemaCommand,
  CommandContext,
} from '../../../../schema/schema-command'
import { t } from '../../../../schema'

export class ZaddCommand extends SchemaCommand<
  [Buffer, string, Buffer, Array<[string, Buffer]>]
> {
  metadata = defineCommand('zadd', {
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

  protected schema = t.tuple([
    t.key(),
    t.string(),
    t.string(),
    t.variadic(t.tuple([t.string(), t.string()])),
  ])

  protected execute(
    [key, firstScoreStr, firstMember, restPairs]: [
      Buffer,
      string,
      Buffer,
      Array<[string, Buffer]>,
    ],
    { db, transport }: CommandContext,
  ) {
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
  }
}
