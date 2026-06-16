import { defineCommand } from '../../core/command-definition'
import { t } from '../../core/command-schema'
import {
  PositiveCountError,
  WrongNumberOfArgumentsError,
} from '../../core/redis-error'
import { RedisValue } from '../../core/redis-value'
import { array, scoreBuffer } from '../helpers'
import { deleteSortedSetIfEmpty, getSortedMembers } from './helpers'

function parsePopCountArg(s: string): number {
  if (!/^-?\d+$/.test(s)) {
    throw new PositiveCountError()
  }

  const count = Number(s)
  if (!Number.isSafeInteger(count) || count < 0) {
    throw new PositiveCountError()
  }

  return count
}

function zpopCountSchema() {
  return t.custom<number>((input, index, ctx) => {
    const token = input[index]
    if (!token) {
      throw new WrongNumberOfArgumentsError(ctx.commandName)
    }

    return {
      value: parsePopCountArg(token.toString()),
      nextIndex: index + 1,
    }
  })
}

export const zpopminCommand = defineCommand({
  name: 'zpopmin',
  schema: t.object({ key: t.key(), count: t.optional(zpopCountSchema()) }),
  flags: ['write', 'fast'],
  keys: args => [args.key],
  execute: (args, ctx) => {
    const zset = ctx.db.getSortedSet(args.key)
    if (!zset) return array([])
    const count = args.count ?? 1
    const sorted = getSortedMembers(zset)
    const toRemove = sorted.slice(0, count)
    if (toRemove.length === 0) return array([])
    ctx.db.updateSortedSet(args.key, z => {
      for (const entry of toRemove) {
        z.deleteMember(entry.member)
      }
    })
    deleteSortedSetIfEmpty(ctx.db, args.key)
    const items: RedisValue[] = []
    for (const entry of toRemove) {
      items.push(RedisValue.bulkString(entry.member))
      items.push(RedisValue.bulkString(scoreBuffer(entry.score)))
    }
    return array(items)
  },
})

export const zpopmaxCommand = defineCommand({
  name: 'zpopmax',
  schema: t.object({ key: t.key(), count: t.optional(zpopCountSchema()) }),
  flags: ['write', 'fast'],
  keys: args => [args.key],
  execute: (args, ctx) => {
    const zset = ctx.db.getSortedSet(args.key)
    if (!zset) return array([])
    const count = args.count ?? 1
    const sorted = getSortedMembers(zset).slice().reverse()
    const toRemove = sorted.slice(0, count)
    if (toRemove.length === 0) return array([])
    ctx.db.updateSortedSet(args.key, z => {
      for (const entry of toRemove) {
        z.deleteMember(entry.member)
      }
    })
    deleteSortedSetIfEmpty(ctx.db, args.key)
    const items: RedisValue[] = []
    for (const entry of toRemove) {
      items.push(RedisValue.bulkString(entry.member))
      items.push(RedisValue.bulkString(scoreBuffer(entry.score)))
    }
    return array(items)
  },
})
