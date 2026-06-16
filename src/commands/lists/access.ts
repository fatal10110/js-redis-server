import { defineCommand } from '../../core/command-definition'
import { t } from '../../core/command-schema'
import { IndexOutOfRangeError, NoSuchKeyError } from '../../core/redis-error'
import { RedisValue } from '../../core/redis-value'
import { array, bulk, integer, ok } from '../helpers'
import { listRemove, resolveIndex } from './helpers'

export const llenCommand = defineCommand({
  name: 'llen',
  schema: t.object({
    key: t.key(),
  }),
  flags: ['readonly', 'fast'],
  keys: args => [args.key],
  execute: (args, ctx) => {
    const list = ctx.db.getList(args.key)
    return integer(list ? list.values.length : 0)
  },
})

export const lrangeCommand = defineCommand({
  name: 'lrange',
  schema: t.object({
    key: t.key(),
    start: t.integer(),
    stop: t.integer(),
  }),
  flags: ['readonly'],
  keys: args => [args.key],
  execute: (args, ctx) => {
    const list = ctx.db.getList(args.key)
    if (!list || list.values.length === 0) return array([])

    let start = resolveIndex(args.start, list.values.length)
    let stop = resolveIndex(args.stop, list.values.length)
    start = Math.max(0, start)
    stop = Math.min(list.values.length - 1, stop)
    if (start > stop) return array([])

    const slice = list.values.slice(start, stop + 1)
    return array(slice.map(v => RedisValue.bulkString(v)))
  },
})

export const lindexCommand = defineCommand({
  name: 'lindex',
  schema: t.object({
    key: t.key(),
    index: t.integer(),
  }),
  flags: ['readonly'],
  keys: args => [args.key],
  execute: (args, ctx) => {
    const list = ctx.db.getList(args.key)
    if (!list) return bulk(null)

    const idx = resolveIndex(args.index, list.values.length)
    if (idx < 0 || idx >= list.values.length) return bulk(null)
    return bulk(list.values[idx])
  },
})

export const lsetCommand = defineCommand({
  name: 'lset',
  schema: t.object({
    key: t.key(),
    index: t.integer(),
    value: t.key(),
  }),
  flags: ['write'],
  keys: args => [args.key],
  execute: (args, ctx) => {
    const list = ctx.db.getList(args.key)
    if (!list) throw new NoSuchKeyError()

    const idx = resolveIndex(args.index, list.values.length)
    if (idx < 0 || idx >= list.values.length) throw new IndexOutOfRangeError()

    ctx.db.updateList(args.key, list => {
      list.values[idx] = args.value
    })
    return ok()
  },
})

export const lremCommand = defineCommand({
  name: 'lrem',
  schema: t.object({
    key: t.key(),
    count: t.integer(),
    element: t.key(),
  }),
  flags: ['write'],
  keys: args => [args.key],
  execute: (args, ctx) => {
    const list = ctx.db.getList(args.key)
    if (!list) return integer(0)

    const result = ctx.db.updateList(args.key, list => {
      const removed = listRemove(list.values, args.count, args.element)
      return { removed, empty: list.values.length === 0 }
    })
    if (result.empty) ctx.db.delete(args.key)
    return integer(result.removed)
  },
})

export const ltrimCommand = defineCommand({
  name: 'ltrim',
  schema: t.object({
    key: t.key(),
    start: t.integer(),
    stop: t.integer(),
  }),
  flags: ['write'],
  keys: args => [args.key],
  execute: (args, ctx) => {
    const list = ctx.db.getList(args.key)
    if (!list) return ok()

    const result = ctx.db.updateList(args.key, list => {
      let start = resolveIndex(args.start, list.values.length)
      let stop = resolveIndex(args.stop, list.values.length)
      start = Math.max(0, start)
      stop = Math.min(list.values.length - 1, stop)
      if (start > stop) {
        list.values.length = 0
      } else {
        list.values.splice(0, start)
        list.values.splice(stop - start + 1)
      }
      return { empty: list.values.length === 0 }
    })
    if (result.empty) ctx.db.delete(args.key)
    return ok()
  },
})
