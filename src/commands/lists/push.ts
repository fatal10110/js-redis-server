import { defineCommand } from '../../core/command-definition'
import { t } from '../../core/command-schema'
import { integer } from '../helpers'

export const lpushCommand = defineCommand({
  name: 'lpush',
  schema: t.object({
    key: t.key(),
    values: t.variadic(t.key(), { min: 1 }),
  }),
  flags: ['write', 'denyoom', 'fast'],
  keys: args => [args.key],
  execute: (args, ctx) => {
    const len = ctx.db.updateList(args.key, list => {
      for (const val of args.values) {
        list.values.unshift(val)
      }
      return list.values.length
    })
    return integer(len)
  },
})

export const rpushCommand = defineCommand({
  name: 'rpush',
  schema: t.object({
    key: t.key(),
    values: t.variadic(t.key(), { min: 1 }),
  }),
  flags: ['write', 'denyoom', 'fast'],
  keys: args => [args.key],
  execute: (args, ctx) => {
    const len = ctx.db.updateList(args.key, list => {
      for (const val of args.values) {
        list.values.push(val)
      }
      return list.values.length
    })
    return integer(len)
  },
})

export const lpushxCommand = defineCommand({
  name: 'lpushx',
  schema: t.object({
    key: t.key(),
    values: t.variadic(t.key(), { min: 1 }),
  }),
  flags: ['write', 'denyoom', 'fast'],
  keys: args => [args.key],
  execute: (args, ctx) => {
    const list = ctx.db.getList(args.key)
    if (!list) return integer(0)

    const len = ctx.db.updateList(args.key, list => {
      for (const val of args.values) {
        list.values.unshift(val)
      }
      return list.values.length
    })
    return integer(len)
  },
})

export const rpushxCommand = defineCommand({
  name: 'rpushx',
  schema: t.object({
    key: t.key(),
    values: t.variadic(t.key(), { min: 1 }),
  }),
  flags: ['write', 'denyoom', 'fast'],
  keys: args => [args.key],
  execute: (args, ctx) => {
    const list = ctx.db.getList(args.key)
    if (!list) return integer(0)

    const len = ctx.db.updateList(args.key, list => {
      for (const val of args.values) {
        list.values.push(val)
      }
      return list.values.length
    })
    return integer(len)
  },
})
