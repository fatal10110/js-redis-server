import { defineCommand } from '../core/command-definition'
import { t } from '../core/command-schema'
import { integer, bulk, ok, array } from './helpers'
import { RedisValue } from '../core/redis-value'
import { RedisResult } from '../core/redis-result'
import {
  IndexOutOfRangeError,
  NoSuchKeyError,
  RedisSyntaxError,
  WrongNumberOfArgumentsError,
} from '../core/redis-error'
import type { RedisExecutionContext } from '../core/redis-context'
import type { RedisDatabase } from '../state'

function resolveIndex(index: number, len: number): number {
  return index < 0 ? len + index : index
}

function listRemove(values: Buffer[], count: number, element: Buffer): number {
  const target = element.toString('binary')
  let removed = 0

  if (count === 0) {
    for (let i = values.length - 1; i >= 0; i--) {
      if (values[i].toString('binary') === target) {
        values.splice(i, 1)
        removed++
      }
    }
  } else if (count > 0) {
    for (let i = 0; i < values.length && removed < count; i++) {
      if (values[i].toString('binary') === target) {
        values.splice(i, 1)
        removed++
        i--
      }
    }
  } else {
    const absCount = Math.abs(count)
    for (let i = values.length - 1; i >= 0 && removed < absCount; i--) {
      if (values[i].toString('binary') === target) {
        values.splice(i, 1)
        removed++
      }
    }
  }
  return removed
}

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

export const lpopCommand = defineCommand({
  name: 'lpop',
  schema: t.object({
    key: t.key(),
  }),
  flags: ['write', 'fast'],
  keys: args => [args.key],
  execute: (args, ctx) => {
    const list = ctx.db.getList(args.key)
    if (!list || list.values.length === 0) return bulk(null)

    const result = ctx.db.updateList(args.key, list => {
      const value = list.values.shift() ?? null
      return { value, empty: list.values.length === 0 }
    })
    if (result.empty) ctx.db.delete(args.key)
    return bulk(result.value)
  },
})

export const rpopCommand = defineCommand({
  name: 'rpop',
  schema: t.object({
    key: t.key(),
  }),
  flags: ['write', 'fast'],
  keys: args => [args.key],
  execute: (args, ctx) => {
    const list = ctx.db.getList(args.key)
    if (!list || list.values.length === 0) return bulk(null)

    const result = ctx.db.updateList(args.key, list => {
      const value = list.values.pop() ?? null
      return { value, empty: list.values.length === 0 }
    })
    if (result.empty) ctx.db.delete(args.key)
    return bulk(result.value)
  },
})

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

export const rpoplpushCommand = defineCommand({
  name: 'rpoplpush',
  schema: t.object({
    source: t.key(),
    destination: t.key(),
  }),
  flags: ['write', 'denyoom'],
  keys: args => [args.source, args.destination],
  execute: (args, ctx) => {
    const sourceList = ctx.db.getList(args.source)
    if (!sourceList || sourceList.values.length === 0) return bulk(null)

    // Validate destination type before mutating
    ctx.db.getList(args.destination)

    const value = ctx.db.updateList(args.source, list => {
      const val = list.values.pop() ?? null
      return { val, empty: list.values.length === 0 }
    })
    if (value.empty) ctx.db.delete(args.source)
    if (value.val === null) return bulk(null)

    ctx.db.updateList(args.destination, list => {
      list.values.unshift(value.val!)
    })

    return bulk(value.val)
  },
})

function tryListPop(
  keys: readonly Buffer[],
  side: 'left' | 'right',
  db: RedisDatabase,
): RedisResult | null {
  for (const key of keys) {
    const list = db.getList(key)
    if (!list || list.values.length === 0) continue

    const result = db.updateList(key, list => {
      const value = side === 'left' ? list.values.shift()! : list.values.pop()!
      return { value, empty: list.values.length === 0 }
    })
    if (result.empty) db.delete(key)
    return RedisResult.create(
      RedisValue.array([
        RedisValue.bulkString(key),
        RedisValue.bulkString(result.value),
      ]),
    )
  }
  return null
}

async function blockingListPop(
  keys: readonly Buffer[],
  timeoutSecs: number,
  side: 'left' | 'right',
  ctx: RedisExecutionContext,
): Promise<RedisResult> {
  const timeoutMs =
    timeoutSecs === 0 ? undefined : Math.ceil(timeoutSecs * 1000)
  const deadline = timeoutMs !== undefined ? Date.now() + timeoutMs : undefined

  while (true) {
    const remaining =
      deadline !== undefined ? Math.max(0, deadline - Date.now()) : undefined
    if (remaining === 0) return RedisResult.create(RedisValue.nullArray())

    let wake!: (v: true) => void
    const waitFor = new Promise<true>(resolve => {
      wake = () => resolve(true)
    })

    const unsubs = keys.map(key =>
      ctx.db.subscribeKey(key, event => {
        if (event.type === 'write') wake(true)
      }),
    )

    const woken = await ctx.park({
      waitFor,
      timeoutMs: remaining,
      signal: ctx.signal,
    })
    for (const unsub of unsubs) unsub()

    if (woken === null) return RedisResult.create(RedisValue.nullArray())

    const result = tryListPop(keys, side, ctx.db)
    if (result) return result
  }
}

export const blpopCommand = defineCommand({
  name: 'blpop',
  schema: t.custom<{ keys: Buffer[]; timeout: number }>((input, index, ctx) => {
    if (input.length - index < 2)
      throw new WrongNumberOfArgumentsError(ctx.commandName)
    const timeout = Number(input[input.length - 1].toString())
    if (isNaN(timeout) || timeout < 0) throw new RedisSyntaxError()
    const keys = Array.from(input.slice(index, input.length - 1))
    return { value: { keys, timeout }, nextIndex: input.length }
  }),
  flags: ['write', 'noscript'],
  keys: args => args.keys,
  execute: (args, ctx) => {
    const immediate = tryListPop(args.keys, 'left', ctx.db)
    if (immediate) return immediate
    return blockingListPop(args.keys, args.timeout, 'left', ctx)
  },
})

export const brpopCommand = defineCommand({
  name: 'brpop',
  schema: t.custom<{ keys: Buffer[]; timeout: number }>((input, index, ctx) => {
    if (input.length - index < 2)
      throw new WrongNumberOfArgumentsError(ctx.commandName)
    const timeout = Number(input[input.length - 1].toString())
    if (isNaN(timeout) || timeout < 0) throw new RedisSyntaxError()
    const keys = Array.from(input.slice(index, input.length - 1))
    return { value: { keys, timeout }, nextIndex: input.length }
  }),
  flags: ['write', 'noscript'],
  keys: args => args.keys,
  execute: (args, ctx) => {
    const immediate = tryListPop(args.keys, 'right', ctx.db)
    if (immediate) return immediate
    return blockingListPop(args.keys, args.timeout, 'right', ctx)
  },
})

export const listsCommands = [
  lpushCommand,
  rpushCommand,
  lpopCommand,
  rpopCommand,
  llenCommand,
  lrangeCommand,
  lindexCommand,
  lsetCommand,
  lremCommand,
  ltrimCommand,
  lpushxCommand,
  rpushxCommand,
  rpoplpushCommand,
  blpopCommand,
  brpopCommand,
]
