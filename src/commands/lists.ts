import { defineCommand } from '../core/command-definition'
import { t } from '../core/command-schema'
import { integer, bulk, ok, array, parseIntegerToken } from './helpers'
import { RedisValue } from '../core/redis-value'
import { RedisResult } from '../core/redis-result'
import {
  IndexOutOfRangeError,
  LposCountNegativeError,
  LposMaxlenNegativeError,
  LposRankZeroError,
  NoSuchKeyError,
  PositiveCountError,
  RedisSyntaxError,
  TimeoutNegativeError,
  TimeoutNotFloatError,
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

function popList(
  args: { key: Buffer; count?: number },
  ctx: RedisExecutionContext,
  side: 'left' | 'right',
): RedisResult {
  const count = args.count
  if (count !== undefined && count < 0) {
    throw new PositiveCountError()
  }

  const list = ctx.db.getList(args.key)
  if (!list || list.values.length === 0) {
    return count === undefined
      ? bulk(null)
      : RedisResult.create(RedisValue.nullArray())
  }

  if (count === undefined) {
    const result = ctx.db.updateList(args.key, list => {
      const value = side === 'left' ? list.values.shift() : list.values.pop()
      return { value: value ?? null, empty: list.values.length === 0 }
    })
    if (result.empty) ctx.db.delete(args.key)
    return bulk(result.value)
  }

  if (count === 0) {
    return array([])
  }

  const result = ctx.db.updateList(args.key, list => {
    const values =
      side === 'left'
        ? list.values.splice(0, count)
        : list.values.splice(Math.max(0, list.values.length - count))
    return { values, empty: list.values.length === 0 }
  })
  if (result.empty) ctx.db.delete(args.key)

  return array(result.values.map(value => RedisValue.bulkString(value)))
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
    count: t.optional(t.integer()),
  }),
  flags: ['write', 'fast'],
  keys: args => [args.key],
  execute: (args, ctx) => popList(args, ctx, 'left'),
})

export const rpopCommand = defineCommand({
  name: 'rpop',
  schema: t.object({
    key: t.key(),
    count: t.optional(t.integer()),
  }),
  flags: ['write', 'fast'],
  keys: args => [args.key],
  execute: (args, ctx) => popList(args, ctx, 'right'),
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

type LposArgs = {
  key: Buffer
  element: Buffer
  rank: number
  count?: number
  maxlen: number
}

export const lposCommand = defineCommand({
  name: 'lpos',
  schema: t.custom<LposArgs>((input, index, ctx) => {
    const key = input[index]
    const element = input[index + 1]
    if (!key || !element) {
      throw new WrongNumberOfArgumentsError(ctx.commandName)
    }

    let rank = 1
    let count: number | undefined
    let maxlen = 0
    let cursor = index + 2

    while (cursor < input.length) {
      const option = input[cursor].toString().toUpperCase()
      const valueToken = input[cursor + 1]
      if (!valueToken) {
        throw new RedisSyntaxError()
      }

      if (option === 'RANK') {
        rank = parseIntegerToken(valueToken)
        if (rank === 0) throw new LposRankZeroError()
      } else if (option === 'COUNT') {
        count = parseIntegerToken(valueToken)
        if (count < 0) throw new LposCountNegativeError()
      } else if (option === 'MAXLEN') {
        maxlen = parseIntegerToken(valueToken)
        if (maxlen < 0) throw new LposMaxlenNegativeError()
      } else {
        throw new RedisSyntaxError()
      }

      cursor += 2
    }

    return { value: { key, element, rank, count, maxlen }, nextIndex: cursor }
  }),
  flags: ['readonly'],
  keys: args => [args.key],
  execute: (args, ctx) => {
    const hasCount = args.count !== undefined
    const list = ctx.db.getList(args.key)
    if (!list || list.values.length === 0) {
      return hasCount ? array([]) : bulk(null)
    }

    const target = args.element.toString('binary')
    const forward = args.rank > 0
    const step = forward ? 1 : -1
    const len = list.values.length
    // count===0 means "return all matches"; absent means "return first only"
    const limit = hasCount ? (args.count === 0 ? Infinity : args.count!) : 1

    const results: number[] = []
    let toSkip = Math.abs(args.rank) - 1
    let comparisons = 0

    for (let i = forward ? 0 : len - 1; i >= 0 && i < len; i += step) {
      if (args.maxlen !== 0 && comparisons >= args.maxlen) break
      comparisons++
      if (list.values[i].toString('binary') !== target) continue

      if (toSkip > 0) {
        toSkip--
        continue
      }
      results.push(i)
      if (results.length >= limit) break
    }

    if (!hasCount) {
      return results.length === 0 ? bulk(null) : integer(results[0])
    }
    return array(results.map(idx => RedisValue.integer(idx)))
  },
})

function moveDirection(): ReturnType<typeof t.custom<'left' | 'right'>> {
  return t.custom<'left' | 'right'>((input, index, ctx) => {
    const token = input[index]
    if (!token) {
      throw new WrongNumberOfArgumentsError(ctx.commandName)
    }

    const direction = token.toString().toUpperCase()
    if (direction !== 'LEFT' && direction !== 'RIGHT') {
      throw new RedisSyntaxError()
    }

    return {
      value: direction === 'LEFT' ? 'left' : 'right',
      nextIndex: index + 1,
    }
  })
}

// Non-blocking LMOVE core. Returns a bulk-string result on success, or `null`
// when the source is empty/missing (the caller decides whether to block).
function tryListMove(
  source: Buffer,
  destination: Buffer,
  fromDirection: 'left' | 'right',
  toDirection: 'left' | 'right',
  db: RedisDatabase,
): RedisResult | null {
  const sourceList = db.getList(source)
  if (!sourceList || sourceList.values.length === 0) return null

  // Validate destination type before mutating the source
  db.getList(destination)

  const popped = db.updateList(source, list => {
    const value =
      (fromDirection === 'left' ? list.values.shift() : list.values.pop()) ??
      null
    return { value, empty: list.values.length === 0 }
  })
  if (popped.empty) db.delete(source)
  if (popped.value === null) return null

  db.updateList(destination, list => {
    if (toDirection === 'left') list.values.unshift(popped.value!)
    else list.values.push(popped.value!)
  })

  return bulk(popped.value)
}

export const lmoveCommand = defineCommand({
  name: 'lmove',
  schema: t.object({
    source: t.key(),
    destination: t.key(),
    fromDirection: moveDirection(),
    toDirection: moveDirection(),
  }),
  flags: ['write', 'denyoom'],
  keys: args => [args.source, args.destination],
  execute: (args, ctx) =>
    tryListMove(
      args.source,
      args.destination,
      args.fromDirection,
      args.toDirection,
      ctx.db,
    ) ?? bulk(null),
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

    let woken: boolean | null
    try {
      woken = await ctx.park({
        waitFor,
        timeoutMs: remaining,
        signal: ctx.signal,
      })
    } finally {
      for (const unsub of unsubs) {
        try {
          unsub()
        } catch {
          // ignore errors from individual unsubscribers so all are attempted
        }
      }
    }

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

function parseMoveDirection(token: Buffer | undefined): 'left' | 'right' {
  if (!token) throw new RedisSyntaxError()
  const direction = token.toString().toUpperCase()
  if (direction === 'LEFT') return 'left'
  if (direction === 'RIGHT') return 'right'
  throw new RedisSyntaxError()
}

function parseTimeout(token: Buffer): number {
  const value = Number(token.toString())
  if (isNaN(value)) throw new TimeoutNotFloatError()
  if (value < 0) throw new TimeoutNegativeError()
  return value
}

async function blockingListMove(
  source: Buffer,
  destination: Buffer,
  fromDirection: 'left' | 'right',
  toDirection: 'left' | 'right',
  timeoutSecs: number,
  ctx: RedisExecutionContext,
): Promise<RedisResult> {
  const timeoutMs =
    timeoutSecs === 0 ? undefined : Math.ceil(timeoutSecs * 1000)
  const deadline = timeoutMs !== undefined ? Date.now() + timeoutMs : undefined

  while (true) {
    const remaining =
      deadline !== undefined ? Math.max(0, deadline - Date.now()) : undefined
    // BLMOVE/BRPOPLPUSH reply with a null array (`*-1`) on timeout, unlike the
    // bulk-string reply on success.
    if (remaining === 0) return RedisResult.create(RedisValue.nullArray())

    let wake!: (v: true) => void
    const waitFor = new Promise<true>(resolve => {
      wake = () => resolve(true)
    })

    const unsub = ctx.db.subscribeKey(source, event => {
      if (event.type === 'write') wake(true)
    })

    let woken: boolean | null
    try {
      woken = await ctx.park({
        waitFor,
        timeoutMs: remaining,
        signal: ctx.signal,
      })
    } finally {
      try {
        unsub()
      } catch {
        // ignore unsubscribe errors so cleanup always completes
      }
    }

    if (woken === null) return RedisResult.create(RedisValue.nullArray())

    const result = tryListMove(
      source,
      destination,
      fromDirection,
      toDirection,
      ctx.db,
    )
    if (result) return result
  }
}

type BlmoveArgs = {
  source: Buffer
  destination: Buffer
  fromDirection: 'left' | 'right'
  toDirection: 'left' | 'right'
  timeout: number
}

export const blmoveCommand = defineCommand({
  name: 'blmove',
  schema: t.custom<BlmoveArgs>((input, index, ctx) => {
    if (input.length - index !== 5) {
      throw new WrongNumberOfArgumentsError(ctx.commandName)
    }
    const source = input[index]
    const destination = input[index + 1]
    const fromDirection = parseMoveDirection(input[index + 2])
    const toDirection = parseMoveDirection(input[index + 3])
    const timeout = parseTimeout(input[index + 4])
    return {
      value: { source, destination, fromDirection, toDirection, timeout },
      nextIndex: input.length,
    }
  }),
  flags: ['write', 'noscript'],
  keys: args => [args.source, args.destination],
  execute: (args, ctx) => {
    const immediate = tryListMove(
      args.source,
      args.destination,
      args.fromDirection,
      args.toDirection,
      ctx.db,
    )
    if (immediate) return immediate
    return blockingListMove(
      args.source,
      args.destination,
      args.fromDirection,
      args.toDirection,
      args.timeout,
      ctx,
    )
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
  lposCommand,
  lmoveCommand,
  blmoveCommand,
  blpopCommand,
  brpopCommand,
]
