import { defineCommand } from '../core/command-definition'
import {
  t,
  type CommandSchema,
  type ParseContext,
} from '../core/command-schema'
import type { RedisDatabase } from '../state'
import {
  ExpectedFloatError,
  IncrDecrOverflowError,
  InvalidExpireTimeError,
  OffsetOutOfRangeError,
  RedisSyntaxError,
  WrongTypeRedisError,
  WrongNumberOfArgumentsError,
} from '../core/redis-error'
import { RedisResult } from '../core/redis-result'
import { RedisValue } from '../core/redis-value'
import {
  bulk,
  ensureStringOrMissing,
  INT64_MAX,
  INT64_MIN,
  integer,
  ok,
  parseInt64Token,
  parseIntegerToken,
  parsePositiveExpireToken,
  requireNextOptionValue,
} from './helpers'
import {
  commandDocs,
  commandKeyArgument,
  commandKeySpec,
} from './introspection'

type SetCondition = 'NX' | 'XX'

type SetArgs = {
  key: Buffer
  value: Buffer
  condition?: SetCondition
  expiresAt?: number
  keepTtl?: boolean
  get?: boolean
}

export const getCommand = defineCommand({
  name: 'get',
  schema: t.object({
    key: t.key(),
  }),
  flags: ['readonly', 'fast'],
  introspection: {
    arity: 2,
    flags: ['readonly', 'fast'],
    firstKey: 1,
    lastKey: 1,
    keyStep: 1,
    categories: ['@read', '@string', '@fast'],
    keySpecs: [commandKeySpec(1, 0, 1, ['RO', 'access'])],
    docs: commandDocs('Get the value of a key', 'string', [
      commandKeyArgument('key', 0),
    ]),
  },
  keys: args => [args.key],
  execute: (args, ctx) => bulk(ensureStringOrMissing(ctx.db, args.key)),
})

export const setCommand = defineCommand({
  name: 'set',
  schema: createSetSchema(),
  flags: ['write', 'denyoom'],
  introspection: {
    arity: -3,
    flags: ['write', 'denyoom'],
    firstKey: 1,
    lastKey: 1,
    keyStep: 1,
    categories: ['@write', '@string', '@slow'],
    keySpecs: [
      commandKeySpec(1, 0, 1, ['RW', 'access', 'update', 'variable_flags'], {
        notes: 'RW and ACCESS due to the optional `GET` argument',
      }),
    ],
    docs: commandDocs('Set the string value of a key', 'string', [
      commandKeyArgument('key', 0),
      { name: 'value', type: 'string' },
    ]),
  },
  keys: args => [args.key],
  execute: (args, ctx) => {
    const existingType = ctx.db.getType(args.key)
    let oldValue: Buffer | null = null

    if (args.get) {
      if (existingType === 'string') {
        oldValue = ctx.db.getString(args.key)
      } else if (existingType !== null) {
        throw new WrongTypeRedisError()
      }
    }

    if (args.condition === 'NX' && existingType !== null) {
      return args.get ? bulk(oldValue) : RedisResult.nil()
    }

    if (args.condition === 'XX' && existingType === null) {
      return args.get ? bulk(null) : RedisResult.nil()
    }

    ctx.db.setString(args.key, args.value, {
      expiresAt: args.expiresAt,
      keepTtl: args.keepTtl,
    })

    if (args.get) {
      return bulk(oldValue)
    }

    return ok()
  },
})

export const mgetCommand = defineCommand({
  name: 'mget',
  schema: t.object({
    keys: t.variadic(t.key(), { min: 1 }),
  }),
  flags: ['readonly'],
  introspection: {
    arity: -2,
    flags: ['readonly', 'fast'],
    firstKey: 1,
    lastKey: -1,
    keyStep: 1,
    categories: ['@read', '@string', '@fast'],
    tips: ['request_policy:multi_shard'],
    keySpecs: [commandKeySpec(1, -1, 1, ['RO', 'access'])],
    docs: commandDocs('Get the values of all the given keys', 'string', [
      commandKeyArgument('key', 0, { flags: ['multiple'] }),
    ]),
  },
  keys: args => args.keys,
  execute: (args, ctx) =>
    RedisResult.create(
      RedisValue.array(
        args.keys.map(key => {
          const value =
            ctx.db.getType(key) === 'string' ? ctx.db.getString(key) : null
          return RedisValue.bulkString(value)
        }),
      ),
    ),
})

export const appendCommand = defineCommand({
  name: 'append',
  schema: t.object({ key: t.key(), value: t.key() }),
  flags: ['write', 'denyoom', 'fast'],
  keys: args => [args.key],
  execute: (args, ctx) => {
    const existing = ensureStringOrMissing(ctx.db, args.key)
    const next = existing ? Buffer.concat([existing, args.value]) : args.value
    ctx.db.setString(args.key, next, { keepTtl: true })
    return integer(next.length)
  },
})

export const strlenCommand = defineCommand({
  name: 'strlen',
  schema: t.object({ key: t.key() }),
  flags: ['readonly', 'fast'],
  keys: args => [args.key],
  execute: (args, ctx) => {
    const existing = ensureStringOrMissing(ctx.db, args.key)
    return integer(existing?.byteLength ?? 0)
  },
})

export const incrCommand = defineCommand({
  name: 'incr',
  schema: t.object({ key: t.key() }),
  flags: ['write', 'fast'],
  keys: args => [args.key],
  execute: (args, ctx) => incrementBy(ctx.db, args.key, 1n),
})

export const decrCommand = defineCommand({
  name: 'decr',
  schema: t.object({ key: t.key() }),
  flags: ['write', 'fast'],
  keys: args => [args.key],
  execute: (args, ctx) => incrementBy(ctx.db, args.key, -1n),
})

export const incrbyCommand = defineCommand({
  name: 'incrby',
  schema: t.object({
    key: t.key(),
    amount: t.bigInteger({ min: INT64_MIN, max: INT64_MAX }),
  }),
  flags: ['write', 'fast'],
  keys: args => [args.key],
  execute: (args, ctx) => incrementBy(ctx.db, args.key, args.amount),
})

export const decrbyCommand = defineCommand({
  name: 'decrby',
  schema: t.object({
    key: t.key(),
    amount: t.bigInteger({ min: INT64_MIN, max: INT64_MAX }),
  }),
  flags: ['write', 'fast'],
  keys: args => [args.key],
  execute: (args, ctx) => {
    // Negating INT64_MIN overflows int64, so Redis rejects it before the op.
    if (args.amount === INT64_MIN) {
      throw new IncrDecrOverflowError('decrement would overflow')
    }
    return incrementBy(ctx.db, args.key, -args.amount)
  },
})

export const incrbyfloatCommand = defineCommand({
  name: 'incrbyfloat',
  schema: t.object({ key: t.key(), amount: t.key() }),
  flags: ['write', 'fast'],
  keys: args => [args.key],
  execute: (args, ctx) => {
    const increment = parseFloat(args.amount.toString())
    if (!Number.isFinite(increment)) {
      throw new ExpectedFloatError()
    }

    const existing = ensureStringOrMissing(ctx.db, args.key)
    let current = 0
    if (existing) {
      const parsed = parseFloat(existing.toString())
      if (!Number.isFinite(parsed)) {
        throw new ExpectedFloatError()
      }
      current = parsed
    }

    const next = current + increment
    if (!Number.isFinite(next)) {
      throw new ExpectedFloatError()
    }

    const valueBuf = Buffer.from(next.toString())
    ctx.db.setString(args.key, valueBuf, { keepTtl: true })
    return bulk(valueBuf)
  },
})

export const getsetCommand = defineCommand({
  name: 'getset',
  schema: t.object({ key: t.key(), value: t.key() }),
  flags: ['write', 'denyoom', 'fast'],
  keys: args => [args.key],
  execute: (args, ctx) => {
    const existing = ensureStringOrMissing(ctx.db, args.key)
    ctx.db.setString(args.key, args.value)
    return bulk(existing)
  },
})

export const getdelCommand = defineCommand({
  name: 'getdel',
  schema: t.object({ key: t.key() }),
  flags: ['write', 'fast'],
  keys: args => [args.key],
  execute: (args, ctx) => {
    const existing = ensureStringOrMissing(ctx.db, args.key)
    if (existing === null) {
      return bulk(null)
    }
    ctx.db.delete(args.key)
    return bulk(existing)
  },
})

export const setnxCommand = defineCommand({
  name: 'setnx',
  schema: t.object({ key: t.key(), value: t.key() }),
  flags: ['write', 'denyoom', 'fast'],
  keys: args => [args.key],
  execute: (args, ctx) => {
    if (ctx.db.getType(args.key) !== null) {
      return integer(0)
    }
    ctx.db.setString(args.key, args.value)
    return integer(1)
  },
})

export const setexCommand = defineCommand({
  name: 'setex',
  schema: t.object({
    key: t.key(),
    seconds: t.integer(),
    value: t.key(),
  }),
  flags: ['write', 'denyoom'],
  keys: args => [args.key],
  execute: (args, ctx) => {
    if (args.seconds <= 0) {
      throw new InvalidExpireTimeError('setex')
    }
    ctx.db.setString(args.key, args.value, {
      expiresAt: Date.now() + args.seconds * 1000,
    })
    return ok()
  },
})

export const psetexCommand = defineCommand({
  name: 'psetex',
  schema: t.object({
    key: t.key(),
    milliseconds: t.integer(),
    value: t.key(),
  }),
  flags: ['write', 'denyoom'],
  keys: args => [args.key],
  execute: (args, ctx) => {
    if (args.milliseconds <= 0) {
      throw new InvalidExpireTimeError('psetex')
    }
    ctx.db.setString(args.key, args.value, {
      expiresAt: Date.now() + args.milliseconds,
    })
    return ok()
  },
})

export const msetCommand = defineCommand({
  name: 'mset',
  schema: createKeyValuePairsSchema(),
  flags: ['write', 'denyoom'],
  keys: args => args.map(pair => pair.key),
  execute: (args, ctx) => {
    for (const { key, value } of args) {
      ctx.db.setString(key, value)
    }
    return ok()
  },
})

export const msetnxCommand = defineCommand({
  name: 'msetnx',
  schema: createKeyValuePairsSchema(),
  flags: ['write', 'denyoom'],
  keys: args => args.map(pair => pair.key),
  execute: (args, ctx) => {
    for (const { key } of args) {
      if (ctx.db.getType(key) !== null) {
        return integer(0)
      }
    }
    for (const { key, value } of args) {
      ctx.db.setString(key, value)
    }
    return integer(1)
  },
})

export const getrangeCommand = defineCommand({
  name: 'getrange',
  schema: t.object({
    key: t.key(),
    start: t.integer(),
    end: t.integer(),
  }),
  flags: ['readonly'],
  keys: args => [args.key],
  execute: (args, ctx) => {
    const existing = ensureStringOrMissing(ctx.db, args.key)
    if (!existing) {
      return bulk(Buffer.alloc(0))
    }

    const length = existing.length
    let startIdx = args.start < 0 ? length + args.start : args.start
    let endIdx = args.end < 0 ? length + args.end : args.end

    if (startIdx < 0) startIdx = 0
    if (endIdx >= length) endIdx = length - 1

    if (startIdx > endIdx || startIdx >= length) {
      return bulk(Buffer.alloc(0))
    }

    return bulk(existing.slice(startIdx, endIdx + 1))
  },
})

export const substrCommand = defineCommand({
  ...getrangeCommand,
  name: 'substr',
})

export const setrangeCommand = defineCommand({
  name: 'setrange',
  schema: t.object({
    key: t.key(),
    offset: createSetrangeOffsetSchema(),
    value: t.key(),
  }),
  flags: ['write', 'denyoom'],
  keys: args => [args.key],
  execute: (args, ctx) => {
    const existing = ensureStringOrMissing(ctx.db, args.key)

    if (args.value.length === 0) {
      return integer(existing?.length ?? 0)
    }

    const current = existing ?? Buffer.alloc(0)
    const requiredSize = args.offset + args.value.length
    const target =
      requiredSize > current.length ? Buffer.alloc(requiredSize) : current

    if (target !== current) {
      current.copy(target, 0)
    }

    args.value.copy(target, args.offset)
    ctx.db.setString(args.key, target, { keepTtl: true })
    return integer(target.length)
  },
})

export const getexCommand = defineCommand({
  name: 'getex',
  schema: createGetexSchema(),
  flags: ['write', 'fast'],
  keys: args => [args.key],
  execute: (args, ctx) => {
    const existing = ensureStringOrMissing(ctx.db, args.key)
    if (existing === null) {
      return bulk(null)
    }

    if (args.persist) {
      ctx.db.persist(args.key)
    } else if (args.expiresAt !== undefined) {
      ctx.db.setString(args.key, existing, { expiresAt: args.expiresAt })
    }

    return bulk(existing)
  },
})

export const stringsCommands = [
  getCommand,
  setCommand,
  mgetCommand,
  appendCommand,
  strlenCommand,
  incrCommand,
  decrCommand,
  incrbyCommand,
  decrbyCommand,
  incrbyfloatCommand,
  getsetCommand,
  getdelCommand,
  setnxCommand,
  setexCommand,
  psetexCommand,
  msetCommand,
  msetnxCommand,
  getrangeCommand,
  substrCommand,
  setrangeCommand,
  getexCommand,
]

function createSetSchema(): CommandSchema<SetArgs> {
  return t.custom(
    (input: readonly Buffer[], index: number, ctx: ParseContext) => {
      const key = input[index]
      const value = input[index + 1]
      if (!key || !value) {
        throwWrongArity(ctx.commandName)
      }

      const args: SetArgs = {
        key,
        value,
      }

      let cursor = index + 2
      while (cursor < input.length) {
        const option = input[cursor]!.toString().toUpperCase()

        if (option === 'NX' || option === 'XX') {
          if (args.condition) {
            throw new RedisSyntaxError()
          }

          args.condition = option
          cursor += 1
          continue
        }

        if (option === 'GET') {
          if (args.get) {
            throw new RedisSyntaxError()
          }

          args.get = true
          cursor += 1
          continue
        }

        if (option === 'KEEPTTL') {
          if (args.keepTtl || args.expiresAt !== undefined) {
            throw new RedisSyntaxError()
          }

          args.keepTtl = true
          cursor += 1
          continue
        }

        if (
          option === 'EX' ||
          option === 'PX' ||
          option === 'EXAT' ||
          option === 'PXAT'
        ) {
          if (args.expiresAt !== undefined || args.keepTtl) {
            throw new RedisSyntaxError()
          }

          const ttl = requireNextOptionValue(input, cursor + 1)
          args.expiresAt = parseSetExpiration(option, ttl)
          cursor += 2
          continue
        }

        throw new RedisSyntaxError()
      }

      return { value: args, nextIndex: input.length }
    },
  )
}

function parseSetExpiration(option: string, token: Buffer): number {
  const value = parsePositiveExpireToken(token, 'set')

  switch (option) {
    case 'EX':
      return Date.now() + value * 1000
    case 'PX':
      return Date.now() + value
    case 'EXAT':
      return value * 1000
    case 'PXAT':
      return value
    default:
      throw new RedisSyntaxError()
  }
}

function throwWrongArity(commandName: string): never {
  throw new WrongNumberOfArgumentsError(commandName)
}

type KeyValuePair = { key: Buffer; value: Buffer }

type GetexArgs = {
  key: Buffer
  expiresAt?: number
  persist?: boolean
}

function incrementBy(
  db: RedisDatabase,
  key: Buffer,
  delta: bigint,
): RedisResult {
  const existing = ensureStringOrMissing(db, key)
  const current = existing ? parseInt64Token(existing) : 0n
  const next = current + delta
  if (next > INT64_MAX || next < INT64_MIN) {
    throw new IncrDecrOverflowError()
  }
  db.setString(key, Buffer.from(next.toString()), { keepTtl: true })
  return integer(next)
}

function createKeyValuePairsSchema(): CommandSchema<KeyValuePair[]> {
  return t.custom(
    (input: readonly Buffer[], index: number, ctx: ParseContext) => {
      const pairs: KeyValuePair[] = []
      let cursor = index

      if (cursor >= input.length) {
        throwWrongArity(ctx.commandName)
      }

      while (cursor < input.length) {
        const key = input[cursor]
        const value = input[cursor + 1]
        if (!key || !value) {
          throwWrongArity(ctx.commandName)
        }
        pairs.push({ key, value })
        cursor += 2
      }

      if (pairs.length === 0) {
        throwWrongArity(ctx.commandName)
      }

      return { value: pairs, nextIndex: cursor }
    },
  )
}

function createSetrangeOffsetSchema(): CommandSchema<number> {
  return t.custom((input, index, ctx) => {
    const token = input[index]
    if (!token) {
      throwWrongArity(ctx.commandName)
    }

    const offset = parseIntegerToken(token)
    if (offset < 0) {
      throw new OffsetOutOfRangeError()
    }

    return { value: offset, nextIndex: index + 1 }
  })
}

function createGetexSchema(): CommandSchema<GetexArgs> {
  return t.custom(
    (input: readonly Buffer[], index: number, ctx: ParseContext) => {
      const key = input[index]
      if (!key) {
        throwWrongArity(ctx.commandName)
      }

      const args: GetexArgs = { key }
      let cursor = index + 1

      while (cursor < input.length) {
        const option = input[cursor]!.toString().toUpperCase()

        if (option === 'PERSIST') {
          if (args.persist || args.expiresAt !== undefined) {
            throw new RedisSyntaxError()
          }
          args.persist = true
          cursor += 1
          continue
        }

        if (
          option === 'EX' ||
          option === 'PX' ||
          option === 'EXAT' ||
          option === 'PXAT'
        ) {
          if (args.expiresAt !== undefined || args.persist) {
            throw new RedisSyntaxError()
          }

          const ttl = requireNextOptionValue(input, cursor + 1)
          args.expiresAt = parseGetexExpiration(option, ttl)
          cursor += 2
          continue
        }

        throw new RedisSyntaxError()
      }

      return { value: args, nextIndex: input.length }
    },
  )
}

function parseGetexExpiration(option: string, token: Buffer): number {
  const value = parsePositiveExpireToken(token, 'getex')

  switch (option) {
    case 'EX':
      return Date.now() + value * 1000
    case 'PX':
      return Date.now() + value
    case 'EXAT':
      return value * 1000
    case 'PXAT':
      return value
    default:
      throw new RedisSyntaxError()
  }
}
