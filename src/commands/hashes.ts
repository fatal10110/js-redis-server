import { defineCommand } from '../core/command-definition'
import {
  isIntegerToken,
  parseFiniteFloatToken,
  t,
  type ParseContext,
} from '../core/command-schema'
import type { RedisExecutionContext } from '../core/redis-context'
import {
  ExpectedIntegerError,
  HashValueNotFloatError,
  HashValueNotIntegerError,
  IncrDecrOverflowError,
  InvalidExpireTimeError,
  RedisCommandError,
  RedisSyntaxError,
  WrongNumberOfArgumentsError,
} from '../core/redis-error'
import { RedisResult } from '../core/redis-result'
import { RedisValue } from '../core/redis-value'
import type { TrackedHashData } from '../state/tracked-values'
import {
  array,
  bulk,
  integer,
  ok,
  parseIntegerToken,
  ttlMilliseconds,
  ttlSeconds,
} from './helpers'

type FieldValuePair = { field: Buffer; value: Buffer }
type HrandfieldArgs = {
  key: Buffer
  count: number | undefined
  withValues: boolean
}
type HashFieldsArgs = {
  key: Buffer
  fields: Buffer[]
}
type HashExpireOption = 'NX' | 'XX' | 'GT' | 'LT'
type HashExpireMode =
  | 'seconds'
  | 'milliseconds'
  | 'unix-seconds'
  | 'unix-milliseconds'

const HASH_FIELD_EXPIRATION_SINCE = { redis: '7.4.0', valkey: '9.0.0' } as const
const HGETEX_SINCE = { redis: '8.0.0', valkey: '9.0.0' } as const
const HGETDEL_SINCE = { redis: '8.0.0', valkey: '9.1.0' } as const
type HashExpireArgs = {
  key: Buffer
  rawArgs: Buffer[]
}
type ParsedHashExpireArgs = {
  time: bigint
  option: HashExpireOption | undefined
  fields: Buffer[]
}
type HgetexExpiration =
  | { kind: 'keep' }
  | { kind: 'persist' }
  | { kind: 'set'; mode: HashExpireMode; time: bigint }
type HgetexPlan =
  | { kind: 'keep' }
  | { kind: 'persist' }
  | { kind: 'expireAt'; at: number }
type HgetexArgs = {
  key: Buffer
  expiration: HgetexExpiration
  fields: Buffer[]
}

const LONG_MAX = 9223372036854775807n
const LONG_MIN = -9223372036854775808n
const HASH_FIELD_EXPIRE_MAX_ABS_MS = 0x0000ffffffffffffn >> 2n

function createFieldValuePairsSchema() {
  return t.custom<FieldValuePair[]>(
    (input: readonly Buffer[], index: number, ctx: ParseContext) => {
      const pairs: FieldValuePair[] = []
      let cursor = index
      if (cursor >= input.length) {
        throw new WrongNumberOfArgumentsError(ctx.commandName)
      }
      while (cursor < input.length) {
        const field = input[cursor]
        const value = input[cursor + 1]
        if (!field || !value) {
          throw new WrongNumberOfArgumentsError(ctx.commandName)
        }
        pairs.push({ field, value })
        cursor += 2
      }
      if (pairs.length === 0) {
        throw new WrongNumberOfArgumentsError(ctx.commandName)
      }
      return { value: pairs, nextIndex: cursor }
    },
  )
}

function createHrandfieldSchema() {
  return t.custom<HrandfieldArgs>(
    (input: readonly Buffer[], index: number, ctx: ParseContext) => {
      const key = input[index]
      if (!key) {
        throw new WrongNumberOfArgumentsError(ctx.commandName)
      }

      let cursor = index + 1
      if (cursor >= input.length) {
        return {
          value: { key, count: undefined, withValues: false },
          nextIndex: cursor,
        }
      }

      const count = parseIntegerToken(input[cursor])
      cursor++

      let withValues = false
      if (cursor < input.length) {
        if (input[cursor].toString().toUpperCase() !== 'WITHVALUES') {
          throw new RedisSyntaxError()
        }

        withValues = true
        cursor++
      }

      if (cursor !== input.length) {
        throw new RedisSyntaxError()
      }

      return { value: { key, count, withValues }, nextIndex: cursor }
    },
  )
}

function createHashFieldsSchema() {
  return t.custom<HashFieldsArgs>(
    (input: readonly Buffer[], index: number, ctx: ParseContext) => {
      const key = input[index]
      if (!key || input.length - index < 4) {
        throw new WrongNumberOfArgumentsError(ctx.commandName)
      }

      const fieldsToken = input[index + 1]
      if (fieldsToken.toString().toUpperCase() !== 'FIELDS') {
        throw new RedisCommandError(
          'Mandatory argument FIELDS is missing or not at the right position',
        )
      }

      const fieldCount = parsePositiveFieldCount(input[index + 2])
      const fields = input.slice(index + 3)
      if (fieldCount !== BigInt(fields.length)) {
        throw new RedisCommandError(
          'The `numfields` parameter must match the number of arguments',
        )
      }

      return {
        value: { key, fields },
        nextIndex: input.length,
      }
    },
  )
}

function createHgetexSchema() {
  return t.custom<HgetexArgs>(
    (input: readonly Buffer[], index: number, ctx: ParseContext) => {
      const key = input[index]
      if (!key || input.length - index < 4) {
        throw new WrongNumberOfArgumentsError(ctx.commandName)
      }

      const { expiration, nextIndex } = parseHgetexExpiration(
        input,
        index + 1,
        ctx.commandName,
      )

      const fieldsToken = input[nextIndex]
      if (!fieldsToken || fieldsToken.toString().toUpperCase() !== 'FIELDS') {
        throw new RedisCommandError(
          'Mandatory argument FIELDS is missing or not at the right position',
        )
      }

      const fieldCountToken = input[nextIndex + 1]
      if (!fieldCountToken) {
        throw new WrongNumberOfArgumentsError(ctx.commandName)
      }

      const fieldCount = parsePositiveFieldCount(fieldCountToken)
      const fields = input.slice(nextIndex + 2)
      if (fieldCount !== BigInt(fields.length)) {
        throw new RedisCommandError(
          'The `numfields` parameter must match the number of arguments',
        )
      }

      return { value: { key, expiration, fields }, nextIndex: input.length }
    },
  )
}

function parseHgetexExpiration(
  input: readonly Buffer[],
  index: number,
  commandName: string,
): { expiration: HgetexExpiration; nextIndex: number } {
  const token = input[index]?.toString().toUpperCase()
  if (token === 'PERSIST') {
    return { expiration: { kind: 'persist' }, nextIndex: index + 1 }
  }

  const mode = hgetexExpireMode(token)
  if (!mode) {
    return { expiration: { kind: 'keep' }, nextIndex: index }
  }

  const timeToken = input[index + 1]
  if (!timeToken) {
    throw new WrongNumberOfArgumentsError(commandName)
  }

  const time = parseHashExpireTime(timeToken)
  return { expiration: { kind: 'set', mode, time }, nextIndex: index + 2 }
}

function hgetexExpireMode(
  token: string | undefined,
): HashExpireMode | undefined {
  if (token === 'EX') return 'seconds'
  if (token === 'PX') return 'milliseconds'
  if (token === 'EXAT') return 'unix-seconds'
  if (token === 'PXAT') return 'unix-milliseconds'
  return undefined
}

function persistHashFields(
  args: HashFieldsArgs,
  ctx: RedisExecutionContext,
): RedisResult {
  const existingHash = ctx.db.getHash(args.key)
  if (!existingHash) {
    return array(args.fields.map(() => RedisValue.integer(-2)))
  }

  return ctx.db.updateHash(args.key, hash => {
    return array(
      args.fields.map(field => {
        const entry = hash.getField(field)
        if (!entry) {
          return RedisValue.integer(-2)
        }

        if (entry.expiresAt === undefined) {
          return RedisValue.integer(-1)
        }

        hash.clearFieldExpiration(field)
        return RedisValue.integer(1)
      }),
    )
  })
}

function hashFieldTtls(
  args: HashFieldsArgs,
  ctx: RedisExecutionContext,
  mode: 'seconds' | 'milliseconds',
): RedisResult {
  const existingHash = ctx.db.getHash(args.key)
  if (!existingHash) {
    return array(args.fields.map(() => RedisValue.integer(-2)))
  }

  return ctx.db.updateHash(args.key, hash => {
    return array(
      args.fields.map(field => {
        const entry = hash.getField(field)
        if (!entry) {
          return RedisValue.integer(-2)
        }

        if (entry.expiresAt === undefined) {
          return RedisValue.integer(-1)
        }

        const ttl =
          mode === 'seconds'
            ? ttlSeconds(entry.expiresAt)
            : ttlMilliseconds(entry.expiresAt)
        return RedisValue.integer(ttl)
      }),
    )
  })
}

function createHashExpireSchema() {
  return t.custom<HashExpireArgs>(
    (input: readonly Buffer[], index: number, ctx: ParseContext) => {
      const key = input[index]
      if (!key || input.length - index < 5) {
        throw new WrongNumberOfArgumentsError(ctx.commandName)
      }

      return {
        value: { key, rawArgs: input.slice(index + 1) },
        nextIndex: input.length,
      }
    },
  )
}

function parseHashExpireArgs(
  rawArgs: readonly Buffer[],
  commandName: string,
): ParsedHashExpireArgs {
  const time = parseHashExpireTime(rawArgs[0])
  let cursor = 1

  let option: HashExpireOption | undefined
  const maybeOption = rawArgs[cursor].toString().toUpperCase()
  if (isHashExpireOption(maybeOption)) {
    option = maybeOption
    cursor += 1
  }

  const fieldsToken = rawArgs[cursor]
  if (!fieldsToken) {
    throw new WrongNumberOfArgumentsError(commandName)
  }
  if (fieldsToken.toString().toUpperCase() !== 'FIELDS') {
    throw new RedisCommandError(
      'Mandatory argument FIELDS is missing or not at the right position',
    )
  }

  const fieldCountToken = rawArgs[cursor + 1]
  if (!fieldCountToken) {
    throw new WrongNumberOfArgumentsError(commandName)
  }

  const fieldCount = parseHashExpireFieldCount(fieldCountToken)
  const fields = rawArgs.slice(cursor + 2)
  if (fieldCount !== BigInt(fields.length)) {
    throw new RedisCommandError(
      'The `numfields` parameter must match the number of arguments',
    )
  }

  return { time, option, fields }
}

function parsePositiveFieldCount(token: Buffer): bigint {
  const raw = token.toString()
  if (!isIntegerToken(raw)) {
    throw new RedisCommandError('Number of fields must be a positive integer')
  }

  const value = BigInt(raw)
  if (value < 1n || value > LONG_MAX) {
    throw new RedisCommandError('Number of fields must be a positive integer')
  }

  return value
}

function parseHashExpireTime(token: Buffer): bigint {
  const raw = token.toString()
  if (!isIntegerToken(raw)) {
    throw new ExpectedIntegerError()
  }

  const value = BigInt(raw)
  if (value < LONG_MIN || value > LONG_MAX) {
    throw new ExpectedIntegerError()
  }

  return value
}

function parseHashExpireFieldCount(token: Buffer): bigint {
  const raw = token.toString()
  if (!isIntegerToken(raw)) {
    throw new RedisCommandError(
      'Parameter `numFields` should be greater than 0',
    )
  }

  const value = BigInt(raw)
  if (value < 1n || value > LONG_MAX) {
    throw new RedisCommandError(
      'Parameter `numFields` should be greater than 0',
    )
  }

  return value
}

function isHashExpireOption(value: string): value is HashExpireOption {
  return value === 'NX' || value === 'XX' || value === 'GT' || value === 'LT'
}

function withExistingHash<TResult>(
  ctx: RedisExecutionContext,
  key: Buffer,
  missing: () => TResult,
  callback: (hash: TrackedHashData) => TResult,
): TResult {
  const hash = ctx.db.getHash(key)
  if (!hash) return missing()

  return ctx.db.updateHash(key, callback)
}

function expireHashFields(
  args: HashExpireArgs,
  ctx: RedisExecutionContext,
  mode: HashExpireMode,
): RedisResult {
  const commandName = hashExpireCommandName(mode)
  const existingHash = ctx.db.getHash(args.key)
  const parsedArgs = parseHashExpireArgs(args.rawArgs, commandName)
  const expiresAt = hashExpireTimeToTimestamp(
    parsedArgs.time,
    mode,
    commandName,
  )
  const now = Date.now()

  if (!existingHash) {
    return array(parsedArgs.fields.map(() => RedisValue.integer(-2)))
  }

  return ctx.db.updateHash(args.key, hash => {
    return array(
      parsedArgs.fields.map(field => {
        const entry = hash.getField(field)
        if (!entry) {
          return RedisValue.integer(-2)
        }

        if (
          !shouldApplyHashExpire(entry.expiresAt, expiresAt, parsedArgs.option)
        ) {
          return RedisValue.integer(0)
        }

        if (expiresAt <= now) {
          hash.deleteField(field)
          return RedisValue.integer(2)
        }

        hash.setFieldExpiration(field, expiresAt)
        return RedisValue.integer(1)
      }),
    )
  })
}

function resolveHgetexPlan(expiration: HgetexExpiration): HgetexPlan {
  if (expiration.kind === 'set') {
    return {
      kind: 'expireAt',
      at: hashExpireTimeToTimestamp(expiration.time, expiration.mode, 'hgetex'),
    }
  }
  return expiration
}

function getexHashFields(
  args: HgetexArgs,
  ctx: RedisExecutionContext,
): RedisResult {
  // Resolve the target timestamp up front so an invalid expire time is
  // reported even when the key is missing, matching the HEXPIRE ordering.
  const plan = resolveHgetexPlan(args.expiration)

  const existingHash = ctx.db.getHash(args.key)
  if (!existingHash) {
    return array(args.fields.map(() => RedisValue.bulkString(null)))
  }

  const now = Date.now()
  let remaining = 0
  const values = ctx.db.updateHash(args.key, hash => {
    const replies: RedisValue[] = []
    for (const field of args.fields) {
      const entry = hash.getField(field)
      replies.push(RedisValue.bulkString(entry?.value ?? null))
      if (!entry) continue

      if (plan.kind === 'persist') {
        hash.clearFieldExpiration(field)
        continue
      }

      if (plan.kind === 'expireAt') {
        if (plan.at <= now) {
          hash.deleteField(field)
        } else {
          hash.setFieldExpiration(field, plan.at)
        }
      }
    }
    remaining = hash.size
    return replies
  })

  if (remaining === 0) {
    ctx.db.delete(args.key)
  }
  return array(values)
}

function hashExpireTimeToTimestamp(
  value: bigint,
  mode: HashExpireMode,
  commandName: string,
): number {
  if (value < 0n) {
    throw new RedisCommandError('invalid expire time, must be >= 0')
  }

  const isSecondsMode = mode === 'seconds' || mode === 'unix-seconds'
  if (isSecondsMode && value > HASH_FIELD_EXPIRE_MAX_ABS_MS / 1000n) {
    throw new InvalidExpireTimeError(commandName)
  }

  const milliseconds = isSecondsMode ? value * 1000n : value
  const baseTime =
    mode === 'seconds' || mode === 'milliseconds' ? BigInt(Date.now()) : 0n

  if (milliseconds > HASH_FIELD_EXPIRE_MAX_ABS_MS - baseTime) {
    throw new InvalidExpireTimeError(commandName)
  }

  return Number(milliseconds + baseTime)
}

function hashExpireCommandName(mode: HashExpireMode): string {
  if (mode === 'seconds') {
    return 'hexpire'
  }

  if (mode === 'milliseconds') {
    return 'hpexpire'
  }

  if (mode === 'unix-seconds') {
    return 'hexpireat'
  }

  return 'hpexpireat'
}

function shouldApplyHashExpire(
  currentExpiresAt: number | undefined,
  nextExpiresAt: number,
  option: HashExpireOption | undefined,
): boolean {
  if (option === 'NX') {
    return currentExpiresAt === undefined
  }

  if (option === 'XX') {
    return currentExpiresAt !== undefined
  }

  if (option === 'GT') {
    return currentExpiresAt !== undefined && nextExpiresAt > currentExpiresAt
  }

  if (option === 'LT') {
    return currentExpiresAt === undefined || nextExpiresAt < currentExpiresAt
  }

  return true
}

function randomHashEntries<TValue>(entries: TValue[], count: number): TValue[] {
  if (count === 0) return []

  if (count > 0) {
    const limit = Math.min(count, entries.length)
    const pool = entries.slice()
    const result: TValue[] = []

    for (let i = 0; i < limit; i++) {
      const j = i + Math.floor(Math.random() * (pool.length - i))
      ;[pool[i], pool[j]] = [pool[j], pool[i]]
      result.push(pool[i])
    }

    return result
  }

  const result: TValue[] = []
  for (let i = 0; i < Math.abs(count); i++) {
    result.push(entries[Math.floor(Math.random() * entries.length)])
  }

  return result
}

function hashEntriesResult(
  entries: FieldValuePair[],
  withValues: boolean,
): RedisResult {
  if (!withValues) {
    return array(entries.map(({ field }) => RedisValue.bulkString(field)))
  }

  // WITHVALUES is a flat [field, value, ...] array on RESP2 and an array of
  // [field, value] pairs on RESP3 — same shape as sorted-set WITHSCORES.
  return RedisResult.create(
    RedisValue.flatPairs(
      entries.map(({ field, value }) => [
        RedisValue.bulkString(field),
        RedisValue.bulkString(value),
      ]),
    ),
  )
}

export const hsetCommand = defineCommand({
  name: 'hset',
  schema: t.object({ key: t.key(), pairs: createFieldValuePairsSchema() }),
  flags: ['write', 'denyoom', 'fast'],
  keys: args => [args.key],
  execute: (args, ctx) => {
    const added = ctx.db.updateHash(args.key, hash => {
      let count = 0
      for (const { field, value } of args.pairs) {
        if (hash.setField(field, value, { forceDirty: true }).added) count++
      }
      return count
    })
    return integer(added)
  },
})

export const hsetnxCommand = defineCommand({
  name: 'hsetnx',
  schema: t.object({ key: t.key(), field: t.key(), value: t.key() }),
  flags: ['write', 'denyoom', 'fast'],
  keys: args => [args.key],
  execute: (args, ctx) => {
    const set = ctx.db.updateHash(args.key, hash => {
      return hash.setFieldIfAbsent(args.field, args.value)
    })
    return integer(set ? 1 : 0)
  },
})

export const hgetCommand = defineCommand({
  name: 'hget',
  schema: t.object({ key: t.key(), field: t.key() }),
  flags: ['readonly', 'fast'],
  keys: args => [args.key],
  execute: (args, ctx) => {
    return withExistingHash(
      ctx,
      args.key,
      () => bulk(null),
      hash => bulk(hash.getField(args.field)?.value ?? null),
    )
  },
})

export const hdelCommand = defineCommand({
  name: 'hdel',
  schema: t.object({ key: t.key(), fields: t.variadic(t.key(), { min: 1 }) }),
  flags: ['write', 'fast'],
  keys: args => [args.key],
  execute: (args, ctx) => {
    const existingHash = ctx.db.getHash(args.key)
    if (!existingHash) return integer(0)

    let deleted = 0
    const remaining = ctx.db.updateHash(args.key, hash => {
      for (const field of args.fields) {
        if (hash.deleteField(field)) deleted++
      }
      return hash.size
    })
    if (remaining === 0) {
      ctx.db.delete(args.key)
    }
    return integer(deleted)
  },
})

export const hgetdelCommand = defineCommand({
  name: 'hgetdel',
  since: HGETDEL_SINCE,
  schema: createHashFieldsSchema(),
  flags: ['write', 'fast'],
  keys: args => [args.key],
  execute: (args, ctx) => {
    if (!ctx.db.getHash(args.key)) {
      return array(args.fields.map(() => RedisValue.bulkString(null)))
    }

    let remaining = 0
    const values = ctx.db.updateHash(args.key, hash => {
      const replies: RedisValue[] = []
      for (const field of args.fields) {
        const entry = hash.getField(field)
        replies.push(RedisValue.bulkString(entry?.value ?? null))
        if (entry) hash.deleteField(field)
      }
      remaining = hash.size
      return replies
    })

    if (remaining === 0) {
      ctx.db.delete(args.key)
    }
    return array(values)
  },
})

export const hgetexCommand = defineCommand({
  name: 'hgetex',
  since: HGETEX_SINCE,
  schema: createHgetexSchema(),
  flags: ['write', 'fast'],
  keys: args => [args.key],
  execute: getexHashFields,
})

export const hpersistCommand = defineCommand({
  name: 'hpersist',
  since: HASH_FIELD_EXPIRATION_SINCE,
  schema: createHashFieldsSchema(),
  flags: ['write', 'fast'],
  keys: args => [args.key],
  execute: persistHashFields,
})

export const hexpireCommand = defineCommand({
  name: 'hexpire',
  since: HASH_FIELD_EXPIRATION_SINCE,
  schema: createHashExpireSchema(),
  flags: ['write', 'fast'],
  keys: args => [args.key],
  execute: (args, ctx) => expireHashFields(args, ctx, 'seconds'),
})

export const hpexpireCommand = defineCommand({
  name: 'hpexpire',
  since: HASH_FIELD_EXPIRATION_SINCE,
  schema: createHashExpireSchema(),
  flags: ['write', 'fast'],
  keys: args => [args.key],
  execute: (args, ctx) => expireHashFields(args, ctx, 'milliseconds'),
})

export const hexpireatCommand = defineCommand({
  name: 'hexpireat',
  since: HASH_FIELD_EXPIRATION_SINCE,
  schema: createHashExpireSchema(),
  flags: ['write', 'fast'],
  keys: args => [args.key],
  execute: (args, ctx) => expireHashFields(args, ctx, 'unix-seconds'),
})

export const hpexpireatCommand = defineCommand({
  name: 'hpexpireat',
  since: HASH_FIELD_EXPIRATION_SINCE,
  schema: createHashExpireSchema(),
  flags: ['write', 'fast'],
  keys: args => [args.key],
  execute: (args, ctx) => expireHashFields(args, ctx, 'unix-milliseconds'),
})

export const httlCommand = defineCommand({
  name: 'httl',
  since: HASH_FIELD_EXPIRATION_SINCE,
  schema: createHashFieldsSchema(),
  flags: ['readonly', 'fast'],
  keys: args => [args.key],
  execute: (args, ctx) => hashFieldTtls(args, ctx, 'seconds'),
})

export const hpttlCommand = defineCommand({
  name: 'hpttl',
  since: HASH_FIELD_EXPIRATION_SINCE,
  schema: createHashFieldsSchema(),
  flags: ['readonly', 'fast'],
  keys: args => [args.key],
  execute: (args, ctx) => hashFieldTtls(args, ctx, 'milliseconds'),
})

export const hmsetCommand = defineCommand({
  name: 'hmset',
  schema: t.object({ key: t.key(), pairs: createFieldValuePairsSchema() }),
  flags: ['write', 'denyoom', 'fast'],
  keys: args => [args.key],
  execute: (args, ctx) => {
    ctx.db.updateHash(args.key, hash => {
      for (const { field, value } of args.pairs) {
        hash.setField(field, value, { forceDirty: true })
      }
    })
    return ok()
  },
})

export const hmgetCommand = defineCommand({
  name: 'hmget',
  schema: t.object({ key: t.key(), fields: t.variadic(t.key(), { min: 1 }) }),
  flags: ['readonly', 'fast'],
  keys: args => [args.key],
  execute: (args, ctx) => {
    return withExistingHash(
      ctx,
      args.key,
      () => array(args.fields.map(() => RedisValue.bulkString(null))),
      hash =>
        array(
          args.fields.map(field => {
            const entry = hash.getField(field)
            return RedisValue.bulkString(entry?.value ?? null)
          }),
        ),
    )
  },
})

export const hgetallCommand = defineCommand({
  name: 'hgetall',
  schema: t.object({ key: t.key() }),
  flags: ['readonly', 'fast'],
  keys: args => [args.key],
  execute: (args, ctx) => {
    return withExistingHash(
      ctx,
      args.key,
      () => RedisResult.create(RedisValue.map([])),
      hash => {
        const entries: [RedisValue, RedisValue][] = []
        for (const { field, value } of hash.entries()) {
          entries.push([
            RedisValue.bulkString(field),
            RedisValue.bulkString(value),
          ])
        }
        return RedisResult.create(RedisValue.map(entries))
      },
    )
  },
})

export const hkeysCommand = defineCommand({
  name: 'hkeys',
  schema: t.object({ key: t.key() }),
  flags: ['readonly', 'fast'],
  keys: args => [args.key],
  execute: (args, ctx) => {
    return withExistingHash(
      ctx,
      args.key,
      () => array([]),
      hash =>
        array(
          Array.from(hash.entries()).map(({ field }) =>
            RedisValue.bulkString(field),
          ),
        ),
    )
  },
})

export const hvalsCommand = defineCommand({
  name: 'hvals',
  schema: t.object({ key: t.key() }),
  flags: ['readonly', 'fast'],
  keys: args => [args.key],
  execute: (args, ctx) => {
    return withExistingHash(
      ctx,
      args.key,
      () => array([]),
      hash =>
        array(
          Array.from(hash.entries()).map(({ value }) =>
            RedisValue.bulkString(value),
          ),
        ),
    )
  },
})

export const hrandfieldCommand = defineCommand({
  name: 'hrandfield',
  since: { redis: '6.2.0', valkey: '7.2.0' },
  schema: createHrandfieldSchema(),
  flags: ['readonly', 'random', 'noscript'],
  keys: args => [args.key],
  execute: (args, ctx) => {
    return withExistingHash(
      ctx,
      args.key,
      () => (args.count === undefined ? bulk(null) : array([])),
      hash => {
        const entries = Array.from(hash.entries())
        if (entries.length === 0) {
          return args.count === undefined ? bulk(null) : array([])
        }

        if (args.count === undefined) {
          const entry = entries[Math.floor(Math.random() * entries.length)]
          return bulk(entry.field)
        }

        return hashEntriesResult(
          randomHashEntries(entries, args.count),
          args.withValues,
        )
      },
    )
  },
})

export const hlenCommand = defineCommand({
  name: 'hlen',
  schema: t.object({ key: t.key() }),
  flags: ['readonly', 'fast'],
  keys: args => [args.key],
  execute: (args, ctx) => {
    return withExistingHash(
      ctx,
      args.key,
      () => integer(0),
      hash => integer(hash.size),
    )
  },
})

export const hexistsCommand = defineCommand({
  name: 'hexists',
  schema: t.object({ key: t.key(), field: t.key() }),
  flags: ['readonly', 'fast'],
  keys: args => [args.key],
  execute: (args, ctx) => {
    return withExistingHash(
      ctx,
      args.key,
      () => integer(0),
      hash => integer(hash.hasField(args.field) ? 1 : 0),
    )
  },
})

export const hincrbyCommand = defineCommand({
  name: 'hincrby',
  schema: t.object({
    key: t.key(),
    field: t.key(),
    increment: t.bigInteger({ min: LONG_MIN, max: LONG_MAX }),
  }),
  flags: ['write', 'fast'],
  keys: args => [args.key],
  execute: (args, ctx) => {
    const result = ctx.db.updateHash(args.key, hash => {
      const entry = hash.getField(args.field)
      let current = 0n
      if (entry) {
        const raw = entry.value.toString()
        // Redis parses the stored value as a 64-bit signed integer; a value
        // outside that range is "hash value is not an integer", not overflow.
        if (!isIntegerToken(raw)) {
          throw new HashValueNotIntegerError()
        }
        current = BigInt(raw)
        if (current < LONG_MIN || current > LONG_MAX) {
          throw new HashValueNotIntegerError()
        }
      }
      const next = current + args.increment
      if (next < LONG_MIN || next > LONG_MAX) {
        throw new IncrDecrOverflowError()
      }
      const valueBuf = Buffer.from(next.toString())
      hash.setField(args.field, valueBuf, { forceDirty: true, keepTtl: true })
      return next
    })
    return integer(result)
  },
})

export const hincrbyfloatCommand = defineCommand({
  name: 'hincrbyfloat',
  schema: t.object({ key: t.key(), field: t.key(), increment: t.float() }),
  flags: ['write', 'fast'],
  keys: args => [args.key],
  execute: (args, ctx) => {
    const result = ctx.db.updateHash(args.key, hash => {
      const entry = hash.getField(args.field)
      let current = 0
      if (entry) {
        const raw = entry.value.toString()
        const parsed = parseFiniteFloatToken(raw)
        if (parsed === undefined) {
          throw new HashValueNotFloatError()
        }
        current = parsed
      }
      const next = current + args.increment
      if (isNaN(next) || !isFinite(next)) {
        throw new HashValueNotFloatError()
      }
      // Format like Redis: strip trailing zeros, use fixed notation for small values
      let formatted = String(next)
      if (formatted.includes('e') || formatted.includes('E')) {
        formatted = next.toFixed(17).replace(/\.?0+$/, '')
      }
      const valueBuf = Buffer.from(formatted)
      hash.setField(args.field, valueBuf, { forceDirty: true, keepTtl: true })
      return valueBuf
    })
    return bulk(result)
  },
})

export const hstrlenCommand = defineCommand({
  name: 'hstrlen',
  schema: t.object({ key: t.key(), field: t.key() }),
  flags: ['readonly', 'fast'],
  keys: args => [args.key],
  execute: (args, ctx) => {
    return withExistingHash(
      ctx,
      args.key,
      () => integer(0),
      hash => integer(hash.getField(args.field)?.value.byteLength ?? 0),
    )
  },
})

export const hashesCommands = [
  hsetCommand,
  hsetnxCommand,
  hgetCommand,
  hdelCommand,
  hgetdelCommand,
  hgetexCommand,
  hpersistCommand,
  hexpireCommand,
  hpexpireCommand,
  hexpireatCommand,
  hpexpireatCommand,
  httlCommand,
  hpttlCommand,
  hmsetCommand,
  hmgetCommand,
  hgetallCommand,
  hkeysCommand,
  hvalsCommand,
  hrandfieldCommand,
  hlenCommand,
  hexistsCommand,
  hincrbyCommand,
  hincrbyfloatCommand,
  hstrlenCommand,
]
