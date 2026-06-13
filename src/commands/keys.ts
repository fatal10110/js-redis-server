import { defineCommand } from '../core/command-definition'
import { t } from '../core/command-schema'
import { NoSuchKeyError } from '../core/redis-error'
import type { RedisDatabase } from '../state'
import {
  integer,
  ok,
  simpleString,
  ttlMilliseconds,
  ttlSeconds,
  typeName,
} from './helpers'

export const delCommand = defineCommand({
  name: 'del',
  schema: t.object({
    keys: t.variadic(t.key(), { min: 1 }),
  }),
  flags: ['write'],
  keys: args => args.keys,
  execute: (args, ctx) => {
    let count = 0
    for (const key of args.keys) {
      if (ctx.db.delete(key)) {
        count += 1
      }
    }

    return integer(count)
  },
})

export const unlinkCommand = defineCommand({
  ...delCommand,
  name: 'unlink',
})

export const existsCommand = defineCommand({
  name: 'exists',
  schema: t.object({
    keys: t.variadic(t.key(), { min: 1 }),
  }),
  flags: ['readonly', 'fast'],
  keys: args => args.keys,
  execute: (args, ctx) => {
    let count = 0
    for (const key of args.keys) {
      if (ctx.db.getType(key) !== null) {
        count += 1
      }
    }

    return integer(count)
  },
})

export const typeCommand = defineCommand({
  name: 'type',
  schema: t.object({
    key: t.key(),
  }),
  flags: ['readonly', 'fast'],
  keys: args => [args.key],
  execute: (args, ctx) => simpleString(typeName(ctx.db.getType(args.key))),
})

export const dbsizeCommand = defineCommand({
  name: 'dbsize',
  schema: t.object({}),
  flags: ['readonly', 'fast'],
  keys: () => [],
  execute: (_args, ctx) => integer(ctx.db.size()),
})

export const ttlCommand = defineCommand({
  name: 'ttl',
  schema: t.object({
    key: t.key(),
  }),
  flags: ['readonly', 'fast'],
  keys: args => [args.key],
  execute: (args, ctx) => {
    const expiration = ctx.db.getExpiration(args.key)
    if (expiration.kind === 'missing') {
      return integer(-2)
    }

    if (expiration.kind === 'persistent') {
      return integer(-1)
    }

    return integer(ttlSeconds(expiration.expiresAt))
  },
})

export const pttlCommand = defineCommand({
  name: 'pttl',
  schema: t.object({
    key: t.key(),
  }),
  flags: ['readonly', 'fast'],
  keys: args => [args.key],
  execute: (args, ctx) => {
    const expiration = ctx.db.getExpiration(args.key)
    if (expiration.kind === 'missing') {
      return integer(-2)
    }

    if (expiration.kind === 'persistent') {
      return integer(-1)
    }

    return integer(ttlMilliseconds(expiration.expiresAt))
  },
})

export const expiretimeCommand = defineCommand({
  name: 'expiretime',
  schema: t.object({
    key: t.key(),
  }),
  flags: ['readonly', 'fast'],
  keys: args => [args.key],
  execute: (args, ctx) => {
    const expiration = ctx.db.getExpiration(args.key)
    if (expiration.kind === 'missing') {
      return integer(-2)
    }

    if (expiration.kind === 'persistent') {
      return integer(-1)
    }

    return integer(Math.floor(expiration.expiresAt / 1000))
  },
})

export const pexpiretimeCommand = defineCommand({
  name: 'pexpiretime',
  schema: t.object({
    key: t.key(),
  }),
  flags: ['readonly', 'fast'],
  keys: args => [args.key],
  execute: (args, ctx) => {
    const expiration = ctx.db.getExpiration(args.key)
    if (expiration.kind === 'missing') {
      return integer(-2)
    }

    if (expiration.kind === 'persistent') {
      return integer(-1)
    }

    return integer(expiration.expiresAt)
  },
})

export const expireCommand = defineCommand({
  name: 'expire',
  schema: t.object({
    key: t.key(),
    seconds: t.integer(),
  }),
  flags: ['write', 'fast'],
  keys: args => [args.key],
  execute: (args, ctx) => expireKey(ctx.db, args.key, args.seconds, 1000),
})

export const pexpireCommand = defineCommand({
  name: 'pexpire',
  schema: t.object({
    key: t.key(),
    milliseconds: t.integer(),
  }),
  flags: ['write', 'fast'],
  keys: args => [args.key],
  execute: (args, ctx) => expireKey(ctx.db, args.key, args.milliseconds, 1),
})

export const persistCommand = defineCommand({
  name: 'persist',
  schema: t.object({
    key: t.key(),
  }),
  flags: ['write', 'fast'],
  keys: args => [args.key],
  execute: (args, ctx) => integer(ctx.db.persist(args.key) ? 1 : 0),
})

export const flushdbCommand = defineCommand({
  name: 'flushdb',
  schema: t.object({}),
  flags: ['write'],
  keys: () => [],
  execute: (_args, ctx) => {
    ctx.db.flush()
    return ok()
  },
})

export const flushallCommand = defineCommand({
  name: 'flushall',
  schema: t.object({}),
  flags: ['write'],
  keys: () => [],
  execute: (_args, ctx) => {
    ctx.server.flushAllDatabases()
    return ok()
  },
})

export const expireatCommand = defineCommand({
  name: 'expireat',
  schema: t.object({ key: t.key(), timestamp: t.integer() }),
  flags: ['write', 'fast'],
  keys: args => [args.key],
  execute: (args, ctx) => {
    if (ctx.db.getType(args.key) === null) return integer(0)
    const expiresAt = args.timestamp * 1000
    if (expiresAt <= Date.now()) {
      ctx.db.delete(args.key)
      return integer(1)
    }
    return integer(ctx.db.expire(args.key, expiresAt) ? 1 : 0)
  },
})

export const pexpireatCommand = defineCommand({
  name: 'pexpireat',
  schema: t.object({ key: t.key(), timestamp: t.integer() }),
  flags: ['write', 'fast'],
  keys: args => [args.key],
  execute: (args, ctx) => {
    if (ctx.db.getType(args.key) === null) return integer(0)
    if (args.timestamp <= Date.now()) {
      ctx.db.delete(args.key)
      return integer(1)
    }
    return integer(ctx.db.expire(args.key, args.timestamp) ? 1 : 0)
  },
})

export const renameCommand = defineCommand({
  name: 'rename',
  schema: t.object({ key: t.key(), newKey: t.key() }),
  flags: ['write'],
  keys: args => [args.key, args.newKey],
  execute: (args, ctx) => {
    const value = ctx.db.get(args.key)
    if (!value) throw new NoSuchKeyError()

    const expiration = ctx.db.getExpiration(args.key)
    const expiresAt =
      expiration.kind === 'expires' ? expiration.expiresAt : undefined

    ctx.db.delete(args.key)
    ctx.db.set(
      args.newKey,
      value,
      expiresAt !== undefined ? { expiresAt } : undefined,
    )

    return ok()
  },
})

export const renamenxCommand = defineCommand({
  name: 'renamenx',
  schema: t.object({ key: t.key(), newKey: t.key() }),
  flags: ['write', 'fast'],
  keys: args => [args.key, args.newKey],
  execute: (args, ctx) => {
    const value = ctx.db.get(args.key)
    if (!value) throw new NoSuchKeyError()

    if (ctx.db.getType(args.newKey) !== null) return integer(0)

    const expiration = ctx.db.getExpiration(args.key)
    const expiresAt =
      expiration.kind === 'expires' ? expiration.expiresAt : undefined

    ctx.db.delete(args.key)
    ctx.db.set(
      args.newKey,
      value,
      expiresAt !== undefined ? { expiresAt } : undefined,
    )

    return integer(1)
  },
})

export const keysCommands = [
  delCommand,
  unlinkCommand,
  existsCommand,
  typeCommand,
  dbsizeCommand,
  ttlCommand,
  pttlCommand,
  expiretimeCommand,
  pexpiretimeCommand,
  expireCommand,
  pexpireCommand,
  persistCommand,
  flushdbCommand,
  flushallCommand,
  expireatCommand,
  pexpireatCommand,
  renameCommand,
  renamenxCommand,
]

function expireKey(
  db: RedisDatabase,
  key: Buffer,
  duration: number,
  multiplier: number,
) {
  if (db.getType(key) === null) {
    return integer(0)
  }

  if (duration <= 0) {
    return integer(db.delete(key) ? 1 : 0)
  }

  return integer(db.expire(key, Date.now() + duration * multiplier) ? 1 : 0)
}
