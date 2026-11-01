import { asciiUpperCase } from '../core/ascii-case'
import { defineCommand } from '../core/command-definition'
import { t, type InferSchema } from '../core/command-schema'
import type { RedisExecutionContext } from '../core/redis-context'
import { errors } from '../core/redis-error'
import {
  createDumpPayload,
  deserializeValue,
  RdbFormatError,
  rdbFormatFor,
  verifyDumpPayload,
} from '../state/rdb/payload'
import type { RedisDataValue } from '../state'
import { configuredEncodingLimits } from './config'
import { bulk, ok, parseInt64Token } from './helpers'

/**
 * DUMP key / RESTORE key ttl serialized-value [REPLACE] [ABSTTL]
 * [IDLETIME seconds] [FREQ frequency] (cluster.c dumpCommand /
 * restoreCommand). Payloads are real RDB objects with the profile's RDB
 * version and a CRC-64 footer (`src/state/rdb/payload.ts`), so they move
 * between real servers and the mock in both directions.
 */

export const dumpCommand = defineCommand({
  name: 'dump',
  schema: t.object({ key: t.key() }),
  flags: ['readonly'],
  keys: args => [args.key],
  execute: (args, ctx) => {
    const value = ctx.db.get(args.key)
    if (!value) return bulk(null)
    return bulk(
      createDumpPayload(
        value,
        rdbFormatFor(ctx.server.profile),
        configuredEncodingLimits(ctx.server),
      ),
    )
  },
})

type RestoreOptions = {
  replace: boolean
  absttl: boolean
}

/**
 * RESTORE's options, parsed in argv order like `restoreCommand()`: IDLETIME
 * and FREQ exclude each other (the second is a syntax error), take the next
 * argument whatever it is, and check its range before the key is looked at.
 * Their values only seed the LRU / LFU clock, which the mock does not keep.
 */
const restoreOptionsSchema = t.custom<RestoreOptions>((input, index) => {
  let replace = false
  let absttl = false
  let idle = false
  let freq = false
  for (let j = index; j < input.length; j++) {
    const token = asciiUpperCase(input[j].toString('latin1'))
    const additional = input.length - j - 1
    if (token === 'REPLACE') {
      replace = true
    } else if (token === 'ABSTTL') {
      absttl = true
    } else if (token === 'IDLETIME' && additional >= 1 && !freq) {
      if (parseInt64Token(input[j + 1]) < 0n) {
        throw errors.restoreInvalidIdleTime()
      }
      idle = true
      j++
    } else if (token === 'FREQ' && additional >= 1 && !idle) {
      const value = parseInt64Token(input[j + 1])
      if (value < 0n || value > 255n) throw errors.restoreInvalidFreq()
      freq = true
      j++
    } else {
      throw errors.syntax()
    }
  }
  return { value: { replace, absttl }, nextIndex: input.length }
})

const restoreSchema = t.object({
  key: t.key(),
  // Parsed in execute: an existing key answers BUSYKEY before the TTL is
  // looked at.
  ttl: t.bulk(),
  payload: t.bulk(),
  options: restoreOptionsSchema,
})

type RestoreArgs = InferSchema<typeof restoreSchema>

const restoreSpec = {
  schema: restoreSchema,
  keys: (args: RestoreArgs) => [args.key],
  execute: (args: RestoreArgs, ctx: RedisExecutionContext) => {
    const { key, options } = args
    if (!options.replace && ctx.db.getType(key) !== null) {
      throw errors.busyKey()
    }
    const ttl = parseInt64Token(args.ttl)
    if (ttl < 0n) throw errors.restoreInvalidTtl()

    const format = rdbFormatFor(ctx.server.profile)
    const body = verifyDumpPayload(args.payload, format)
    if (!body) throw errors.dumpPayloadInvalid()
    let value: RedisDataValue
    try {
      value = deserializeValue(
        body,
        format,
        configuredEncodingLimits(ctx.server),
      )
    } catch (error) {
      if (error instanceof RdbFormatError) throw errors.badDataFormat()
      throw error
    }

    const now = Date.now()
    // A relative deadline is `ttl + now` in int64, as real servers compute it
    // (`ttl += commandTimeSnapshot()`): one past INT64_MAX wraps negative and
    // so counts as already expired.
    const expiresAt =
      ttl === 0n
        ? undefined
        : Number(options.absttl ? ttl : BigInt.asIntN(64, ttl + BigInt(now)))
    if (expiresAt !== undefined && expiresAt <= now) {
      // Already expired: nothing is created; REPLACE still deletes the old
      // key, announced as a plain `del`.
      if (options.replace) ctx.db.delete(key)
      return ok()
    }

    // REPLACE deletes and re-adds the key, so it is announced `new` either
    // way, then `restore` (generic class).
    ctx.db.set(key, value, { expiresAt, announceNew: true })
    return ok()
  },
}

export const restoreCommand = defineCommand({
  ...restoreSpec,
  name: 'restore',
  flags: ['write', 'denyoom'],
})

/**
 * RESTORE-ASKING: RESTORE with the `asking` flag, which a cluster node
 * importing the key's slot serves without a preceding ASKING (redis-cli
 * `--cluster` and MIGRATE send it to the target). Everything else is
 * RESTORE's.
 */
export const restoreAskingCommand = defineCommand({
  ...restoreSpec,
  name: 'restore-asking',
  flags: ['write', 'denyoom', 'asking'],
})

export const dumpRestoreCommands = [
  dumpCommand,
  restoreCommand,
  restoreAskingCommand,
]
