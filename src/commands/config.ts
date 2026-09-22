import { defineCommand } from '../core/command-definition'
import { t } from '../core/command-schema'
import type { CompatibilityProfile } from '../core/compatibility'
import type { RedisExecutionContext } from '../core/redis-context'
import {
  RedisCommandError,
  WrongNumberOfArgumentsError,
} from '../core/redis-error'
import { RedisResult } from '../core/redis-result'
import { RedisValue } from '../core/redis-value'
import { normalizeKeyspaceNotifyConfig } from '../state'
import { INT64_MAX, ok } from './helpers'
import { commandSubcommandInfo } from './introspection'

// Behavior-driving parameters whose authoritative value lives on the server
// state (not the inert defaults map below), so other subsystems can read them.
const KEYSPACE_NOTIFY_PARAM = 'notify-keyspace-events'
const PROTO_MAX_BULK_LEN_PARAM = 'proto-max-bulk-len'

// Redis' bounds for `proto-max-bulk-len` (config.c: createSizeTConfig).
const PROTO_MAX_BULK_LEN_MIN = 1048576n
const PROTO_MAX_BULK_LEN_MAX = 9223372036854775807n

// Redis memory-value suffixes (util.c: memtoull) — the bare `k`/`m`/`g` forms
// are decimal, the `b`-suffixed ones binary. A Map, not an object literal: the
// suffix comes straight off the wire, and an object literal would resolve
// `constructor` (already lower-case, so `toLowerCase()` does not save us)
// through the prototype chain and hand the caller `Object` instead of undefined.
const MEMORY_UNITS = new Map<string, bigint>([
  ['b', 1n],
  ['k', 1000n],
  ['kb', 1024n],
  ['m', 1000000n],
  ['mb', 1048576n],
  ['g', 1000000000n],
  ['gb', 1073741824n],
])

/**
 * CONFIG SET's failure wording, which Redis 7.0 changed wholesale when it
 * rewrote the subcommand to accept several parameter pairs:
 *
 *   6.2   ERR Invalid argument '<value>' for CONFIG SET '<name>' - <detail>
 *   7.0+  ERR CONFIG SET failed (possibly related to argument '<name>') - <detail>
 */
function configSetFailed(
  profile: CompatibilityProfile,
  name: string,
  value: string,
  detail: string,
): RedisCommandError {
  if (!profile.has('config.set.failure-message')) {
    return new RedisCommandError(
      `Invalid argument '${value}' for CONFIG SET '${name}' - ${detail}`,
    )
  }

  return new RedisCommandError(
    `CONFIG SET failed (possibly related to argument '${name}') - ${detail}`,
  )
}

/**
 * Parse a Redis memory value (`1048576`, `1mb`, `512MB`, ...) into bytes and
 * range-check it against a parameter's bounds, reproducing CONFIG SET's two
 * distinct failure messages.
 *
 * Empty input is *not* a parse failure: Redis' `memtoull` reads it as 0, which
 * then fails the range check instead.
 *
 * Redis 6.2 parses the decimal literal with `strtoll`, which saturates at
 * {@link INT64_MAX} rather than failing, and only *then* runs the parameter's boundary check;
 * 7.0+ rejects an over-long literal outright. So the saturation below clamps to
 * int64 max and falls through to the boundary check, rather than clamping to
 * `max` — for a parameter whose maximum is under int64 max, 6.2 clamps and then
 * still fails the check. It is also narrowed to an absent/`b` unit: once a
 * multiplier is involved the C multiply overflows and 6.2 errors too.
 *
 * Two known approximations on the multiply path, both requiring a 19+ digit
 * literal. Real Redis multiplies in 64-bit and wraps, so
 * `9007199254740993mb` is accepted as `1048576` on 6.2, 7.2 and 8.0 alike where
 * the exact arithmetic here range-errors; and on 6.2 a product that wraps
 * negative reports `argument must be a memory value` where this reports the
 * range error. Modelling C's overflow was judged not worth it — see the
 * discussion on PR #409.
 */
function parseMemoryValue(
  profile: CompatibilityProfile,
  name: string,
  raw: string,
  min: bigint,
  max: bigint,
): bigint {
  const match = /^(\d*)([a-zA-Z]*)$/.exec(raw)
  const unit = match
    ? MEMORY_UNITS.get(match[2].toLowerCase() || 'b')
    : undefined
  if (!match || unit === undefined) {
    throw configSetFailed(profile, name, raw, 'argument must be a memory value')
  }

  let literal = match[1] === '' ? 0n : BigInt(match[1])
  if (
    unit === 1n &&
    literal > INT64_MAX &&
    !profile.has('config.memory-value.reject-overflow')
  ) {
    literal = INT64_MAX
  }

  const value = literal * unit
  if (value < min || value > max) {
    throw configSetFailed(
      profile,
      name,
      raw,
      `argument must be between ${min} and ${max} inclusive`,
    )
  }

  return value
}

/**
 * Plausible Redis defaults for the parameters client libraries probe during
 * connection setup. There is no real configuration subsystem behind this — the
 * values exist only so that `CONFIG GET` returns something sane and `CONFIG SET`
 * has a known parameter set to validate against. Extend this map as more
 * parameters are needed; do not treat any value here as authoritative.
 */
const CONFIG_DEFAULTS: Readonly<Record<string, string>> = {
  appendonly: 'no',
  'bind-source-addr': '',
  databases: '16',
  'hash-max-listpack-entries': '128',
  'hash-max-listpack-value': '64',
  'list-max-listpack-size': '128',
  loglevel: 'notice',
  maxclients: '10000',
  maxmemory: '0',
  'maxmemory-clients': '0',
  'maxmemory-policy': 'noeviction',
  'maxmemory-samples': '5',
  save: '3600 1 300 100 60 10000',
  'set-max-intset-entries': '512',
  'set-max-listpack-entries': '128',
  'tcp-keepalive': '300',
  timeout: '0',
  'zset-max-listpack-entries': '128',
  'zset-max-listpack-value': '64',
}

// Per-server backing store, lazily seeded from CONFIG_DEFAULTS. Keyed on the
// server object so concurrent in-process servers (e.g. the mock cluster used in
// tests) never share mutable config state.
const configStores = new WeakMap<object, Map<string, string>>()

function getConfigStore(ctx: RedisExecutionContext): Map<string, string> {
  let store = configStores.get(ctx.server)
  if (store === undefined) {
    store = new Map(Object.entries(CONFIG_DEFAULTS))
    configStores.set(ctx.server, store)
  }
  return store
}

function globMatches(pattern: string, value: string): boolean {
  let source = '^'
  for (let i = 0; i < pattern.length; i++) {
    const char = pattern[i]
    if (char === '*') {
      source += '.*'
    } else if (char === '?') {
      source += '.'
    } else {
      source += char.replace(/[\\^$.*+?()[\]{}|]/g, '\\$&')
    }
  }
  source += '$'
  return new RegExp(source, 'i').test(value)
}

function configGet(
  args: readonly Buffer[],
  ctx: RedisExecutionContext,
): RedisResult {
  if (args.length === 0) {
    throw new WrongNumberOfArgumentsError('config|get')
  }

  const store = getConfigStore(ctx)
  // Overlay server-backed params so CONFIG GET reflects their live values.
  const effective = new Map(store)
  effective.set(KEYSPACE_NOTIFY_PARAM, ctx.server.notifyKeyspaceEvents)
  effective.set(PROTO_MAX_BULK_LEN_PARAM, ctx.server.protoMaxBulkLen.toString())

  const patterns = args.map(arg => arg.toString())
  const matched = new Map<string, string>()
  for (const [name, value] of effective) {
    if (patterns.some(pattern => globMatches(pattern, name))) {
      matched.set(name, value)
    }
  }

  const bulk = (text: string): RedisValue =>
    RedisValue.bulkString(Buffer.from(text))

  const entries: [RedisValue, RedisValue][] = []
  for (const [name, value] of matched) {
    entries.push([bulk(name), bulk(value)])
  }
  return RedisResult.create(RedisValue.map(entries))
}

/**
 * A validated CONFIG SET assignment. Parameters whose value is parsed during
 * validation carry the parsed form through to the apply pass, so nothing is
 * re-derived (and possibly re-thrown) once updates have started landing.
 */
type ConfigUpdate =
  | { name: string; value: string }
  | { name: typeof PROTO_MAX_BULK_LEN_PARAM; bytes: bigint }

function configSet(
  args: readonly Buffer[],
  ctx: RedisExecutionContext,
): RedisResult {
  if (args.length === 0 || args.length % 2 !== 0) {
    throw new WrongNumberOfArgumentsError('config|set')
  }

  const store = getConfigStore(ctx)
  const updates: ConfigUpdate[] = []
  for (let i = 0; i < args.length; i += 2) {
    const name = args[i].toString().toLowerCase()
    const value = args[i + 1].toString()
    if (name === KEYSPACE_NOTIFY_PARAM) {
      // Validate + normalize now so the whole SET aborts before applying any.
      updates.push({ name, value: normalizeKeyspaceNotifyConfig(value) })
      continue
    }
    if (name === PROTO_MAX_BULK_LEN_PARAM) {
      updates.push({
        name,
        bytes: parseMemoryValue(
          ctx.server.profile,
          name,
          value,
          PROTO_MAX_BULK_LEN_MIN,
          PROTO_MAX_BULK_LEN_MAX,
        ),
      })
      continue
    }
    if (!store.has(name)) {
      throw new RedisCommandError(
        `Unknown option or number of arguments for CONFIG SET - '${args[i].toString()}'`,
      )
    }
    updates.push({ name, value })
  }

  // Validate every parameter before applying any — CONFIG SET is atomic.
  for (const update of updates) {
    const { name } = update
    if ('bytes' in update) {
      ctx.server.protoMaxBulkLen = update.bytes
      continue
    }
    const { value } = update
    if (name === KEYSPACE_NOTIFY_PARAM) {
      ctx.server.notifyKeyspaceEvents = value
      continue
    }
    store.set(name, value)
  }
  return ok()
}

function configResetStat(args: readonly Buffer[]): RedisResult {
  if (args.length !== 0) {
    throw new WrongNumberOfArgumentsError('config|resetstat')
  }

  return ok()
}

function configRewrite(args: readonly Buffer[]): RedisResult {
  if (args.length !== 0) {
    throw new WrongNumberOfArgumentsError('config|rewrite')
  }

  throw new RedisCommandError('The server is running without a config file')
}

export const configCommand = defineCommand({
  name: 'config',
  schema: t.object({
    subcommand: t.string(),
    args: t.variadic(t.bulk()),
  }),
  flags: ['admin', 'noscript'],
  monitor: {
    skip: true,
  },
  introspection: {
    arity: -2,
    flags: [],
    firstKey: 0,
    lastKey: 0,
    keyStep: 0,
    categories: ['@admin', '@slow', '@dangerous'],
    keySpecs: [],
    subcommands: [
      commandSubcommandInfo('config|get', -3),
      commandSubcommandInfo('config|set', -4),
      commandSubcommandInfo('config|help', 2),
      commandSubcommandInfo('config|resetstat', 2),
      commandSubcommandInfo('config|rewrite', 2),
    ],
  },
  keys: () => [],
  execute: (args, ctx) => {
    const subcommand = args.subcommand.toLowerCase()

    if (subcommand === 'get') {
      return configGet(args.args, ctx)
    }

    if (subcommand === 'set') {
      return configSet(args.args, ctx)
    }

    if (subcommand === 'resetstat') {
      return configResetStat(args.args)
    }

    if (subcommand === 'rewrite') {
      return configRewrite(args.args)
    }

    throw new RedisCommandError(
      `Unknown CONFIG subcommand or wrong number of arguments for '${args.subcommand}'. Try CONFIG HELP.`,
    )
  },
})

export const configCommands = [configCommand]
