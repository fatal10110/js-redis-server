import { asciiLowerCase } from '../core/ascii-case'
import { defineCommand } from '../core/command-definition'
import { t } from '../core/command-schema'
import type { CompatibilityProfile, FeatureId } from '../core/compatibility'
import { redisGlobMatch } from '../core/glob'
import type { RedisExecutionContext } from '../core/redis-context'
import {
  RedisCommandError,
  WrongNumberOfArgumentsError,
  errors,
} from '../core/redis-error'
import { RedisResult } from '../core/redis-result'
import { RedisValue } from '../core/redis-value'
import {
  INVALID_NOTIFY_FLAG_DETAIL,
  keyspaceNotifyFlagsToString,
  parseKeyspaceNotifyFlags,
  type KeyspaceNotifyFlags,
} from '../state'
import {
  INT64_MAX,
  INT64_MIN,
  ok,
  expectSubcommandArgCount,
  helpReply,
  subcommandSyntaxError,
  unknownSubcommandError,
} from './helpers'
import { configHelpLines } from './container-help'
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
// `constructor` (already lower-case, so case folding does not save us)
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
 * One CONFIG SET name as resolved against the profile's parameter table:
 * `sent` is the name exactly as the client typed it, `name` the table spelling
 * it matched — the parameter's own name, or the alias the client used (Redis
 * 7.0 registers each alias as a config of its own, so `hash-max-ziplist-
 * entries` never turns into `hash-max-listpack-entries`).
 */
interface ResolvedConfigName {
  sent: string
  name: string
  param: ConfigParam
}

/**
 * CONFIG SET's failure wording, which Redis 7.0 changed wholesale when it
 * rewrote the subcommand to accept several parameter pairs:
 *
 *   6.2   ERR Invalid argument '<value>' for CONFIG SET '<sent>'[ - <detail>]
 *   7.0+  ERR CONFIG SET failed (possibly related to argument '<name>') - <detail>
 *
 * 6.2 echoes the parameter as the client sent it (`'TIMEOUT'`); 7.0+ echoes
 * the table spelling it matched (`set_configs[i]->name`), so `TIMEOUT` comes
 * back as `timeout` and `Hash-Max-Ziplist-Entries` as `hash-max-ziplist-
 * entries`. 6.2 only appends the detail for parameters on its typed config
 * table; hand-parsed ones such as `notify-keyspace-events` and `save` fail
 * through a bare `badfmt`, so pass `legacyDetail: false` for those.
 *
 * Invalid-value failures go through this helper and unknown parameters through
 * {@link configSetUnknownParameter}. Repeated and immutable names use
 * {@link configSetDuplicateParameter} / {@link configSetImmutableParameter},
 * which echo the name as sent, and shape/arity errors come from
 * {@link checkConfigSetNames}.
 */
function configSetFailed(
  profile: CompatibilityProfile,
  target: ResolvedConfigName,
  value: string,
  detail: string,
  { legacyDetail = true }: { legacyDetail?: boolean } = {},
): RedisCommandError {
  if (!profile.has('config.set.failure-message')) {
    const suffix = legacyDetail ? ` - ${detail}` : ''
    return new RedisCommandError(
      `Invalid argument '${value}' for CONFIG SET '${target.sent}'${suffix}`,
    )
  }

  return new RedisCommandError(
    `CONFIG SET failed (possibly related to argument '${target.name}') - ${detail}`,
  )
}

/**
 * CONFIG SET's unknown-parameter wording, changed by the same 7.0 rewrite.
 * Both echo the name as the client sent it:
 *
 *   6.2   ERR Unsupported CONFIG parameter: <name>
 *   7.0+  ERR Unknown option or number of arguments for CONFIG SET - '<name>'
 */
function configSetUnknownParameter(
  profile: CompatibilityProfile,
  name: string,
): RedisCommandError {
  if (!profile.has('config.set.failure-message')) {
    return new RedisCommandError(`Unsupported CONFIG parameter: ${name}`)
  }

  return new RedisCommandError(
    `Unknown option or number of arguments for CONFIG SET - '${name}'`,
  )
}

function configSetOutOfRange(
  profile: CompatibilityProfile,
  target: ResolvedConfigName,
  value: string,
  min: bigint,
  max: bigint,
): RedisCommandError {
  return configSetFailed(
    profile,
    target,
    value,
    `argument must be between ${min} and ${max} inclusive`,
  )
}

/**
 * A Redis memory value (`1048576`, `1mb`, `512MB`, ...) in bytes, or
 * `undefined` when `raw` is not one (`memtoull` / 6.2's `memtoll`).
 *
 * Empty input is *not* a parse failure: Redis' `memtoull` reads it as 0, which
 * then fails the caller's range check instead.
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
function memoryBytes(
  profile: CompatibilityProfile,
  raw: string,
): bigint | undefined {
  const match = /^(\d*)([a-zA-Z]*)$/.exec(raw)
  const unit = match
    ? MEMORY_UNITS.get(asciiLowerCase(match[2]) || 'b')
    : undefined
  if (!match || unit === undefined) {
    return undefined
  }

  let literal = match[1] === '' ? 0n : BigInt(match[1])
  if (
    unit === 1n &&
    literal > INT64_MAX &&
    !profile.has('config.memory-value.reject-overflow')
  ) {
    literal = INT64_MAX
  }

  return literal * unit
}

/**
 * Parse a memory value and range-check it against a parameter's bounds,
 * reproducing CONFIG SET's two distinct failure messages.
 */
function parseMemoryValue(
  profile: CompatibilityProfile,
  target: ResolvedConfigName,
  raw: string,
  min: bigint,
  max: bigint,
): bigint {
  const value = memoryBytes(profile, raw)
  if (value === undefined) {
    throw configSetFailed(
      profile,
      target,
      raw,
      'argument must be a memory value',
    )
  }
  if (value < min || value > max) {
    throw configSetOutOfRange(profile, target, raw, min, max)
  }
  return value
}

/**
 * Redis' `string2ll`: an optional `-`, no `+`, no leading zeros or `-0`, no
 * whitespace, and the signed 64-bit range. `undefined` when it fails.
 */
function string2ll(text: string): bigint | undefined {
  if (!/^(?:0|-?[1-9][0-9]*)$/.test(text)) {
    return undefined
  }
  const value = BigInt(text)
  return value < INT64_MIN || value > INT64_MAX ? undefined : value
}

/**
 * C `strtoll` as the `save` parser uses it: leading whitespace, a sign, then
 * digits up to the end (`*eptr == '\0'`), saturating on overflow. An empty
 * token is 0 (no digits, but `eptr` already sits on the terminator); a token
 * of whitespace only is not a number.
 */
function strtoll(token: string): bigint | undefined {
  if (token === '') {
    return 0n
  }
  const match = /^[ \t\n\v\f\r]*([+-]?[0-9]+)$/.exec(token)
  if (!match) {
    return undefined
  }
  const value = BigInt(match[1])
  return value > INT64_MAX ? INT64_MAX : value < INT64_MIN ? INT64_MIN : value
}

/**
 * The `save` parameter: `<seconds> <changes>` pairs, seconds at least 1 and
 * changes at least 0 (`setConfigSaveOption`; 6.2's special-cased `save`).
 * 7.0+ splits the value like a command line (`sdssplitargs`), 6.2 on single
 * spaces (`sdssplitlen`), so `"3600  1"` is three tokens there. Returns the
 * value `CONFIG GET save` reports: the pairs re-printed, changes as a C `int`.
 */
function parseSaveParams(
  profile: CompatibilityProfile,
  target: ResolvedConfigName,
  value: string,
): string {
  const tokens = !profile.has('config.set.failure-message')
    ? value === ''
      ? []
      : value.split(' ')
    : value.split(/[ \t\n\v\f\r]+/).filter(token => token !== '')
  const invalid = (): RedisCommandError =>
    configSetFailed(profile, target, value, 'Invalid save parameters', {
      legacyDetail: false,
    })
  if (tokens.length % 2 !== 0) {
    throw invalid()
  }

  const pairs: string[] = []
  for (let j = 0; j < tokens.length; j += 2) {
    const seconds = strtoll(tokens[j])
    const changes = strtoll(tokens[j + 1])
    if (
      seconds === undefined ||
      changes === undefined ||
      seconds < 1n ||
      changes < 0n
    ) {
      throw invalid()
    }
    pairs.push(`${seconds} ${BigInt.asIntN(32, changes)}`)
  }
  return pairs.join(' ')
}

const INT_MIN = -2147483648n
const INT_MAX = 2147483647n
const UINT_MAX = 4294967295n
const ULLONG_MAX = 18446744073709551615n

/**
 * How CONFIG SET parses and normalizes a parameter's value, after the typed
 * configs of Redis' config.c (`createBoolConfig`, `createIntConfig`, ...).
 * Integer bounds are the C type's, so `timeout -1` is out of range rather
 * than unparsable on every version.
 */
type ConfigKind =
  | { type: 'bool' }
  | {
      type: 'int'
      min: bigint
      max: (profile: CompatibilityProfile) => bigint
    }
  | { type: 'memory'; min: bigint; max: bigint }
  // `MEMORY_CONFIG | PERCENT_CONFIG`: bytes, or `<n>%` stored as -n.
  | { type: 'memory-or-percent' }
  | {
      type: 'enum'
      values: (profile: CompatibilityProfile) => readonly string[]
    }
  | { type: 'save' }
  | { type: 'string' }
  // Hand-parsed, and backed by server state rather than the config store.
  | { type: 'notify-keyspace-events' }
  | { type: 'proto-max-bulk-len' }

interface ConfigParam {
  /** The name on Redis 7.0+ and Valkey, and the config store's key. */
  name: string
  /**
   * The 6.2 name of a parameter Redis 7.0 renamed from `ziplist` to `listpack`.
   * 7.0+ keeps it as an alias; 6.2 knows the parameter by this name only.
   */
  alias?: string
  defaultValue: string
  kind: ConfigKind
  /** `IMMUTABLE_CONFIG`: set at startup only. */
  immutable?: boolean
  /** The feature gate of the release that added the parameter. */
  since?: FeatureId
}

const BOOL: ConfigKind = { type: 'bool' }
const STRING: ConfigKind = { type: 'string' }

function intKind(
  min: bigint,
  max: bigint | ((profile: CompatibilityProfile) => bigint),
): ConfigKind {
  return { type: 'int', min, max: typeof max === 'bigint' ? () => max : max }
}

// The enum lists in their config.c order, which the failure detail lists.
const LOGLEVELS = ['debug', 'verbose', 'notice', 'warning'] as const
const MAXMEMORY_POLICIES = [
  'volatile-lru',
  'volatile-lfu',
  'volatile-random',
  'volatile-ttl',
  'allkeys-lru',
  'allkeys-lfu',
  'allkeys-random',
  'noeviction',
] as const

/**
 * The parameters the mock knows, with the real servers' defaults and types
 * (redis 6.2.14 .. 8.0.0 and valkey 8.0.0 / 9.0.0 config.c). Only CONFIG GET
 * and CONFIG SET read them; `set-max-intset-entries`, `notify-keyspace-events`
 * and `proto-max-bulk-len` also drive behavior. Names outside the table are
 * unknown, as they would be to a real server without them.
 */
const CONFIG_PARAMS: readonly ConfigParam[] = [
  { name: 'appendonly', defaultValue: 'no', kind: BOOL },
  {
    name: 'bind-source-addr',
    defaultValue: '',
    kind: STRING,
    since: 'config.redis7-table',
  },
  { name: 'daemonize', defaultValue: 'no', kind: BOOL, immutable: true },
  {
    name: 'databases',
    defaultValue: '16',
    kind: intKind(1n, INT_MAX),
    immutable: true,
  },
  {
    name: 'hash-max-listpack-entries',
    alias: 'hash-max-ziplist-entries',
    defaultValue: '512',
    kind: intKind(0n, INT64_MAX),
  },
  {
    name: 'hash-max-listpack-value',
    alias: 'hash-max-ziplist-value',
    defaultValue: '64',
    kind: { type: 'memory', min: 0n, max: INT64_MAX },
  },
  {
    name: 'list-max-listpack-size',
    alias: 'list-max-ziplist-size',
    defaultValue: '-2',
    kind: intKind(INT_MIN, INT_MAX),
  },
  {
    name: 'loglevel',
    defaultValue: 'notice',
    kind: {
      type: 'enum',
      values: profile =>
        profile.has('config.loglevel-nothing')
          ? [...LOGLEVELS, 'nothing']
          : LOGLEVELS,
    },
  },
  { name: 'maxclients', defaultValue: '10000', kind: intKind(1n, UINT_MAX) },
  {
    name: 'maxmemory',
    defaultValue: '0',
    kind: { type: 'memory', min: 0n, max: ULLONG_MAX },
  },
  {
    name: 'maxmemory-clients',
    defaultValue: '0',
    kind: { type: 'memory-or-percent' },
    since: 'config.redis7-table',
  },
  {
    name: 'maxmemory-policy',
    defaultValue: 'noeviction',
    kind: { type: 'enum', values: () => MAXMEMORY_POLICIES },
  },
  {
    name: 'maxmemory-samples',
    defaultValue: '5',
    kind: intKind(1n, profile =>
      profile.has('config.maxmemory-samples-max-64') ? 64n : INT_MAX,
    ),
  },
  {
    name: KEYSPACE_NOTIFY_PARAM,
    defaultValue: '',
    kind: { type: 'notify-keyspace-events' },
  },
  {
    name: PROTO_MAX_BULK_LEN_PARAM,
    defaultValue: '',
    kind: { type: 'proto-max-bulk-len' },
  },
  {
    name: 'save',
    defaultValue: '3600 1 300 100 60 10000',
    kind: { type: 'save' },
  },
  {
    name: 'set-max-intset-entries',
    defaultValue: '512',
    kind: intKind(0n, INT64_MAX),
  },
  {
    name: 'set-max-listpack-entries',
    defaultValue: '128',
    kind: intKind(0n, INT64_MAX),
    since: 'set.listpack-encoding',
  },
  {
    name: 'set-max-listpack-value',
    defaultValue: '64',
    kind: intKind(0n, INT64_MAX),
    since: 'set.listpack-encoding',
  },
  { name: 'tcp-keepalive', defaultValue: '300', kind: intKind(0n, INT_MAX) },
  { name: 'timeout', defaultValue: '0', kind: intKind(0n, INT_MAX) },
  {
    name: 'zset-max-listpack-entries',
    alias: 'zset-max-ziplist-entries',
    defaultValue: '128',
    kind: intKind(0n, INT64_MAX),
  },
  {
    name: 'zset-max-listpack-value',
    alias: 'zset-max-ziplist-value',
    defaultValue: '64',
    kind: { type: 'memory', min: 0n, max: INT64_MAX },
  },
]

interface ConfigName {
  name: string
  param: ConfigParam
}

/**
 * Every parameter name the profile knows, each a key of its own, in table
 * order (a parameter, then its alias). Redis 6.2 has the `ziplist` names
 * only; 7.0+ the `listpack` names with the `ziplist` ones as aliases.
 */
function configNames(profile: CompatibilityProfile): Map<string, ConfigName> {
  const renamed = profile.has('config.redis7-table')
  const names = new Map<string, ConfigName>()
  for (const param of CONFIG_PARAMS) {
    if (param.since !== undefined && !profile.has(param.since)) {
      continue
    }
    if (param.alias === undefined || renamed) {
      names.set(param.name, { name: param.name, param })
    }
    if (param.alias !== undefined) {
      names.set(param.alias, { name: param.alias, param })
    }
  }
  return names
}

// Per-server backing store, lazily seeded from CONFIG_PARAMS. Keyed on the
// server object so concurrent in-process servers (e.g. the mock cluster used in
// tests) never share mutable config state.
const configStores = new WeakMap<object, Map<string, string>>()

function getConfigStore(ctx: RedisExecutionContext): Map<string, string> {
  return configStoreFor(ctx.server)
}

function configStoreFor(server: object): Map<string, string> {
  let store = configStores.get(server)
  if (store === undefined) {
    store = new Map(
      CONFIG_PARAMS.map(param => [param.name, param.defaultValue]),
    )
    configStores.set(server, store)
  }
  return store
}

const SET_MAX_INTSET_ENTRIES_DEFAULT = 512

/**
 * The live `set-max-intset-entries`, which decides when a set stops being an
 * intset (#504). CONFIG SET only stores a validated, normalized integer; the
 * guard is belt and braces.
 */
export function configuredSetMaxIntsetEntries(server: object): number {
  const raw = configStoreFor(server).get('set-max-intset-entries') ?? ''
  if (!/^[0-9]+$/.test(raw)) {
    return SET_MAX_INTSET_ENTRIES_DEFAULT
  }
  return Number(raw)
}

/**
 * The live `zset-max-listpack-entries` / `zset-max-listpack-value`, which
 * decide whether a sorted set is a listpack (#512).
 */
export function configuredZsetListpackLimits(server: object): {
  maxEntries: number
  maxValue: number
} {
  const store = configStoreFor(server)
  const read = (name: string, fallback: number): number => {
    const raw = store.get(name) ?? ''
    return /^[0-9]+$/.test(raw) ? Number(raw) : fallback
  }
  return {
    maxEntries: read('zset-max-listpack-entries', 128),
    maxValue: read('zset-max-listpack-value', 64),
  }
}

function configValue(ctx: RedisExecutionContext, param: ConfigParam): string {
  switch (param.kind.type) {
    case 'notify-keyspace-events':
      return keyspaceNotifyFlagsToString(ctx.server.notifyKeyspaceEvents)
    case 'proto-max-bulk-len':
      return ctx.server.protoMaxBulkLen.toString()
    default:
      return getConfigStore(ctx).get(param.name) ?? param.defaultValue
  }
}

/**
 * CONFIG GET. Redis 6.2 takes exactly one pattern and answers any other count
 * with its subcommand syntax error (config.c:2599); 7.0+ takes several.
 *
 * Patterns are globs matched ASCII case-insensitively against every name,
 * aliases included, and reply with the table spelling. 7.0+ looks a pattern
 * without `[`, `*` or `?` up directly and replies with the name exactly as
 * sent (`CONFIG GET MAXMEMORY` -> `MAXMEMORY`), where 6.2 globs it too and
 * replies `maxmemory`. 7.0+ collects matches in a case-insensitive dict, so a
 * name asked for twice comes back once.
 */
function configGet(
  subcommand: Buffer,
  args: readonly Buffer[],
  ctx: RedisExecutionContext,
): RedisResult {
  const { profile } = ctx.server
  const multiPattern = profile.has('config.get.multi-pattern')
  expectSubcommandArgCount(
    'CONFIG',
    subcommand,
    args,
    multiPattern ? { min: 1 } : 1,
    profile,
  )

  const names = configNames(profile)
  const matched = new Map<string, { reply: Buffer; param: ConfigParam }>()
  for (const arg of args) {
    const folded = asciiLowerCase(arg.toString('latin1'))
    if (multiPattern && !/[[*?]/.test(folded)) {
      const entry = names.get(folded)
      if (entry !== undefined && !matched.has(folded)) {
        matched.set(folded, { reply: arg, param: entry.param })
      }
      continue
    }

    const pattern = Buffer.from(folded, 'latin1')
    for (const [name, entry] of names) {
      if (!matched.has(name) && redisGlobMatch(pattern, Buffer.from(name))) {
        matched.set(name, { reply: Buffer.from(name), param: entry.param })
      }
    }
  }

  const entries: [RedisValue, RedisValue][] = []
  for (const { reply, param } of matched.values()) {
    entries.push([
      RedisValue.bulkString(reply),
      RedisValue.bulkString(Buffer.from(configValue(ctx, param))),
    ])
  }
  return RedisResult.create(RedisValue.map(entries))
}

/**
 * A validated CONFIG SET assignment. Parameters whose value is parsed during
 * validation carry the parsed form through to the apply pass, so nothing is
 * re-derived (and possibly re-thrown) once updates have started landing.
 */
type ConfigUpdate =
  | { kind: 'store'; name: string; value: string }
  | { kind: 'proto-max-bulk-len'; bytes: bigint }
  | { kind: 'notify-keyspace-events'; flags: KeyspaceNotifyFlags }

/**
 * CONFIG SET's repeated-parameter failure. The `CONFIG SET failed` wrapper of
 * {@link configSetFailed}, but only ever reachable on 7.0+ profiles (6.2 never
 * accepts a second pair), and it echoes the repeat exactly as the client sent
 * it — real Redis reports `argv` here, not the table name its value
 * failures report. A parameter and its alias are not repeats: 7.0 registers
 * the alias as a config of its own. Captured from 7.0.15, 8.0.6 and Valkey
 * 7.2.14:
 *
 *   CONFIG SET timeout 0 TIMEOUT 0
 *     -> ERR CONFIG SET failed (possibly related to argument 'TIMEOUT') - duplicate parameter
 */
function configSetDuplicateParameter(name: string): RedisCommandError {
  return new RedisCommandError(
    `CONFIG SET failed (possibly related to argument '${name}') - duplicate parameter`,
  )
}

/**
 * 7.0+'s answer for an `IMMUTABLE_CONFIG` parameter (`databases`,
 * `daemonize`), echoing the name as sent. 6.2 has no such reply: its CONFIG
 * SET only knows modifiable parameters, so an immutable one is unsupported.
 */
function configSetImmutableParameter(name: string): RedisCommandError {
  return new RedisCommandError(
    `CONFIG SET failed (possibly related to argument '${name}') - can't set immutable config`,
  )
}

/**
 * CONFIG SET's shape and name checks, run before any value is looked at.
 *
 * Redis 6.2 dispatches SET only for exactly one pair; every other shape falls
 * through to the legacy subcommand syntax error. 7.0+ (`config.set.multi-pair`)
 * takes any number of pairs, and — like its `configSetCommand` — resolves every
 * name in one pass before validating a value: the first unknown, immutable or
 * repeated name in argument order is the error, so a bad value never masks
 * any of them. Names fold ASCII only, like C `strcasecmp` (#508).
 */
function checkConfigSetNames(
  subcommand: Buffer,
  args: readonly Buffer[],
  ctx: RedisExecutionContext,
): ResolvedConfigName[] {
  const { profile } = ctx.server
  if (!profile.has('config.set.multi-pair')) {
    if (args.length !== 2) {
      throw subcommandSyntaxError('CONFIG', subcommand, profile)
    }
  } else if (args.length < 2) {
    throw new WrongNumberOfArgumentsError('config|set')
  } else if (args.length % 2 !== 0) {
    throw errors.syntax()
  }

  const names = configNames(profile)
  const immutableError = profile.has('config.set.failure-message')
  const resolved: ResolvedConfigName[] = []
  const seen = new Set<string>()
  for (let i = 0; i < args.length; i += 2) {
    const sent = args[i].toString()
    const entry = names.get(asciiLowerCase(sent))
    if (entry === undefined || (entry.param.immutable && !immutableError)) {
      throw configSetUnknownParameter(profile, sent)
    }
    if (entry.param.immutable) {
      throw configSetImmutableParameter(sent)
    }
    if (seen.has(entry.name)) {
      throw configSetDuplicateParameter(sent)
    }
    seen.add(entry.name)
    resolved.push({ sent, name: entry.name, param: entry.param })
  }
  return resolved
}

/** `MEMORY_CONFIG | PERCENT_CONFIG` (`maxmemory-clients`), 7.0+ only. */
function parseMemoryOrPercent(
  profile: CompatibilityProfile,
  target: ResolvedConfigName,
  value: string,
): string {
  let parsed: bigint | undefined
  const bytes = memoryBytes(profile, value)
  if (bytes !== undefined && bytes <= ULLONG_MAX) {
    // `memtoull`'s result lands in a `long long`.
    parsed = BigInt.asIntN(64, bytes)
  } else if (value.length > 1 && value.endsWith('%')) {
    const percent = string2ll(value.slice(0, -1))
    if (percent !== undefined && percent >= 0n) {
      parsed = -percent
    }
  }
  if (parsed === undefined) {
    throw configSetFailed(
      profile,
      target,
      value,
      'argument must be a memory or percent value',
    )
  }
  if (parsed < 0n) {
    if (parsed < -100n) {
      throw configSetFailed(
        profile,
        target,
        value,
        'percentage argument must be less or equal to 100',
      )
    }
    return `${-parsed}%`
  }
  return parsed.toString()
}

/** Validate a stored parameter's value; returns what CONFIG GET reports. */
function normalizeConfigValue(
  profile: CompatibilityProfile,
  target: ResolvedConfigName,
  value: string,
): string {
  const { kind } = target.param
  switch (kind.type) {
    case 'bool': {
      const folded = asciiLowerCase(value)
      if (folded !== 'yes' && folded !== 'no') {
        throw configSetFailed(
          profile,
          target,
          value,
          "argument must be 'yes' or 'no'",
        )
      }
      return folded
    }
    case 'int': {
      const parsed = string2ll(value)
      if (parsed === undefined) {
        throw configSetFailed(
          profile,
          target,
          value,
          "argument couldn't be parsed into an integer",
        )
      }
      const max = kind.max(profile)
      if (parsed < kind.min || parsed > max) {
        throw configSetOutOfRange(profile, target, value, kind.min, max)
      }
      return parsed.toString()
    }
    case 'memory':
      return parseMemoryValue(
        profile,
        target,
        value,
        kind.min,
        kind.max,
      ).toString()
    case 'memory-or-percent':
      return parseMemoryOrPercent(profile, target, value)
    case 'enum': {
      const values = kind.values(profile)
      const folded = asciiLowerCase(value)
      const match = values.find(candidate => candidate === folded)
      if (match === undefined) {
        const lead = profile.has('config.set.failure-message')
          ? 'argument(s) must be'
          : 'argument must be'
        throw configSetFailed(
          profile,
          target,
          value,
          `${lead} one of the following: ${values.join(', ')}`,
        )
      }
      return match
    }
    case 'save':
      return parseSaveParams(profile, target, value)
    case 'string':
      return value
    case 'notify-keyspace-events':
    case 'proto-max-bulk-len':
      throw new Error(`${target.param.name} is not a stored parameter`)
  }
}

function parseConfigUpdate(
  profile: CompatibilityProfile,
  target: ResolvedConfigName,
  value: string,
): ConfigUpdate {
  switch (target.param.kind.type) {
    case 'notify-keyspace-events': {
      const flags = parseKeyspaceNotifyFlags(value, {
        newKeyClass: profile.has('notify.keyspace.new-key-class'),
      })
      if (!flags) {
        throw configSetFailed(
          profile,
          target,
          value,
          INVALID_NOTIFY_FLAG_DETAIL,
          { legacyDetail: false },
        )
      }
      return { kind: KEYSPACE_NOTIFY_PARAM, flags }
    }
    case 'proto-max-bulk-len':
      return {
        kind: PROTO_MAX_BULK_LEN_PARAM,
        bytes: parseMemoryValue(
          profile,
          target,
          value,
          PROTO_MAX_BULK_LEN_MIN,
          PROTO_MAX_BULK_LEN_MAX,
        ),
      }
    default:
      return {
        kind: 'store',
        name: target.param.name,
        value: normalizeConfigValue(profile, target, value),
      }
  }
}

function configSet(
  subcommand: Buffer,
  args: readonly Buffer[],
  ctx: RedisExecutionContext,
): RedisResult {
  const { profile } = ctx.server
  const targets = checkConfigSetNames(subcommand, args, ctx)

  // Validate every parameter, in argument order, before applying any —
  // CONFIG SET is atomic.
  const updates = targets.map((target, index) =>
    parseConfigUpdate(profile, target, args[index * 2 + 1].toString()),
  )

  const store = getConfigStore(ctx)
  for (const update of updates) {
    switch (update.kind) {
      case 'proto-max-bulk-len':
        ctx.server.protoMaxBulkLen = update.bytes
        break
      case 'notify-keyspace-events':
        ctx.server.notifyKeyspaceEvents = update.flags
        break
      case 'store':
        store.set(update.name, update.value)
        break
    }
  }
  return ok()
}

function configResetStat(
  subcommand: Buffer,
  args: readonly Buffer[],
  ctx: RedisExecutionContext,
): RedisResult {
  expectSubcommandArgCount('CONFIG', subcommand, args, 0, ctx.server.profile)
  return ok()
}

function configRewrite(
  subcommand: Buffer,
  args: readonly Buffer[],
  ctx: RedisExecutionContext,
): RedisResult {
  expectSubcommandArgCount('CONFIG', subcommand, args, 0, ctx.server.profile)

  throw new RedisCommandError('The server is running without a config file')
}

export const configCommand = defineCommand({
  name: 'config',
  schema: t.object({
    // Raw bytes, not `t.string()`: the unknown-subcommand reply echoes the
    // name the client sent, and a UTF-8 decode here would lose its bytes.
    subcommand: t.bulk(),
    args: t.variadic(t.bulk()),
  }),
  flags: ['admin', 'noscript'],
  introspection: {
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
    const subcommand = asciiLowerCase(args.subcommand.toString())

    if (subcommand === 'get') {
      return configGet(args.subcommand, args.args, ctx)
    }

    if (subcommand === 'set') {
      return configSet(args.subcommand, args.args, ctx)
    }

    if (subcommand === 'resetstat') {
      return configResetStat(args.subcommand, args.args, ctx)
    }

    if (subcommand === 'rewrite') {
      return configRewrite(args.subcommand, args.args, ctx)
    }

    if (subcommand === 'help') {
      expectSubcommandArgCount(
        'CONFIG',
        args.subcommand,
        args.args,
        0,
        ctx.server.profile,
      )
      return helpReply(configHelpLines(), ctx.server.profile)
    }

    // Version-specific wording: Redis 7.0 moved container commands into the
    // command table and changed this template, so the reply is gated on
    // `error.unknown-subcommand-wording` rather than hard-coded. Captured from
    // real servers (6.2.24, 7.0.15, 8.0.6) and pinned by
    // tests-integration/raw-tcp/command-errors-config.test.ts plus the profile
    // sweep in tests-integration/compatibility/profile-gates.test.ts (#388).
    throw unknownSubcommandError('CONFIG', args.subcommand, ctx.server.profile)
  },
})

export const configCommands = [configCommand]
