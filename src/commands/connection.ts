import { asciiLowerCase, equalsAscii } from '../core/ascii-case'
import { defineCommand } from '../core/command-definition'
import { isIntegerToken, t } from '../core/command-schema'
import type {
  RedisClientSession,
  RedisExecutionContext,
} from '../core/redis-context'
import {
  NoAuthError,
  RedisCommandError,
  UnknownSubcommandError,
  WrongNumberOfArgumentsError,
  errors,
} from '../core/redis-error'
import { RedisResult } from '../core/redis-result'
import { RedisValue } from '../core/redis-value'
import {
  array,
  bulk,
  integer,
  ok,
  parseInt64Token,
  simpleString,
  unknownSubcommandError,
  expectSubcommandArgCount,
  helpReply,
  subcommandSyntaxError,
} from './helpers'
import { commandDocs, commandSubcommandInfo } from './introspection'
import {
  aclHelpLines,
  clientHelpLines,
  slowlogHelpLines,
} from './container-help'
import { isClientBlocked } from './blocking'
import { resetClientReplyMode, setClientReplyMode } from '../core/client-reply'
import type { ClientPauseMode } from '../state/client-pause'
import {
  VALKEY_REDIS_COMPAT_VERSION,
  type CompatibilityProfile,
} from '../core/compatibility/profile'
import { dbIndexSchema } from './db-index'

const MASTER_REPLID = '0000000000000000000000000000000000000000'

// There is no persistence, so LASTSAVE reports the process/server start time.
const serverStartTimeSeconds = Math.floor(Date.now() / 1000)

const clientIds = new WeakMap<RedisClientSession, number>()
const clientNames = new WeakMap<RedisClientSession, Buffer>()
const clientLibraryNames = new WeakMap<RedisClientSession, Buffer>()
const clientLibraryVersions = new WeakMap<RedisClientSession, Buffer>()
const noEvictClients = new WeakSet<RedisClientSession>()
const noTouchClients = new WeakSet<RedisClientSession>()
let nextClientId = 1
const INVALID_CLIENT_NAME_MESSAGE =
  'Client names cannot contain spaces, newlines or special characters.'

function getClientId(session: RedisClientSession): number {
  const existing = clientIds.get(session)
  if (existing !== undefined) {
    return existing
  }

  const id = nextClientId++
  clientIds.set(session, id)
  return id
}

function assertValidClientName(name: Buffer): void {
  for (const byte of name) {
    if (byte >= 0x21 && byte <= 0x7e) {
      continue
    }

    throw new RedisCommandError(INVALID_CLIENT_NAME_MESSAGE)
  }
}

function setClientName(session: RedisClientSession, name: Buffer): void {
  assertValidClientName(name)

  if (name.length === 0) {
    clientNames.delete(session)
    return
  }

  clientNames.set(session, name)
}

function redisMode(ctx: RedisExecutionContext): string {
  return ctx.server.clusterEnabled ? 'cluster' : 'standalone'
}

function value(value: string): RedisValue {
  return RedisValue.bulkString(Buffer.from(value))
}

/**
 * INFO, CLIENT INFO and CLIENT LIST answer with `addReplyVerbatim(..., "txt")`
 * on every version: a RESP3 verbatim string (`=<len>\r\ntxt:...`), which
 * RESP2 sends as a plain bulk string.
 */
function verbatimText(text: string): RedisResult {
  return RedisResult.create(RedisValue.verbatim('txt', Buffer.from(text)))
}

function expectArgCount(
  commandName: string,
  args: readonly Buffer[],
  count: number,
) {
  if (args.length !== count) {
    throw new WrongNumberOfArgumentsError(commandName)
  }
}

function buildInfo(
  ctx: RedisExecutionContext,
  sections: readonly string[] = [],
): string {
  const requestedSections =
    sections.length === 0
      ? ['default']
      : sections.map(section => asciiLowerCase(section))
  const clustered = ctx.server.clusterEnabled
  const defaultSections = [
    'server',
    'clients',
    'memory',
    'persistence',
    'stats',
    'replication',
    'cpu',
    'cluster',
    'keyspace',
  ]
  const sectionBuilders: Record<string, () => string[]> = {
    server: () => [
      '# Server',
      ...serverIdentityLines(ctx),
      'redis_git_sha1:00000000',
      'redis_git_dirty:0',
      `redis_mode:${redisMode(ctx)}`,
      'os:mocked redis',
      'arch_bits:64',
      'multiplexing_api:mock',
      'process_id:1',
      'tcp_port:6379',
      'uptime_in_seconds:0',
      'uptime_in_days:0',
    ],
    clients: () => [
      '# Clients',
      'connected_clients:1',
      'client_recent_max_input_buffer:0',
      'client_recent_max_output_buffer:0',
      `blocked_clients:${
        ctx.server.getConnectedClients().filter(isClientBlocked).length
      }`,
    ],
    memory: () => [
      '# Memory',
      'used_memory:0',
      'used_memory_human:0B',
      'used_memory_rss:0',
      'used_memory_peak:0',
    ],
    persistence: () => [
      '# Persistence',
      'loading:0',
      'async_loading:0',
      'rdb_bgsave_in_progress:0',
      'aof_enabled:0',
    ],
    stats: () => [
      '# Stats',
      'total_connections_received:1',
      'total_commands_processed:0',
      'instantaneous_ops_per_sec:0',
      'rejected_connections:0',
      'expired_keys:0',
      'evicted_keys:0',
      'keyspace_hits:0',
      'keyspace_misses:0',
    ],
    replication: () => [
      '# Replication',
      'role:master',
      'connected_slaves:0',
      'master_failover_state:no-failover',
      `master_replid:${MASTER_REPLID}`,
      'master_repl_offset:0',
      'second_repl_offset:-1',
      'repl_backlog_active:0',
    ],
    cpu: () => [
      '# CPU',
      'used_cpu_sys:0.00',
      'used_cpu_user:0.00',
      'used_cpu_sys_children:0.00',
      'used_cpu_user_children:0.00',
    ],
    // Only `cluster_enabled`, on a cluster node too: the cluster state is
    // CLUSTER INFO's.
    cluster: () => ['# Cluster', `cluster_enabled:${clustered ? 1 : 0}`],
    commandstats: () => ['# Commandstats'],
    latencystats: () => ['# Latencystats'],
    errorstats: () => ['# Errorstats'],
    modules: () => ['# Modules'],
    sentinel: () => ['# Sentinel'],
    keyspace: () => {
      const lines = ['# Keyspace']
      const profile = ctx.server.profile
      for (const [index, database] of ctx.server.databases.entries()) {
        const stats = database.keyspaceStats()
        if (stats.keys === 0) {
          continue
        }

        let line = `db${index}:keys=${stats.keys},expires=${stats.expires},avg_ttl=${stats.avgTtlMs}`
        if (profile.has('info.keyspace.subexpiry')) {
          line += `,subexpiry=${stats.hashesWithFieldTtl}`
        } else if (profile.has('info.keyspace.volatile-items')) {
          line += `,keys_with_volatile_items=${stats.hashesWithFieldTtl}`
        }
        lines.push(line)
      }
      return lines
    },
  }

  const selectedSections: string[] = []
  for (const requestedSection of requestedSections) {
    if (requestedSection === 'default' || requestedSection === 'all') {
      selectedSections.push(...defaultSections)
      continue
    }

    if (requestedSection in sectionBuilders) {
      selectedSections.push(requestedSection)
    }
  }

  const lines: string[] = []
  for (const selectedSection of selectedSections) {
    if (lines.length > 0) {
      lines.push('')
    }

    lines.push(...sectionBuilders[selectedSection]())
  }

  // Real Redis replies with an empty bulk string for an unrecognized section
  // name rather than an error (clients like ioredis probe with arbitrary
  // section names during connection setup).
  if (lines.length === 0) {
    return ''
  }

  return `${lines.join('\r\n')}\r\n`
}

/**
 * One CLIENT LIST / CLIENT INFO line, field for field as `catClientInfoString`
 * prints it for the profile's version (see the `client.list.*` gates). The
 * buffer and memory counters (`qbuf`, `rbs`, `tot-mem`, ...) and the network
 * counters have nothing to measure here and read 0.
 */
function formatClientLine(
  session: RedisClientSession,
  profile: CompatibilityProfile,
): string {
  const name = clientNames.get(session)?.toString() ?? ''
  const redis7 = profile.has('client.list.redis7-fields')
  const fields = [
    `id=${getClientId(session)}`,
    `addr=${clientPeerAddress(session)}`,
    `laddr=${clientLocalAddress(session)}`,
    `fd=${session.fd ?? -1}`,
    `name=${name}`,
    `age=${clientAgeSeconds(session)}`,
    `idle=${clientIdleSeconds(session)}`,
    `flags=${clientFlags(session)}`,
  ]
  if (profile.has('client.list.capa')) {
    fields.push('capa=')
  }
  fields.push(
    `db=${session.selectedDatabase}`,
    `sub=${session.pubsubChannelCount}`,
    `psub=${session.pubsubPatternCount}`,
  )
  if (redis7) {
    fields.push(`ssub=${session.pubsubShardChannelCount}`)
  }
  fields.push(`multi=${session.queuedCommandCount}`)
  if (profile.has('client.list.watch')) {
    fields.push(`watch=${session.watchedKeyCount}`)
  }
  fields.push('qbuf=0', 'qbuf-free=0', 'argv-mem=0')
  if (redis7) {
    fields.push('multi-mem=0', 'rbs=0', 'rbp=0')
  }
  fields.push(
    'obl=0',
    'oll=0',
    'omem=0',
    'tot-mem=0',
    'events=r',
    `cmd=${session.lastCommand ?? 'NULL'}`,
    'user=default',
    'redir=-1',
  )
  if (redis7) {
    fields.push(`resp=${session.protocolVersion}`)
  }
  if (profile.has('client.setinfo')) {
    fields.push(
      `lib-name=${clientLibraryNames.get(session)?.toString() ?? ''}`,
      `lib-ver=${clientLibraryVersions.get(session)?.toString() ?? ''}`,
    )
  }
  if (profile.has('client.list.io-thread')) {
    fields.push('io-thread=0')
  }
  if (profile.has('client.list.net-stats')) {
    fields.push('tot-net-in=0', 'tot-net-out=0', 'tot-cmds=0')
  }

  return `${fields.join(' ')}\n`
}

/**
 * The `flags=` letters, in `catClientInfoString`'s order, for the states this
 * server models: `O` MONITOR, `P` subscribed, `x` in MULTI (until its EXEC
 * has run), `b` blocked, `d` a WATCHed key changed, `r` READONLY (cluster),
 * `e` CLIENT NO-EVICT, `T` CLIENT NO-TOUCH; `N` for none.
 */
function clientFlags(session: RedisClientSession): string {
  let flags = ''
  if (session.monitoring) flags += 'O'
  if (session.pubsubSubscriptionCount > 0) flags += 'P'
  if (session.queuedCommandCount >= 0) flags += 'x'
  if (isClientBlocked(session)) flags += 'b'
  if (session.isWatchDirty()) flags += 'd'
  if (session.clusterReadOnly) flags += 'r'
  if (noEvictClients.has(session)) flags += 'e'
  if (noTouchClients.has(session)) flags += 'T'
  return flags === '' ? 'N' : flags
}

/** The CLIENT KILL filter; an unset field matches every client. */
type ClientKillFilter = {
  /** `undefined` matches any id; an empty set (Valkey 9.0 `ID` with no ids) none. */
  ids?: Set<bigint>
  addr?: string
  laddr?: string
  type?: ClientType
  maxAgeSeconds?: bigint
  skipMe: boolean
}

/**
 * `getClientType`: master links and replicas have their own types, then a
 * subscribed client is `pubsub`. A MONITOR client is `normal`. This server
 * has neither replication links nor replicas.
 */
type ClientType = 'normal' | 'master' | 'replica' | 'pubsub'

function clientType(session: RedisClientSession): ClientType {
  return session.pubsubSubscriptionCount > 0 ? 'pubsub' : 'normal'
}

/** `getClientTypeByName`, case-insensitive; Valkey 8.0 added `primary`. */
function clientTypeByName(
  name: string,
  profile: CompatibilityProfile,
): ClientType | undefined {
  switch (asciiLowerCase(name)) {
    case 'normal':
      return 'normal'
    case 'slave':
    case 'replica':
      return 'replica'
    case 'pubsub':
      return 'pubsub'
    case 'master':
      return 'master'
    case 'primary':
      return profile.has('client.kill.type-primary') ? 'master' : undefined
    default:
      return undefined
  }
}

function clientPeerAddress(session: RedisClientSession): string {
  return session.clientAddress ?? '127.0.0.1:0'
}

function clientLocalAddress(session: RedisClientSession): string {
  return session.localAddress ?? clientPeerAddress(session)
}

function clientAgeSeconds(session: RedisClientSession): number {
  return Math.max(0, Math.floor((Date.now() - session.connectedAtMs) / 1000))
}

function clientIdleSeconds(session: RedisClientSession): number {
  return Math.max(
    0,
    Math.floor((Date.now() - session.lastInteractionMs) / 1000),
  )
}

const LLONG_MAX = 2n ** 63n - 1n

/**
 * CLIENT PAUSE timeout [WRITE|ALL] (`clientPauseCommand` / the `pause`
 * branch of `clientCommand`): the mode is checked before the timeout
 * (`getTimeoutFromObjectOrReply` in milliseconds), and no mode means ALL.
 */
function clientPause(
  subcommand: Buffer,
  args: readonly Buffer[],
  ctx: RedisExecutionContext,
): RedisResult {
  const profile = ctx.server.profile
  expectSubcommandArgCount('CLIENT', subcommand, args, { min: 1 }, profile)
  if (args.length > 2 && !profile.has('client.pause.extra-args')) {
    throw subcommandSyntaxError('CLIENT', subcommand, profile)
  }

  let mode: ClientPauseMode = 'all'
  if (args.length === 2) {
    const token = asciiLowerCase(args[1].toString())
    if (token === 'write') {
      mode = 'write'
    } else if (token !== 'all') {
      throw new RedisCommandError('CLIENT PAUSE mode must be WRITE or ALL')
    }
  }

  const timeout = tryParseInt64(args[0])
  if (timeout === undefined) {
    throw new RedisCommandError('timeout is not an integer or out of range')
  }
  if (timeout < 0n) {
    throw errors.timeoutNegative()
  }

  // A timeout of 0 is an end time of 0: already over.
  let endMs = 0
  if (timeout > 0n) {
    const now = BigInt(Date.now())
    if (timeout <= LLONG_MAX - now) {
      endMs = Number(timeout + now)
    } else if (profile.has('timeout.ms-overflow-check')) {
      throw errors.timeoutOutOfRange()
    } else {
      // 6.2 / 7.0: `tval + now` wraps negative, an end long past.
      endMs = -1
    }
  }

  ctx.server.clientPause.pause(endMs, mode)
  return ok()
}

const CLIENT_ID_NOT_POSITIVE = 'client-id should be greater than 0'

/** A signed 64-bit integer token (`string2ll`), or `undefined`. */
function tryParseInt64(token: Buffer): bigint | undefined {
  try {
    return parseInt64Token(token)
  } catch {
    return undefined
  }
}

/**
 * The filter form of CLIENT KILL (`clientKillCommand` in networking.c), the
 * options parsed left to right and the first bad one answered. An option
 * without a value is a syntax error, except Valkey 9.0's `ID`, which takes
 * every integer after it (and none) and leaves the first non-integer to be
 * read as the next option. NOT-* and Valkey 9.0's other new filters are not
 * modelled and stay syntax errors.
 */
function parseClientKillFilter(
  args: readonly Buffer[],
  profile: CompatibilityProfile,
): ClientKillFilter {
  const filter: ClientKillFilter = { skipMe: true }

  for (let i = 0; i < args.length; i += 2) {
    const option = asciiLowerCase(args[i].toString())

    if (option === 'id' && profile.has('client.kill.id-list')) {
      const ids = (filter.ids ??= new Set())
      for (i++; i < args.length; i++) {
        const id = tryParseInt64(args[i])
        if (id === undefined) {
          break
        }
        if (id < 1n) {
          throw new RedisCommandError(CLIENT_ID_NOT_POSITIVE)
        }
        ids.add(id)
      }
      // `args[i]` is the next option; the loop step adds 2.
      i -= 2
      continue
    }

    const value = args[i + 1]
    if (value === undefined) {
      throw errors.syntax()
    }

    if (option === 'id') {
      if (profile.has('client.kill.id-positive')) {
        const id = tryParseInt64(value)
        if (id === undefined || id < 1n) {
          throw new RedisCommandError(CLIENT_ID_NOT_POSITIVE)
        }
        filter.ids = new Set([id])
      } else {
        // 6.2 takes any long long; 0 means "no id filter".
        const id = parseInt64Token(value)
        filter.ids = id === 0n ? undefined : new Set([id])
      }
      continue
    }

    if (option === 'maxage' && profile.has('client.kill.maxage')) {
      const maxAge = tryParseInt64(value)
      if (maxAge === undefined) {
        throw new RedisCommandError('maxage is not an integer or out of range')
      }
      if (maxAge <= 0n) {
        throw new RedisCommandError('maxage should be greater than 0')
      }
      filter.maxAgeSeconds = maxAge
      continue
    }

    if (option === 'type') {
      const name = value.toString()
      const type = clientTypeByName(name, profile)
      if (type === undefined) {
        throw new RedisCommandError(`Unknown client type '${name}'`)
      }
      filter.type = type
      continue
    }

    if (option === 'addr') {
      filter.addr = value.toString()
      continue
    }

    if (option === 'laddr') {
      filter.laddr = value.toString()
      continue
    }

    if (option === 'user') {
      // `default` is the only user (see DEFAULT_ACL_USER), so every client
      // is logged in as it and the filter matches them all.
      const user = value.toString()
      if (user !== DEFAULT_ACL_USER) {
        throw new RedisCommandError(`No such user '${user}'`)
      }
      continue
    }

    if (option === 'skipme') {
      const skipMe = asciiLowerCase(value.toString())
      if (skipMe === 'yes') {
        filter.skipMe = true
        continue
      }
      if (skipMe === 'no') {
        filter.skipMe = false
        continue
      }
    }

    throw errors.syntax()
  }

  return filter
}

function matchesClientKillFilter(
  session: RedisClientSession,
  currentSession: RedisClientSession,
  filter: ClientKillFilter,
): boolean {
  if (filter.addr !== undefined && clientPeerAddress(session) !== filter.addr) {
    return false
  }
  if (
    filter.laddr !== undefined &&
    clientLocalAddress(session) !== filter.laddr
  ) {
    return false
  }
  if (filter.type !== undefined && clientType(session) !== filter.type) {
    return false
  }
  if (
    filter.ids !== undefined &&
    !filter.ids.has(BigInt(getClientId(session)))
  ) {
    return false
  }
  if (filter.skipMe && session === currentSession) {
    return false
  }
  if (
    filter.maxAgeSeconds !== undefined &&
    BigInt(clientAgeSeconds(session)) < filter.maxAgeSeconds
  ) {
    return false
  }
  return true
}

/** `hello.noauth-proto-wording`: 6.2 and 7.0 leave `<proto>` out. */
function helloNoAuthMessage(profile: CompatibilityProfile): string {
  const form = profile.has('hello.noauth-proto-wording')
    ? 'HELLO <proto> AUTH'
    : 'HELLO AUTH'
  return `HELLO must be called with the client already authenticated, otherwise the ${form} <user> <pass> option can be used to authenticate the client and select the RESP protocol version at the same time`
}

/**
 * The only user this server knows about. Redis' `requirepass` is the password of
 * the built-in `default` user; without an ACL system (no `ACL SETUSER`) it is
 * the sole valid username — any other is rejected with WRONGPASS. Replace this
 * constant with a real user lookup if/when ACL support lands.
 */
const DEFAULT_ACL_USER = 'default'

/**
 * Authenticate `username`/`password` against the server's `requirepass` (the
 * two-argument `AUTH <user> <pass>` / `HELLO ... AUTH <user> <pass>` form). On
 * success the session is marked authenticated.
 *
 *  - With `requirepass` set: only `default` + the matching password succeeds;
 *    anything else is WRONGPASS.
 *  - Without `requirepass`: the `default` user is `nopass`, so any password for
 *    `default` succeeds (no-op); any other username is WRONGPASS.
 */
function authenticateUser(
  ctx: RedisExecutionContext,
  username: string,
  password: Buffer,
): void {
  const requirepass = ctx.server.requirepass

  if (!requirepass) {
    if (username !== DEFAULT_ACL_USER) {
      throw errors.wrongPass()
    }
    ctx.session.setAuthenticated(true)
    return
  }

  if (username !== DEFAULT_ACL_USER || password.toString() !== requirepass) {
    throw errors.wrongPass()
  }

  ctx.session.setAuthenticated(true)
}

/**
 * Parse HELLO's option list. AUTH is processed inline (it can unlock the
 * connection), but SETNAME is only collected as `pendingName` — it must not be
 * applied until the caller has cleared the NOAUTH gate, so a HELLO that fails on
 * a password-protected server leaves connection state untouched.
 */
function parseHelloOptions(
  ctx: RedisExecutionContext,
  args: readonly Buffer[],
): { pendingName?: Buffer } {
  let pendingName: Buffer | undefined

  for (let i = 0; i < args.length; i++) {
    const option = asciiLowerCase(args[i].toString())

    if (option === 'auth') {
      if (i + 2 >= args.length) {
        throw errors.syntax()
      }

      authenticateUser(ctx, args[i + 1].toString(), args[i + 2])
      i += 2
      continue
    }

    if (option === 'setname') {
      if (i + 1 >= args.length) {
        throw errors.syntax()
      }

      pendingName = args[i + 1]
      i++
      continue
    }

    throw errors.syntax()
  }

  return { pendingName }
}

function redactAllMonitorArgs(args: readonly Buffer[]): Buffer[] {
  return args.map(redactedMonitorArg)
}

function redactHelloMonitorArgs(rawArgs: readonly Buffer[]): Buffer[] {
  const args = rawArgs.map(arg => Buffer.from(arg))
  for (let i = 0; i < args.length; i++) {
    if (!equalsAscii(args[i], 'auth')) {
      continue
    }

    redactMonitorArgAt(args, i + 1)
    redactMonitorArgAt(args, i + 2)
    i += 2
  }
  return args
}

function redactMonitorArgAt(args: Buffer[], index: number): void {
  if (index < args.length) {
    args[index] = redactedMonitorArg()
  }
}

function redactedMonitorArg(): Buffer {
  return Buffer.from('(redacted)')
}

export const pingCommand = defineCommand({
  name: 'ping',
  schema: t.object({
    message: t.optional(t.bulk()),
  }),
  flags: ['readonly', 'fast', 'subscribed'],
  keys: () => [],
  execute: (args, ctx) => {
    if (ctx.session.usesSubscribedReplyMode) {
      return RedisResult.create(
        RedisValue.push('pong', [
          RedisValue.bulkString(Buffer.from(args.message ?? '')),
        ]),
      )
    }

    if (args.message) {
      return bulk(args.message)
    }

    return simpleString('PONG')
  },
})

// Not flagged `subscribed`: RESP2 subscribed mode rejects ECHO in real Redis
// (only RESP3 allows it there), and the subscribed-mode policy keys off that flag.
export const echoCommand = defineCommand({
  name: 'echo',
  schema: t.object({
    message: t.bulk(),
  }),
  flags: ['readonly', 'fast'],
  introspection: {
    docs: commandDocs('Returns the given string.', 'connection', [
      { name: 'message', type: 'string' },
    ]),
  },
  keys: () => [],
  execute: args => bulk(args.message),
})

export const quitCommand = defineCommand({
  name: 'quit',
  // Real Redis ignores anything after QUIT (arity -1).
  schema: t.object({ ignored: t.variadic(t.bulk()) }),
  // noscript: real 7.0+ refuses QUIT from a script (6.2 has no QUIT command
  // entry at all, so its script sees an unknown command instead).
  // transaction: every version answers QUIT inside MULTI instead of queueing
  // it (7.0+ excludes it from queueMultiCommand, 6.2 answers it before
  // command lookup), and the close drops the queued commands (#567).
  flags: ['readonly', 'fast', 'subscribed', 'transaction', 'noscript'],
  // 6.2 answers QUIT in the connection loop, before call() feeds MONITOR.
  monitor: {
    skip: profile => !profile.has('command.quit-table-entry'),
  },
  keys: () => [],
  execute: () =>
    RedisResult.create(RedisValue.simpleString('OK'), { close: true }),
})

export const selectCommand = defineCommand({
  name: 'select',
  schema: t.object({
    database: dbIndexSchema,
  }),
  flags: ['fast'],
  capabilities: { clusterMode: 'singleDb' },
  keys: () => [],
  execute: (args, ctx) => {
    // A script runs its commands on its own client: SELECT there moves the
    // script's later commands, never the caller (#566).
    if (ctx.scriptDatabase) {
      if (args.database < 0 || args.database >= ctx.server.databases.length) {
        throw errors.dbIndexOutOfRange()
      }
      ctx.scriptDatabase.selected = args.database
      return ok()
    }
    ctx.session.selectDatabase(args.database)
    return ok()
  },
})

export const infoCommand = defineCommand({
  name: 'info',
  schema: t.object({
    sections: t.variadic(t.string()),
  }),
  flags: ['readonly', 'admin'],
  keys: () => [],
  execute: (args, ctx) => {
    if (
      args.sections.length > 1 &&
      !ctx.server.profile.has('info.multi-section')
    ) {
      throw errors.syntax()
    }

    return verbatimText(buildInfo(ctx, args.sections))
  },
})

export const clientCommand = defineCommand({
  name: 'client',
  schema: t.object({
    // Raw bytes, not `t.string()`: the unknown-subcommand reply echoes the
    // name the client sent, and a UTF-8 decode here would lose its bytes.
    subcommand: t.bulk(),
    args: t.variadic(t.bulk()),
  }),
  // noscript: real Redis refuses CLIENT from scripts on every version — every
  // subcommand but HELP on 7.0+ (see lua-runtime's isRefusedFromScript).
  flags: ['readonly', 'admin', 'noscript'],
  introspection: {
    subcommands: [
      commandSubcommandInfo('client|id', 2),
      commandSubcommandInfo('client|info', 2),
      commandSubcommandInfo('client|kill', -3),
      commandSubcommandInfo('client|list', -2),
      commandSubcommandInfo('client|getname', 2),
      commandSubcommandInfo('client|setname', 3),
      commandSubcommandInfo('client|no-evict', 3),
      commandSubcommandInfo('client|no-touch', 3),
      commandSubcommandInfo('client|pause', -3),
      commandSubcommandInfo('client|unpause', 2),
      commandSubcommandInfo('client|reply', 3),
      commandSubcommandInfo('client|setinfo', 4),
      commandSubcommandInfo('client|help', 2),
    ],
  },
  keys: () => [],
  execute: (args, ctx) => {
    const subcommand = asciiLowerCase(args.subcommand.toString())

    if (subcommand === 'setname') {
      expectSubcommandArgCount(
        'CLIENT',
        args.subcommand,
        args.args,
        1,
        ctx.server.profile,
      )
      setClientName(ctx.session, args.args[0])
      return ok()
    }

    if (subcommand === 'getname') {
      expectSubcommandArgCount(
        'CLIENT',
        args.subcommand,
        args.args,
        0,
        ctx.server.profile,
      )
      return bulk(clientNames.get(ctx.session) ?? null)
    }

    if (subcommand === 'setinfo') {
      if (!ctx.server.profile.has('client.setinfo')) {
        throw unknownSubcommandError(
          'CLIENT',
          args.subcommand,
          ctx.server.profile,
        )
      }

      expectSubcommandArgCount(
        'CLIENT',
        args.subcommand,
        args.args,
        2,
        ctx.server.profile,
      )
      const attribute = asciiLowerCase(args.args[0].toString())
      if (attribute === 'lib-name') {
        clientLibraryNames.set(ctx.session, args.args[1])
      } else if (attribute === 'lib-ver') {
        clientLibraryVersions.set(ctx.session, args.args[1])
      }
      return ok()
    }

    if (subcommand === 'no-evict') {
      if (!ctx.server.profile.has('client.no-evict')) {
        throw unknownSubcommandError(
          'CLIENT',
          args.subcommand,
          ctx.server.profile,
        )
      }

      expectSubcommandArgCount(
        'CLIENT',
        args.subcommand,
        args.args,
        1,
        ctx.server.profile,
      )
      const mode = asciiLowerCase(args.args[0].toString())
      if (mode === 'on') {
        noEvictClients.add(ctx.session)
        return ok()
      }
      if (mode === 'off') {
        noEvictClients.delete(ctx.session)
        return ok()
      }

      throw errors.syntax()
    }

    if (subcommand === 'no-touch') {
      if (!ctx.server.profile.has('client.no-touch')) {
        throw unknownSubcommandError(
          'CLIENT',
          args.subcommand,
          ctx.server.profile,
        )
      }

      expectSubcommandArgCount(
        'CLIENT',
        args.subcommand,
        args.args,
        1,
        ctx.server.profile,
      )
      const mode = asciiLowerCase(args.args[0].toString())
      if (mode === 'on') {
        noTouchClients.add(ctx.session)
        return ok()
      }
      if (mode === 'off') {
        noTouchClients.delete(ctx.session)
        return ok()
      }

      throw errors.syntax()
    }

    if (subcommand === 'pause') {
      return clientPause(args.subcommand, args.args, ctx)
    }

    if (subcommand === 'unpause') {
      expectSubcommandArgCount(
        'CLIENT',
        args.subcommand,
        args.args,
        0,
        ctx.server.profile,
      )
      ctx.server.clientPause.unpause()
      return ok()
    }

    if (subcommand === 'reply') {
      expectSubcommandArgCount(
        'CLIENT',
        args.subcommand,
        args.args,
        1,
        ctx.server.profile,
      )
      const mode = asciiLowerCase(args.args[0].toString())
      if (mode !== 'on' && mode !== 'off' && mode !== 'skip') {
        throw errors.syntax()
      }
      setClientReplyMode(ctx.session, mode)
      // OFF and SKIP add no reply at all. Inside EXEC real Redis then sends
      // the array one element short; here the slot reads +OK (known gap).
      return mode === 'on'
        ? ok()
        : RedisResult.create(RedisValue.simpleString('OK'), {
            omitReply: true,
          })
    }

    if (subcommand === 'kill') {
      const killArgs = args.args
      if (killArgs.length === 0) {
        // Only 6.2 gets here: 7.0+ answer the `client|kill` arity error first.
        throw errors.syntax()
      }
      // One argument is always the legacy `CLIENT KILL <ip:port>`, which may
      // kill the caller and answers +OK or `No such client`.
      const legacy = killArgs.length === 1
      const filter: ClientKillFilter = legacy
        ? { addr: killArgs[0].toString(), skipMe: false }
        : parseClientKillFilter(killArgs, ctx.server.profile)
      const victims = ctx.server
        .getConnectedClients()
        .filter(session =>
          matchesClientKillFilter(session, ctx.session, filter),
        )

      if (legacy && victims.length === 0) {
        throw new RedisCommandError('No such client')
      }

      for (const victim of victims) {
        if (victim !== ctx.session) {
          victim.disconnect('client kill')
        }
      }

      const reply = legacy
        ? RedisValue.simpleString('OK')
        : RedisValue.integer(victims.length)
      if (!victims.includes(ctx.session)) {
        return RedisResult.create(reply)
      }

      return RedisResult.create(reply, {
        afterReply: () => ctx.session.disconnect('client kill'),
      })
    }

    if (subcommand === 'id') {
      expectSubcommandArgCount(
        'CLIENT',
        args.subcommand,
        args.args,
        0,
        ctx.server.profile,
      )
      return integer(getClientId(ctx.session))
    }

    if (subcommand === 'info') {
      expectSubcommandArgCount(
        'CLIENT',
        args.subcommand,
        args.args,
        0,
        ctx.server.profile,
      )
      return verbatimText(formatClientLine(ctx.session, ctx.server.profile))
    }

    if (subcommand === 'list') {
      expectArgCount('client|list', args.args, 0)
      const lines = ctx.server
        .getConnectedClients()
        .map(session => formatClientLine(session, ctx.server.profile))
      return verbatimText(lines.join(''))
    }

    if (subcommand === 'help') {
      expectSubcommandArgCount(
        'CLIENT',
        args.subcommand,
        args.args,
        0,
        ctx.server.profile,
      )
      return helpReply(clientHelpLines(ctx.server.profile), ctx.server.profile)
    }

    throw unknownSubcommandError('CLIENT', args.subcommand, ctx.server.profile)
  },
})

/**
 * Parse HELLO's protocol-version token. Unlike a generic `t.integer()` field,
 * an out-of-range-but-valid integer (e.g. `4`, `0`, `-1`) must produce the
 * protocol-specific `NOPROTO` error rather than the generic `ERR ... out of
 * range`, and a genuinely non-integer token gets HELLO's own wording for the
 * `ERR` case — both only discoverable by checking real Redis's wire replies.
 */
function parseHelloVersion(token: Buffer): 2 | 3 {
  const raw = token.toString()
  if (!isIntegerToken(raw)) {
    throw errors.helloProtocolNotInteger()
  }

  const parsed = Number(raw)
  if (!Number.isSafeInteger(parsed)) {
    throw errors.helloProtocolNotInteger()
  }

  if (parsed !== 2 && parsed !== 3) {
    throw errors.noProto()
  }

  return parsed
}

export const helloCommand = defineCommand({
  name: 'hello',
  schema: t.object({
    version: t.optional(t.bulk()),
    args: t.variadic(t.bulk()),
  }),
  flags: ['noscript'],
  monitor: {
    redactArgs: redactHelloMonitorArgs,
  },
  keys: () => [],
  execute: (args, ctx) => {
    const version =
      args.version === undefined
        ? ctx.session.protocolVersion
        : parseHelloVersion(args.version)
    const { pendingName } = parseHelloOptions(ctx, args.args)

    // A password-protected server rejects HELLO unless the client is already
    // authenticated or authenticates inline via the AUTH option above. The gate
    // runs before any SETNAME side effect is applied, so a failed HELLO leaves
    // connection state untouched.
    if (ctx.server.requirepass && !ctx.session.isAuthenticated) {
      throw new NoAuthError(helloNoAuthMessage(ctx.server.profile))
    }

    if (pendingName !== undefined) {
      setClientName(ctx.session, pendingName)
    }

    ctx.session.setProtocolVersion(version)

    const fields: [RedisValue, RedisValue][] = [
      [value('server'), value(ctx.server.profile.flavor)],
      [value('version'), value(ctx.server.profile.version)],
      [value('proto'), RedisValue.integer(version)],
      [value('id'), RedisValue.integer(getClientId(ctx.session))],
      [value('mode'), value(redisMode(ctx))],
      [value('role'), value(ctx.nodeRole ?? 'master')],
      [value('modules'), RedisValue.array([])],
    ]

    if (version === 3) {
      return RedisResult.create(RedisValue.map(fields))
    }

    return RedisResult.create(RedisValue.array(fields.flat()))
  },
})

function serverIdentityLines(ctx: RedisExecutionContext): string[] {
  const profile = ctx.server.profile
  if (profile.flavor === 'redis') {
    return [`redis_version:${profile.version}`]
  }

  return [
    `redis_version:${VALKEY_REDIS_COMPAT_VERSION}`,
    'server_name:valkey',
    `valkey_version:${profile.version}`,
  ]
}

export const authCommand = defineCommand({
  name: 'auth',
  schema: t.object({
    args: t.variadic(t.bulk(), { min: 1 }),
  }),
  flags: ['noscript'],
  monitor: {
    redactArgs: redactAllMonitorArgs,
  },
  keys: () => [],
  execute: (args, ctx) => {
    // The schema guarantees at least one argument.
    if (args.args.length > 2) {
      throw new WrongNumberOfArgumentsError('auth')
    }

    // Single-arg AUTH targets the default user. When no password is configured
    // Redis answers with a dedicated hint instead of authenticating.
    if (args.args.length === 1) {
      if (!ctx.server.requirepass) {
        throw errors.noPasswordConfigured()
      }
      authenticateUser(ctx, DEFAULT_ACL_USER, args.args[0])
      return ok()
    }

    authenticateUser(ctx, args.args[0].toString(), args.args[1])
    return ok()
  },
})

export const resetCommand = defineCommand({
  name: 'reset',
  since: { redis: '6.2.0', valkey: '7.2.0' },
  schema: t.object({}),
  // 'transaction' makes RESET bypass MULTI queueing and run immediately, like
  // EXEC/DISCARD/WATCH — it aborts the in-flight transaction via
  // discardTransaction() instead of being queued until EXEC (matches real
  // Redis, which excludes RESET from queueMultiCommand).
  // noscript: real Redis refuses RESET from a script on every version.
  flags: ['admin', 'subscribed', 'transaction', 'noscript'],
  keys: () => [],
  execute: (_args, ctx) => {
    clientNames.delete(ctx.session)
    clientLibraryNames.delete(ctx.session)
    clientLibraryVersions.delete(ctx.session)
    noEvictClients.delete(ctx.session)
    noTouchClients.delete(ctx.session)
    resetClientReplyMode(ctx.session)
    ctx.session.resetPushProducers()
    ctx.session.resetPubSub()
    ctx.session.discardTransaction()
    ctx.session.unwatch()
    ctx.session.selectDatabase(0)
    ctx.session.setProtocolVersion(2)
    ctx.session.setClusterReadOnly(false)
    ctx.session.setAuthenticated(false)
    return simpleString('RESET')
  },
})

export const timeCommand = defineCommand({
  name: 'time',
  schema: t.object({}),
  flags: ['readonly', 'random', 'fast'],
  keys: () => [],
  execute: () => {
    const now = Date.now()
    const seconds = Math.floor(now / 1000)
    const microseconds = (now % 1000) * 1000
    return array([
      RedisValue.bulkString(Buffer.from(seconds.toString())),
      RedisValue.bulkString(Buffer.from(microseconds.toString())),
    ])
  },
})

export const lastsaveCommand = defineCommand({
  name: 'lastsave',
  schema: t.object({}),
  flags: ['readonly', 'fast'],
  keys: () => [],
  execute: () => integer(serverStartTimeSeconds),
})

export const aclCommand = defineCommand({
  name: 'acl',
  schema: t.object({
    // Raw bytes, not `t.string()`: the unknown-subcommand reply echoes the
    // name the client sent, and a UTF-8 decode here would lose its bytes.
    subcommand: t.bulk(),
    args: t.variadic(t.bulk()),
  }),
  flags: ['admin', 'noscript'],
  introspection: {
    subcommands: [
      commandSubcommandInfo('acl|whoami', 2),
      commandSubcommandInfo('acl|dryrun', -4),
      commandSubcommandInfo('acl|help', 2),
    ],
  },
  keys: () => [],
  execute: (args, ctx) => {
    const subcommand = asciiLowerCase(args.subcommand.toString())

    if (subcommand === 'whoami') {
      expectSubcommandArgCount(
        'ACL',
        args.subcommand,
        args.args,
        0,
        ctx.server.profile,
      )
      return bulk(Buffer.from(DEFAULT_ACL_USER))
    }

    if (subcommand === 'dryrun') {
      if (!ctx.server.profile.has('acl.dryrun')) {
        throw unknownSubcommandError('ACL', args.subcommand, ctx.server.profile)
      }

      expectSubcommandArgCount(
        'ACL',
        args.subcommand,
        args.args,
        { min: 2 },
        ctx.server.profile,
      )

      const username = args.args[0].toString()
      if (username !== DEFAULT_ACL_USER) {
        throw new RedisCommandError(`User '${username}' not found`)
      }

      const command = args.args[1]
      if (!ctx.executor.getCommandDefinition(command.toString())) {
        throw new RedisCommandError(`Command '${command.toString()}' not found`)
      }
      try {
        ctx.executor.plan(command, args.args.slice(2))
      } catch (err) {
        // A 7.0+ lookup resolves `command|subcommand` as one name, so an
        // unknown subcommand is an unknown command here too (acl.c).
        if (err instanceof UnknownSubcommandError) {
          throw new RedisCommandError(
            `Command '${command.toString()}' not found`,
          )
        }
        throw err
      }
      return ok()
    }

    if (subcommand === 'help') {
      expectSubcommandArgCount(
        'ACL',
        args.subcommand,
        args.args,
        0,
        ctx.server.profile,
      )
      return helpReply(aclHelpLines(ctx.server.profile), ctx.server.profile)
    }

    throw unknownSubcommandError('ACL', args.subcommand, ctx.server.profile)
  },
})

export const slowlogCommand = defineCommand({
  name: 'slowlog',
  schema: t.object({
    // Raw bytes, not `t.string()`: the unknown-subcommand reply echoes the
    // name the client sent, and a UTF-8 decode here would lose its bytes.
    subcommand: t.bulk(),
    args: t.variadic(t.bulk()),
  }),
  flags: ['readonly', 'admin'],
  introspection: {
    subcommands: [
      commandSubcommandInfo('slowlog|get', -2),
      commandSubcommandInfo('slowlog|len', 2),
      commandSubcommandInfo('slowlog|reset', 2),
      commandSubcommandInfo('slowlog|help', 2),
    ],
  },
  keys: () => [],
  execute: (args, ctx) => {
    const subcommand = asciiLowerCase(args.subcommand.toString())

    if (subcommand === 'get') {
      // `slowlog|get` is -2 in the table: lookup lets any count through, and
      // more than one argument is the subcommand syntax error everywhere.
      if (args.args.length > 1) {
        throw subcommandSyntaxError(
          'SLOWLOG',
          args.subcommand,
          ctx.server.profile,
        )
      }
      if (args.args.length === 1) {
        const count = args.args[0].toString()
        if (!isIntegerToken(count) || Number(count) < -1) {
          throw new RedisCommandError(
            'count should be greater than or equal to -1',
          )
        }
      }
      return array([])
    }

    if (subcommand === 'len') {
      expectSubcommandArgCount(
        'SLOWLOG',
        args.subcommand,
        args.args,
        0,
        ctx.server.profile,
      )
      return integer(0)
    }

    if (subcommand === 'reset') {
      expectSubcommandArgCount(
        'SLOWLOG',
        args.subcommand,
        args.args,
        0,
        ctx.server.profile,
      )
      return ok()
    }

    if (subcommand === 'help') {
      expectSubcommandArgCount(
        'SLOWLOG',
        args.subcommand,
        args.args,
        0,
        ctx.server.profile,
      )
      return helpReply(slowlogHelpLines(ctx.server.profile), ctx.server.profile)
    }

    throw unknownSubcommandError('SLOWLOG', args.subcommand, ctx.server.profile)
  },
})

export const shutdownCommand = defineCommand({
  name: 'shutdown',
  schema: t.object({
    args: t.variadic(t.bulk()),
  }),
  flags: ['admin', 'noscript'],
  keys: () => [],
  execute: (args, ctx) => {
    const options = parseShutdownOptions(args.args, ctx)
    if (options.abort) {
      throw new RedisCommandError('No shutdown in progress.')
    }

    return RedisResult.create(RedisValue.null(), {
      close: true,
      omitReply: true,
    })
  },
})

function parseShutdownOptions(
  args: readonly Buffer[],
  ctx: RedisExecutionContext,
): { abort: boolean } {
  let abort = false

  for (const arg of args) {
    const option = asciiLowerCase(arg.toString())
    if (option === 'save' || option === 'nosave') {
      continue
    }

    if (option === 'now' || option === 'force' || option === 'abort') {
      if (!ctx.server.profile.has('shutdown.now-force-abort')) {
        throw errors.syntax()
      }
      abort ||= option === 'abort'
      continue
    }

    throw errors.syntax()
  }

  if (abort && args.length > 1) {
    throw errors.syntax()
  }

  return { abort }
}

export const connectionCommands = [
  pingCommand,
  echoCommand,
  quitCommand,
  selectCommand,
  infoCommand,
  clientCommand,
  helloCommand,
  authCommand,
  resetCommand,
  timeCommand,
  lastsaveCommand,
  aclCommand,
  slowlogCommand,
  shutdownCommand,
]
