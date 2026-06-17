import { defineCommand } from '../core/command-definition'
import { isIntegerToken, t } from '../core/command-schema'
import type {
  RedisClientSession,
  RedisExecutionContext,
} from '../core/redis-context'
import {
  HelloProtocolNotIntegerError,
  NoAuthError,
  NoPasswordConfiguredError,
  NoProtoError,
  RedisCommandError,
  RedisSyntaxError,
  WrongNumberOfArgumentsError,
  WrongPassError,
} from '../core/redis-error'
import { RedisResult } from '../core/redis-result'
import { RedisValue } from '../core/redis-value'
import { array, bulk, integer, ok, simpleString } from './helpers'
import { commandSubcommandInfo } from './introspection'

const REDIS_VERSION = '7.4.4'
const MASTER_REPLID = '0000000000000000000000000000000000000000'

// There is no persistence, so LASTSAVE reports the process/server start time.
const serverStartTimeSeconds = Math.floor(Date.now() / 1000)

const clientIds = new WeakMap<RedisClientSession, number>()
const clientNames = new WeakMap<RedisClientSession, Buffer>()
const clientLibraryNames = new WeakMap<RedisClientSession, Buffer>()
const clientLibraryVersions = new WeakMap<RedisClientSession, Buffer>()
let nextClientId = 1

function getClientId(session: RedisClientSession): number {
  const existing = clientIds.get(session)
  if (existing !== undefined) {
    return existing
  }

  const id = nextClientId++
  clientIds.set(session, id)
  return id
}

function isClusterMode(ctx: RedisExecutionContext): boolean {
  return ctx.server.clusterTopology.nodes.length > 0
}

function redisMode(ctx: RedisExecutionContext): string {
  return isClusterMode(ctx) ? 'cluster' : 'standalone'
}

function value(value: string): RedisValue {
  return RedisValue.bulkString(Buffer.from(value))
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

function buildInfo(ctx: RedisExecutionContext, section?: string): string {
  const requestedSection = section?.toLowerCase() ?? 'default'
  const clustered = isClusterMode(ctx)
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
      `redis_version:${REDIS_VERSION}`,
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
      'blocked_clients:0',
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
    cluster: () => {
      if (!clustered) {
        return ['# Cluster', 'cluster_enabled:0']
      }

      return [
        '# Cluster',
        'cluster_enabled:1',
        'cluster_state:ok',
        'cluster_slots_assigned:16384',
      ]
    },
    commandstats: () => ['# Commandstats'],
    latencystats: () => ['# Latencystats'],
    errorstats: () => ['# Errorstats'],
    modules: () => ['# Modules'],
    sentinel: () => ['# Sentinel'],
    keyspace: () => {
      const lines = ['# Keyspace']
      for (const [index, database] of ctx.server.databases.entries()) {
        const keyCount = database.size()
        if (keyCount === 0) {
          continue
        }

        lines.push(`db${index}:keys=${keyCount},expires=0,avg_ttl=0`)
      }
      return lines
    },
  }

  let selectedSections: string[]
  if (requestedSection === 'default' || requestedSection === 'all') {
    selectedSections = defaultSections
  } else if (requestedSection in sectionBuilders) {
    selectedSections = [requestedSection]
  } else {
    // Real Redis replies with an empty bulk string for an unrecognized
    // section name rather than an error (clients like ioredis probe with
    // arbitrary section names during connection setup).
    return ''
  }

  const lines: string[] = []
  for (const selectedSection of selectedSections) {
    if (lines.length > 0) {
      lines.push('')
    }

    lines.push(...sectionBuilders[selectedSection]())
  }

  return `${lines.join('\r\n')}\r\n`
}

function formatClientLine(session: RedisClientSession): string {
  const name = clientNames.get(session)?.toString() ?? ''
  const libName = clientLibraryNames.get(session)?.toString()
  const libVersion = clientLibraryVersions.get(session)?.toString()
  const fields = [
    `id=${getClientId(session)}`,
    `addr=${session.clientAddress ?? '127.0.0.1:0'}`,
    'laddr=127.0.0.1:6379',
    'fd=0',
    `name=${name}`,
    `db=${session.selectedDatabase}`,
    'age=0',
    'idle=0',
    `flags=${session.mode === 'subscribed' ? 'P' : 'N'}`,
    `sub=${session.pubsubChannelCount}`,
    `psub=${session.pubsubPatternCount}`,
    'multi=-1',
    'qbuf=0',
    'qbuf-free=0',
    'argv-mem=0',
    'multi-mem=0',
    'rbs=0',
    'rbp=0',
    'obl=0',
    'oll=0',
    'omem=0',
    'tot-mem=0',
    'events=r',
    'cmd=client',
    'user=default',
    'redir=-1',
    `resp=${session.protocolVersion}`,
  ]

  if (libName !== undefined) {
    fields.push(`lib-name=${libName}`)
  }
  if (libVersion !== undefined) {
    fields.push(`lib-ver=${libVersion}`)
  }

  return `${fields.join(' ')}\n`
}

const HELLO_NOAUTH_MESSAGE =
  'HELLO must be called with the client already authenticated, otherwise the HELLO <proto> AUTH <user> <pass> option can be used to authenticate the client and select the RESP protocol version at the same time'

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
      throw new WrongPassError()
    }
    ctx.session.setAuthenticated(true)
    return
  }

  if (username !== DEFAULT_ACL_USER || password.toString() !== requirepass) {
    throw new WrongPassError()
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
    const option = args[i].toString().toLowerCase()

    if (option === 'auth') {
      if (i + 2 >= args.length) {
        throw new RedisSyntaxError()
      }

      authenticateUser(ctx, args[i + 1].toString(), args[i + 2])
      i += 2
      continue
    }

    if (option === 'setname') {
      if (i + 1 >= args.length) {
        throw new RedisSyntaxError()
      }

      pendingName = args[i + 1]
      i++
      continue
    }

    throw new RedisSyntaxError()
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

function equalsAscii(value: Buffer, expected: string): boolean {
  return value.toString().toLowerCase() === expected
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

export const quitCommand = defineCommand({
  name: 'quit',
  schema: t.object({}),
  flags: ['readonly', 'fast', 'subscribed'],
  keys: () => [],
  execute: () =>
    RedisResult.create(RedisValue.simpleString('OK'), { close: true }),
})

export const selectCommand = defineCommand({
  name: 'select',
  schema: t.object({
    database: t.integer({ min: 0 }),
  }),
  flags: ['fast'],
  keys: () => [],
  execute: (args, ctx) => {
    ctx.session.selectDatabase(args.database)
    return ok()
  },
})

export const infoCommand = defineCommand({
  name: 'info',
  schema: t.object({
    section: t.optional(t.string()),
  }),
  flags: ['readonly', 'admin'],
  keys: () => [],
  execute: (args, ctx) => bulk(Buffer.from(buildInfo(ctx, args.section))),
})

export const clientCommand = defineCommand({
  name: 'client',
  schema: t.object({
    subcommand: t.string(),
    args: t.variadic(t.bulk()),
  }),
  flags: ['readonly', 'admin'],
  introspection: {
    arity: -2,
    flags: [],
    firstKey: 0,
    lastKey: 0,
    keyStep: 0,
    categories: ['@slow', '@connection'],
    keySpecs: [],
    subcommands: [
      commandSubcommandInfo('client|id', 2),
      commandSubcommandInfo('client|info', 2),
      commandSubcommandInfo('client|list', 2),
      commandSubcommandInfo('client|getname', 2),
      commandSubcommandInfo('client|setname', 3),
      commandSubcommandInfo('client|setinfo', 4),
      commandSubcommandInfo('client|help', 2),
    ],
  },
  keys: () => [],
  execute: (args, ctx) => {
    const subcommand = args.subcommand.toLowerCase()

    if (subcommand === 'setname') {
      expectArgCount('client|setname', args.args, 1)
      clientNames.set(ctx.session, args.args[0])
      return ok()
    }

    if (subcommand === 'getname') {
      expectArgCount('client|getname', args.args, 0)
      return bulk(clientNames.get(ctx.session) ?? null)
    }

    if (subcommand === 'setinfo') {
      expectArgCount('client|setinfo', args.args, 2)
      const attribute = args.args[0].toString().toLowerCase()
      if (attribute === 'lib-name') {
        clientLibraryNames.set(ctx.session, args.args[1])
      } else if (attribute === 'lib-ver') {
        clientLibraryVersions.set(ctx.session, args.args[1])
      }
      return ok()
    }

    if (subcommand === 'id') {
      expectArgCount('client|id', args.args, 0)
      return integer(getClientId(ctx.session))
    }

    if (subcommand === 'info') {
      expectArgCount('client|info', args.args, 0)
      return bulk(Buffer.from(formatClientLine(ctx.session)))
    }

    if (subcommand === 'list') {
      expectArgCount('client|list', args.args, 0)
      const lines = ctx.server.getConnectedClients().map(formatClientLine)
      return bulk(Buffer.from(lines.join('')))
    }

    if (subcommand === 'help') {
      expectArgCount('client|help', args.args, 0)
      return array([
        value(
          'CLIENT <subcommand> [<arg> [value] [opt] ...]. Subcommands are:',
        ),
        value('ID'),
        value('INFO'),
        value('LIST'),
        value('GETNAME'),
        value('SETNAME <connection-name>'),
        value('SETINFO <LIB-NAME|LIB-VER> <value>'),
      ])
    }

    throw new RedisCommandError(
      `unknown subcommand '${subcommand}'. Try CLIENT HELP.`,
    )
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
    throw new HelloProtocolNotIntegerError()
  }

  const parsed = Number(raw)
  if (!Number.isSafeInteger(parsed)) {
    throw new HelloProtocolNotIntegerError()
  }

  if (parsed !== 2 && parsed !== 3) {
    throw new NoProtoError()
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
      throw new NoAuthError(HELLO_NOAUTH_MESSAGE)
    }

    if (pendingName !== undefined) {
      clientNames.set(ctx.session, pendingName)
    }

    ctx.session.setProtocolVersion(version)

    const fields: [RedisValue, RedisValue][] = [
      [value('server'), value('redis')],
      [value('version'), value(REDIS_VERSION)],
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

export const authCommand = defineCommand({
  name: 'auth',
  schema: t.object({
    args: t.variadic(t.bulk()),
  }),
  flags: ['noscript'],
  monitor: {
    redactArgs: redactAllMonitorArgs,
  },
  keys: () => [],
  execute: (args, ctx) => {
    if (args.args.length !== 1 && args.args.length !== 2) {
      throw new WrongNumberOfArgumentsError('auth')
    }

    // Single-arg AUTH targets the default user. When no password is configured
    // Redis answers with a dedicated hint instead of authenticating.
    if (args.args.length === 1) {
      if (!ctx.server.requirepass) {
        throw new NoPasswordConfiguredError()
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
  schema: t.object({}),
  // 'transaction' makes RESET bypass MULTI queueing and run immediately, like
  // EXEC/DISCARD/WATCH — it aborts the in-flight transaction via
  // discardTransaction() instead of being queued until EXEC (matches real
  // Redis, which excludes RESET from queueMultiCommand).
  flags: ['admin', 'subscribed', 'transaction'],
  keys: () => [],
  execute: (_args, ctx) => {
    clientNames.delete(ctx.session)
    clientLibraryNames.delete(ctx.session)
    clientLibraryVersions.delete(ctx.session)
    ctx.session.resetResponseStreams()
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

export const connectionCommands = [
  pingCommand,
  quitCommand,
  selectCommand,
  infoCommand,
  clientCommand,
  helloCommand,
  authCommand,
  resetCommand,
  timeCommand,
  lastsaveCommand,
]
