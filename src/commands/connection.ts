import { defineCommand } from '../core/command-definition'
import { t } from '../core/command-schema'
import type {
  RedisClientSession,
  RedisExecutionContext,
} from '../core/redis-context'
import {
  RedisCommandError,
  RedisSyntaxError,
  WrongNumberOfArgumentsError,
} from '../core/redis-error'
import { RedisResult } from '../core/redis-result'
import { RedisValue } from '../core/redis-value'
import { array, bulk, integer, ok, simpleString } from './helpers'
import { commandSubcommandInfo } from './introspection'

const REDIS_VERSION = '7.4.4'

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
      'repl_backlog_active:0',
    ],
    cpu: () => [
      '# CPU',
      'used_cpu_sys:0.00',
      'used_cpu_user:0.00',
      'used_cpu_sys_children:0.00',
      'used_cpu_user_children:0.00',
    ],
    cluster: () => [
      '# Cluster',
      `cluster_enabled:${clustered ? 1 : 0}`,
      `cluster_state:${clustered ? 'ok' : 'fail'}`,
      `cluster_slots_assigned:${clustered ? 16384 : 0}`,
    ],
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
    throw new RedisCommandError('Invalid section name')
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

function formatClientLine(
  ctx: RedisExecutionContext,
  session: RedisClientSession,
): string {
  const name = clientNames.get(session)?.toString() ?? ''
  const libName = clientLibraryNames.get(session)?.toString()
  const libVersion = clientLibraryVersions.get(session)?.toString()
  const fields = [
    `id=${getClientId(session)}`,
    'addr=127.0.0.1:0',
    'laddr=127.0.0.1:6379',
    'fd=0',
    `name=${name}`,
    `db=${ctx.session.selectedDatabase}`,
    'age=0',
    'idle=0',
    'flags=N',
    'sub=0',
    'psub=0',
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
    'resp=2',
  ]

  if (libName !== undefined) {
    fields.push(`lib-name=${libName}`)
  }
  if (libVersion !== undefined) {
    fields.push(`lib-ver=${libVersion}`)
  }

  return `${fields.join(' ')}\n`
}

function parseHelloOptions(
  ctx: RedisExecutionContext,
  args: readonly Buffer[],
) {
  for (let i = 0; i < args.length; i++) {
    const option = args[i].toString().toLowerCase()

    if (option === 'auth') {
      if (i + 2 >= args.length) {
        throw new RedisSyntaxError()
      }

      throw new RedisCommandError(
        'AUTH <password> called without any password configured for the default user. Are you sure your configuration is correct?',
      )
    }

    if (option === 'setname') {
      if (i + 1 >= args.length) {
        throw new RedisSyntaxError()
      }

      clientNames.set(ctx.session, args[i + 1])
      i++
      continue
    }

    throw new RedisSyntaxError()
  }
}

export const pingCommand = defineCommand({
  name: 'ping',
  schema: t.object({
    message: t.optional(t.bulk()),
  }),
  flags: ['readonly', 'fast'],
  keys: () => [],
  execute: args => {
    if (args.message) {
      return bulk(args.message)
    }

    return simpleString('PONG')
  },
})

export const quitCommand = defineCommand({
  name: 'quit',
  schema: t.object({}),
  flags: ['readonly', 'fast'],
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
      return bulk(Buffer.from(formatClientLine(ctx, ctx.session)))
    }

    if (subcommand === 'list') {
      expectArgCount('client|list', args.args, 0)
      return bulk(Buffer.from(formatClientLine(ctx, ctx.session)))
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

export const helloCommand = defineCommand({
  name: 'hello',
  schema: t.object({
    version: t.optional(t.integer({ min: 2, max: 3 })),
    args: t.variadic(t.bulk()),
  }),
  flags: ['readonly', 'admin'],
  keys: () => [],
  execute: (args, ctx) => {
    const version =
      args.version === undefined
        ? ctx.session.protocolVersion
        : args.version === 3
          ? 3
          : 2
    parseHelloOptions(ctx, args.args)
    ctx.session.setProtocolVersion(version)

    const fields: [RedisValue, RedisValue][] = [
      [value('server'), value('redis')],
      [value('version'), value(REDIS_VERSION)],
      [value('proto'), RedisValue.integer(version)],
      [value('id'), RedisValue.integer(getClientId(ctx.session))],
      [value('mode'), value(redisMode(ctx))],
      [value('role'), value('master')],
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
  flags: ['readonly', 'admin'],
  keys: () => [],
  execute: args => {
    if (args.args.length !== 1 && args.args.length !== 2) {
      throw new WrongNumberOfArgumentsError('auth')
    }

    throw new RedisCommandError(
      'AUTH <password> called without any password configured for the default user. Are you sure your configuration is correct?',
    )
  },
})

export const resetCommand = defineCommand({
  name: 'reset',
  schema: t.object({}),
  flags: ['admin'],
  keys: () => [],
  execute: (_args, ctx) => {
    clientNames.delete(ctx.session)
    clientLibraryNames.delete(ctx.session)
    clientLibraryVersions.delete(ctx.session)
    ctx.session.discardTransaction()
    ctx.session.unwatch()
    ctx.session.selectDatabase(0)
    ctx.session.setProtocolVersion(2)
    ctx.session.setClusterReadOnly(false)
    return simpleString('RESET')
  },
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
]
