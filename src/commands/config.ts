import { defineCommand } from '../core/command-definition'
import { t } from '../core/command-schema'
import type { RedisExecutionContext } from '../core/redis-context'
import {
  RedisCommandError,
  WrongNumberOfArgumentsError,
} from '../core/redis-error'
import { RedisResult } from '../core/redis-result'
import { RedisValue } from '../core/redis-value'
import { ok } from './helpers'
import { commandSubcommandInfo } from './introspection'

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
  'proto-max-bulk-len': '536870912',
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
  const patterns = args.map(arg => arg.toString())
  const matched = new Map<string, string>()
  for (const [name, value] of store) {
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

function configSet(
  args: readonly Buffer[],
  ctx: RedisExecutionContext,
): RedisResult {
  if (args.length === 0 || args.length % 2 !== 0) {
    throw new WrongNumberOfArgumentsError('config|set')
  }

  const store = getConfigStore(ctx)
  const updates: [string, string][] = []
  for (let i = 0; i < args.length; i += 2) {
    const name = args[i].toString().toLowerCase()
    const value = args[i + 1].toString()
    if (!store.has(name)) {
      throw new RedisCommandError(
        `Unknown option or number of arguments for CONFIG SET - '${args[i].toString()}'`,
      )
    }
    updates.push([name, value])
  }

  // Validate every parameter before applying any — CONFIG SET is atomic.
  for (const [name, value] of updates) {
    store.set(name, value)
  }
  return ok()
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

    throw new RedisCommandError(
      `Unknown CONFIG subcommand or wrong number of arguments for '${args.subcommand}'. Try CONFIG HELP.`,
    )
  },
})

export const configCommands = [configCommand]
