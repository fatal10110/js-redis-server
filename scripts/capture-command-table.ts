/**
 * Regenerates `src/core/compatibility/command-table-data.ts`: the flags, ACL
 * categories, tips and key specs real servers report in `COMMAND INFO` for
 * every command and subcommand this server registers (#494).
 *
 * It needs one server per preset profile, each given as `<preset>=<port>`:
 *
 *   docker run -d --rm --name ct-62 -p 47462:6379 redis:6.2.24
 *   docker run -d --rm --name ct-70 -p 47470:6379 redis:7.0.15
 *   docker run -d --rm --name ct-72 -p 47472:6379 redis:7.2.4
 *   docker run -d --rm --name ct-74 -p 47474:6379 redis:7.4.4
 *   docker run -d --rm --name ct-80 -p 47480:6379 redis:8.0.6
 *   docker run -d --rm --name ct-v80 -p 47481:6379 valkey/valkey:8.0.11
 *   docker run -d --rm --name ct-v90 -p 47490:6379 valkey/valkey:9.0.6
 *   node --import tsx scripts/capture-command-table.ts \
 *     redis-6.2=47462 redis-7.0=47470 redis-7.2=47472 redis-7.4=47474 \
 *     redis-8.0=47480 valkey-8.0=47481 valkey-9.0=47490
 *
 * Use exactly these versions (see `TABLES` in command-table.ts for why).
 * The names asked for include the cluster-mode commands (CLUSTER and its
 * subcommands, READONLY, READWRITE); a standalone server knows them too.
 * Only `COMMAND INFO` is sent. Redis 8.0 is written out in full; every other
 * version is written as the entries that differ from the version it is
 * derived from (see `TABLES` in command-table.ts). A command this server
 * registers on a profile but the matching server does not know is left out
 * and reported, so its declared introspection answers.
 */
import { execFileSync } from 'node:child_process'
import { writeFileSync } from 'node:fs'
import { resolve } from 'node:path'
import Redis from 'ioredis'
import type { CommandKeySpec } from '../src/core/command-definition'
import type { CommandTableEntry } from '../src/core/compatibility/command-table'
import {
  createClusterCommands,
  createRedisCommandExecutor,
} from '../src/internal'
import type { CompatibilitySpec } from '../src/core/compatibility'

const OUTPUT = resolve(
  __dirname,
  '../src/core/compatibility/command-table-data.ts',
)

// Each preset, the constant it is written as, and the preset it is a delta
// against (none for the base). Keep in step with `TABLES` in command-table.ts.
const PRESETS: Array<{
  preset: CompatibilitySpec & string
  constant: string
  parent?: string
  extended: boolean
}> = [
  { preset: 'redis-8.0', constant: 'REDIS_80', extended: true },
  {
    preset: 'redis-7.4',
    constant: 'REDIS_74',
    parent: 'redis-8.0',
    extended: true,
  },
  {
    preset: 'redis-7.2',
    constant: 'REDIS_72',
    parent: 'redis-7.4',
    extended: true,
  },
  {
    preset: 'redis-7.0',
    constant: 'REDIS_70',
    parent: 'redis-7.2',
    extended: true,
  },
  {
    preset: 'redis-6.2',
    constant: 'REDIS_62',
    parent: 'redis-7.0',
    extended: false,
  },
  {
    preset: 'valkey-8.0',
    constant: 'VALKEY_80',
    parent: 'redis-7.2',
    extended: true,
  },
  {
    preset: 'valkey-9.0',
    constant: 'VALKEY_90',
    parent: 'valkey-8.0',
    extended: true,
  },
]

type Table = Map<string, CommandTableEntry>
type Reply = string | number | null | Reply[]

// Every command and subcommand this server registers on `preset`, in
// standalone and in cluster mode: cluster nodes add CLUSTER, READONLY and
// READWRITE through `extraCommands`, and real standalone servers answer
// COMMAND INFO for those too.
function registeredNames(preset: CompatibilitySpec): string[] {
  const names: string[] = []
  const executor = createRedisCommandExecutor({
    compatibility: preset,
    extraCommands: createClusterCommands('capture'),
  })
  for (const definition of executor.getCommandDefinitions()) {
    names.push(definition.name)
    for (const subcommand of definition.introspection?.subcommands ?? []) {
      if (subcommand.name) {
        names.push(subcommand.name)
      }
    }
  }
  return names
}

function pairs(reply: Reply): Map<string, Reply> {
  const out = new Map<string, Reply>()
  const items = reply as Reply[]
  for (let i = 0; i < items.length; i += 2) {
    out.set(String(items[i]), items[i + 1])
  }
  return out
}

function strings(reply: Reply): string[] {
  return (reply as Reply[]).map(String)
}

function keySpec(reply: Reply): CommandKeySpec {
  const fields = pairs(reply)
  const begin = pairs(fields.get('begin_search') as Reply)
  const find = pairs(fields.get('find_keys') as Reply)
  const beginSpec = pairs(begin.get('spec') as Reply)
  const findSpec = pairs(find.get('spec') as Reply)
  const spec: {
    -readonly [K in keyof CommandKeySpec]: CommandKeySpec[K]
  } = {
    flags: strings(fields.get('flags') as Reply),
    beginSearchIndex: 0,
    lastKey: 0,
    keyStep: 0,
  }
  const notes = fields.get('notes')
  if (typeof notes === 'string') {
    spec.notes = notes
  }

  switch (begin.get('type')) {
    case 'index':
      spec.beginSearchIndex = Number(beginSpec.get('index'))
      break
    case 'keyword':
      spec.beginSearchKeyword = {
        keyword: String(beginSpec.get('keyword')),
        startFrom: Number(beginSpec.get('startfrom')),
      }
      break
    case 'unknown':
      spec.beginSearchUnknown = true
      break
    default:
      throw new Error(`unexpected begin_search ${String(begin.get('type'))}`)
  }

  switch (find.get('type')) {
    case 'range': {
      spec.lastKey = Number(findSpec.get('lastkey'))
      spec.keyStep = Number(findSpec.get('keystep'))
      const limit = Number(findSpec.get('limit'))
      if (limit !== 0) {
        spec.limit = limit
      }
      break
    }
    case 'keynum':
      spec.keyStep = 1
      spec.findKeysKeynum = {
        keyNumIdx: Number(findSpec.get('keynumidx')),
        firstKey: Number(findSpec.get('firstkey')),
        keyStep: Number(findSpec.get('keystep')),
      }
      break
    case 'unknown':
      spec.findKeysUnknown = true
      break
    default:
      throw new Error(`unexpected find_keys ${String(find.get('type'))}`)
  }
  return spec
}

function entry(info: Reply[], extended: boolean): CommandTableEntry {
  const flags = strings(info[2])
  const categories = strings(info[6])
  if (!extended) {
    return { flags, categories }
  }
  const tips = strings(info[7])
  const keySpecs = (info[8] as Reply[]).map(keySpec)
  return {
    flags,
    categories,
    ...(tips.length > 0 ? { tips } : {}),
    ...(keySpecs.length > 0 ? { keySpecs } : {}),
  }
}

async function capture(
  preset: CompatibilitySpec & string,
  port: number,
  extended: boolean,
): Promise<Table> {
  const names = registeredNames(preset)
  const redis = new Redis({ port, lazyConnect: true })
  await redis.connect()
  try {
    const infos = (await redis.call('COMMAND', 'INFO', ...names)) as Reply[]
    const table: Table = new Map()
    const missing: string[] = []
    names.forEach((name, i) => {
      const info = infos[i]
      if (info === null) {
        // Redis 6.2 has no subcommand entries at all.
        if (extended || !name.includes('|')) {
          missing.push(name)
        }
        return
      }
      table.set(name, entry(info as Reply[], extended))
    })
    if (missing.length > 0) {
      console.warn(`${preset}: the server does not know ${missing.join(' ')}`)
    }
    return table
  } finally {
    redis.disconnect()
  }
}

const FIELDS = ['flags', 'categories', 'tips', 'keySpecs'] as const

function delta(
  table: Table,
  parent: Table,
  extended: boolean,
): Map<string, Partial<CommandTableEntry> | null> {
  const out = new Map<string, Partial<CommandTableEntry> | null>()
  for (const name of new Set([...parent.keys(), ...table.keys()])) {
    const child = table.get(name)
    const base = parent.get(name)
    if (!child) {
      if (base) {
        out.set(name, null)
      }
      continue
    }
    if (!base) {
      out.set(name, child)
      continue
    }

    const changed: Partial<Record<(typeof FIELDS)[number], unknown>> = {}
    for (const field of extended ? FIELDS : FIELDS.slice(0, 2)) {
      const value = child[field] ?? []
      if (JSON.stringify(value) !== JSON.stringify(base[field] ?? [])) {
        changed[field] = value
      }
    }
    if (Object.keys(changed).length > 0) {
      out.set(name, changed as Partial<CommandTableEntry>)
    }
  }
  return out
}

function literal(
  entries: Map<string, Partial<CommandTableEntry> | null>,
): string {
  const lines = [...entries]
    .sort(([a], [b]) => a.localeCompare(b))
    .map(
      ([name, value]) => `  ${JSON.stringify(name)}: ${JSON.stringify(value)},`,
    )
  return `{\n${lines.join('\n')}\n}`
}

async function main(): Promise<void> {
  const ports = new Map<string, number>()
  for (const arg of process.argv.slice(2)) {
    const [preset, port] = arg.split('=')
    ports.set(preset, Number(port))
  }
  const absent = PRESETS.filter(({ preset }) => !ports.has(preset))
  if (absent.length > 0) {
    console.error(
      `usage: capture-command-table.ts ${PRESETS.map(({ preset }) => `${preset}=<port>`).join(' ')}`,
    )
    process.exit(1)
  }

  const tables = new Map<string, Table>()
  const versions: string[] = []
  for (const { preset, extended } of PRESETS) {
    const port = ports.get(preset) as number
    tables.set(preset, await capture(preset, port, extended))
    const redis = new Redis({ port, lazyConnect: true })
    await redis.connect()
    const info = await redis.info('server')
    redis.disconnect()
    const version =
      /valkey_version:(\S+)/.exec(info)?.[1] ??
      /redis_version:(\S+)/.exec(info)?.[1]
    versions.push(`${preset.split('-')[0]} ${version}`)
  }

  const out: string[] = [
    '// Generated by scripts/capture-command-table.ts from ' +
      `${versions.join(', ')}. Do not edit by hand.`,
    "import type { CommandTableDelta, CommandTableEntry } from './command-table'",
    '',
  ]
  for (const { preset, constant, parent, extended } of PRESETS) {
    const table = tables.get(preset) as Table
    if (!parent) {
      out.push(
        `export const ${constant}: Readonly<Record<string, CommandTableEntry>> = ${literal(table)}`,
        '',
      )
      continue
    }
    out.push(
      `export const ${constant}: CommandTableDelta = ${literal(delta(table, tables.get(parent) as Table, extended))}`,
      '',
    )
  }

  writeFileSync(OUTPUT, out.join('\n'))
  execFileSync('npx', ['prettier', '--write', OUTPUT], { stdio: 'inherit' })
  console.log(`wrote ${OUTPUT}`)
}

main().catch(err => {
  console.error(err)
  process.exit(1)
})
