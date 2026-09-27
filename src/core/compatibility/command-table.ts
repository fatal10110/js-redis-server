import type { CommandKeySpec } from '../command-definition'
import {
  REDIS_62,
  REDIS_70,
  REDIS_72,
  REDIS_74,
  REDIS_80,
  VALKEY_80,
  VALKEY_90,
} from './command-table-data'
import {
  gateSatisfied,
  type CompatibilityProfile,
  type VersionGate,
} from './profile'

/**
 * A command-table entry's `COMMAND INFO` metadata that nothing in a command's
 * definition can derive: its flags, ACL categories, tips and key specs, as a
 * real server reports them. Arity and the legacy first/last/step key range are
 * not here. Arity comes from the definition (`commandTableArity`); the key
 * range is folded from these key specs on 7.0+ and derived from the schema
 * on 6.2 or when an entry has no key specs (`legacyKeyRange`).
 * tests/core/command-schema-layout.test.ts keeps the two in agreement.
 */
export type CommandTableEntry = {
  readonly flags: readonly string[]
  readonly categories: readonly string[]
  readonly tips?: readonly string[]
  readonly keySpecs?: readonly CommandKeySpec[]
}

/**
 * The entries of one version that differ from the version it is derived from:
 * the fields that changed, or `null` for a command it does not have.
 */
export type CommandTableDelta = Readonly<
  Record<string, Partial<CommandTableEntry> | null>
>

type TableSource = {
  id: string
  gate: VersionGate
  base: Readonly<Record<string, CommandTableEntry>> | (() => ResolvedTable)
  delta?: CommandTableDelta
  /** Whether the version has tips and key specs (Redis 7.0+ / Valkey 7.2+). */
  extended: boolean
}

type ResolvedTable = ReadonlyMap<string, CommandTableEntry>

const cache = new Map<string, ResolvedTable>()

function resolved(source: TableSource): ResolvedTable {
  const hit = cache.get(source.id)
  if (hit) {
    return hit
  }

  const table = new Map(
    typeof source.base === 'function'
      ? source.base()
      : Object.entries(source.base),
  )
  for (const [name, change] of Object.entries(source.delta ?? {})) {
    if (change === null) {
      table.delete(name)
      continue
    }
    const previous = table.get(name)
    table.set(name, { flags: [], categories: [], ...previous, ...change })
  }
  if (!source.extended) {
    for (const [name, { flags, categories }] of table) {
      table.set(name, { flags, categories })
    }
  }
  cache.set(source.id, table)
  return table
}

const redis80: TableSource = {
  id: 'redis-8.0',
  gate: { redis: '8.0.0' },
  base: REDIS_80,
  extended: true,
}
const redis74: TableSource = {
  id: 'redis-7.4',
  gate: { redis: '7.4.0' },
  base: () => resolved(redis80),
  delta: REDIS_74,
  extended: true,
}
const redis72: TableSource = {
  id: 'redis-7.2',
  gate: { redis: '7.2.0', valkey: '7.2.0' },
  base: () => resolved(redis74),
  delta: REDIS_72,
  extended: true,
}
const redis70: TableSource = {
  id: 'redis-7.0',
  gate: { redis: '7.0.0' },
  base: () => resolved(redis72),
  delta: REDIS_70,
  extended: true,
}
const redis62: TableSource = {
  id: 'redis-6.2',
  gate: { redis: '0.0.0', valkey: '0.0.0' },
  base: () => resolved(redis70),
  delta: REDIS_62,
  extended: false,
}
const valkey80: TableSource = {
  id: 'valkey-8.0',
  gate: { valkey: '8.0.0' },
  base: () => resolved(redis72),
  delta: VALKEY_80,
  extended: true,
}
const valkey90: TableSource = {
  id: 'valkey-9.0',
  gate: { valkey: '9.0.0' },
  base: () => resolved(valkey80),
  delta: VALKEY_90,
  extended: true,
}

/**
 * The captured command tables, newest first: a profile reads the first one
 * whose gate it satisfies. Captured by scripts/capture-command-table.ts from
 * the patch releases the rest of the compatibility gates are verified
 * against: redis-server 6.2.24, 7.0.15, 7.2.4, 7.4.4, 8.0.6 (8.0.0 answers
 * the same) and valkey 8.0.11 / 9.0.6. Valkey 7.2 reads Redis 7.2's table.
 * Patch releases do change the table, so recapture from the same versions:
 * 7.4.11 marks the SUBSCRIBE family `denyoom` and GEORADIUS's STORE specs
 * `incomplete`; valkey 8.0.0 lacks the `variable_flags` 8.0.11 has. Some
 * presets are versioned below the patch captured for them and so answer as
 * the captured patch, not as their nominal version: the redis-6.2 preset
 * (6.2.14) reports 6.2.24's `denyoom` on SUBSCRIBE / PSUBSCRIBE, which 6.2.14
 * does not have, and the valkey-8.0 / valkey-9.0 presets (8.0.0 / 9.0.0)
 * report 8.0.11 / 9.0.6's metadata. Redis 8.0 is stored whole, every other
 * version as its differences from the one it is derived from.
 */
const TABLES: readonly TableSource[] = [
  valkey90,
  valkey80,
  redis80,
  redis74,
  redis72,
  redis70,
  redis62,
]

/**
 * The real command-table entry `name` (`container|subcommand` for a
 * subcommand) has on `profile`, or `undefined` when the table this server was
 * captured against has none: a command added with `extraCommands` under a
 * name real Redis does not have, or one the real server of that version does
 * not have. Tips and key specs are left out
 * on Redis 6.2, which has neither.
 */
export function commandTableEntry(
  name: string,
  profile: CompatibilityProfile,
): CommandTableEntry | undefined {
  // redis62's gate is satisfied by every profile, so a table is always found.
  const source = TABLES.find(table => gateSatisfied(table.gate, profile))
  return source ? resolved(source).get(name) : undefined
}
