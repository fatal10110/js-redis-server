import { describe, test } from 'node:test'
import assert from 'node:assert'
import {
  ClientSession,
  RedisServerState,
  createClusterCommands,
  createRedisCommandExecutor,
} from '../../src/internal'
import {
  resolveCompatibilityProfile,
  type CompatibilitySpec,
} from '../../src/core/compatibility'
import { commandTableEntry } from '../../src/core/compatibility/command-table'
import {
  defineCommand,
  type CommandDefinition,
} from '../../src/core/command-definition'
import { t } from '../../src/core/command-schema'
import { RedisResult } from '../../src/core/redis-result'
import type { RedisValue } from '../../src/core/redis-value'

const PRESETS = [
  'redis-6.2',
  'redis-7.0',
  'redis-7.2',
  'redis-7.4',
  'redis-8.0',
  'valkey-8.0',
  'valkey-9.0',
] as const

function session(
  compatibility: CompatibilitySpec,
  extraCommands?: readonly CommandDefinition[],
): ClientSession {
  const server = new RedisServerState({ compatibility })
  const executor = createRedisCommandExecutor({
    compatibility: server.profile,
    extraCommands,
  })
  return new ClientSession({ server, executor })
}

function text(value: RedisValue): string {
  if (value.kind === 'simple-string') {
    return value.value
  }
  assert.strictEqual(value.kind, 'bulk-string')
  assert.ok(value.value)
  return value.value.toString()
}

function items(value: RedisValue): RedisValue[] {
  assert.ok(value.kind === 'array' || value.kind === 'set', value.kind)
  return value.items
}

// Every entry COMMAND lists: the name and its flags, with subcommands.
function listed(value: RedisValue): Map<string, string[]> {
  const out = new Map<string, string[]>()
  const visit = (entry: RedisValue) => {
    const fields = items(entry)
    out.set(text(fields[0]), items(fields[2]).map(text))
    if (fields[9]) {
      items(fields[9]).forEach(visit)
    }
  }
  items(value).forEach(visit)
  return out
}

describe('real command table (#494)', () => {
  for (const preset of PRESETS) {
    test(`every entry COMMAND lists on ${preset} comes from the table`, async () => {
      const profile = resolveCompatibilityProfile(preset)
      const result = await session(preset).execute('command', [])
      const entries = listed(result.value)
      assert.ok(entries.size > 150)
      for (const [name, flags] of entries) {
        const real = commandTableEntry(name, profile)
        assert.ok(real, `${name} has no ${preset} table entry`)
        assert.deepStrictEqual(flags, real.flags, name)
      }
    })

    // A cluster node registers CLUSTER, READONLY and READWRITE through
    // extraCommands; they are in the real table too.
    test(`every entry a cluster node lists on ${preset} comes from the table`, async () => {
      const profile = resolveCompatibilityProfile(preset)
      const result = await session(
        preset,
        createClusterCommands('node-1'),
      ).execute('command', [])
      const entries = listed(result.value)
      for (const name of ['cluster', 'readonly', 'readwrite']) {
        assert.ok(entries.has(name), `${name} is not listed on ${preset}`)
      }
      for (const [name, flags] of entries) {
        const real = commandTableEntry(name, profile)
        assert.ok(real, `${name} has no ${preset} table entry`)
        assert.deepStrictEqual(flags, real.flags, name)
      }
    })
  }

  test('cluster-mode commands report real metadata', async () => {
    // Real redis 8.0.6 COMMAND INFO readonly / cluster|info / cluster|slots.
    const client = session('redis-8.0', createClusterCommands('node-1'))
    const info = await client.execute('command', [
      Buffer.from('info'),
      Buffer.from('readonly'),
      Buffer.from('cluster|info'),
      Buffer.from('cluster|slots'),
      Buffer.from('cluster'),
    ])
    const [readonly, clusterInfo, clusterSlots, cluster] = items(
      info.value,
    ).map(items)
    assert.deepStrictEqual(items(readonly[2]).map(text), [
      'loading',
      'stale',
      'fast',
    ])
    assert.deepStrictEqual(items(readonly[6]).map(text), [
      '@fast',
      '@connection',
    ])
    assert.deepStrictEqual(items(clusterInfo[2]).map(text), ['stale'])
    assert.deepStrictEqual(items(clusterInfo[7]).map(text), [
      'nondeterministic_output',
    ])
    assert.deepStrictEqual(items(clusterSlots[2]).map(text), [
      'loading',
      'stale',
    ])
    assert.deepStrictEqual(items(cluster[2]).map(text), [])
    assert.deepStrictEqual(items(cluster[6]).map(text), ['@slow'])

    // Redis 6.2.24: CLUSTER is `admin random stale`, READONLY `fast`.
    const client62 = session('redis-6.2', createClusterCommands('node-1'))
    const info62 = await client62.execute('command', [
      Buffer.from('info'),
      Buffer.from('cluster'),
      Buffer.from('readonly'),
    ])
    const [cluster62, readonly62] = items(info62.value).map(items)
    assert.deepStrictEqual(items(cluster62[2]).map(text), [
      'admin',
      'random',
      'stale',
    ])
    assert.deepStrictEqual(items(readonly62[2]).map(text), ['fast'])
    assert.deepStrictEqual(items(readonly62[6]).map(text), [
      '@keyspace',
      '@fast',
    ])
  })

  test('each profile reads the table of its version', () => {
    const hexpire = (spec: CompatibilitySpec) =>
      commandTableEntry('hexpire', resolveCompatibilityProfile(spec))?.flags
    // Redis 7.4.4 marks HEXPIRE denyoom; 8.0.6 does not.
    assert.deepStrictEqual(hexpire('redis-7.4'), ['write', 'denyoom', 'fast'])
    assert.deepStrictEqual(hexpire('redis-8.0'), ['write', 'fast'])
    // A version between two captures reads the older one; one past the
    // newest reads the newest.
    assert.deepStrictEqual(
      hexpire({ flavor: 'redis', version: '7.9.0' }),
      hexpire('redis-7.4'),
    )
    assert.deepStrictEqual(
      hexpire({ flavor: 'redis', version: '9.0.0' }),
      hexpire('redis-8.0'),
    )

    // Valkey 8.0 marks GEORADIUS's STORE specs variable_flags; Valkey 7.2
    // shares Redis 7.2's table, which does not.
    const storeFlags = (spec: CompatibilitySpec) =>
      commandTableEntry('georadius', resolveCompatibilityProfile(spec))
        ?.keySpecs?.[1].flags
    assert.deepStrictEqual(storeFlags('valkey-8.0'), [
      'OW',
      'update',
      'variable_flags',
    ])
    assert.deepStrictEqual(storeFlags({ flavor: 'valkey', version: '7.2.0' }), [
      'OW',
      'update',
    ])
    assert.deepStrictEqual(storeFlags('redis-8.0'), ['OW', 'update'])

    // Redis 6.2 has no tips or key specs, and no subcommand entries.
    const redis62 = resolveCompatibilityProfile('redis-6.2')
    assert.deepStrictEqual(commandTableEntry('get', redis62), {
      flags: ['readonly', 'fast'],
      categories: ['@read', '@string', '@fast'],
    })
    assert.strictEqual(commandTableEntry('xinfo|stream', redis62), undefined)
    // A command a version does not have has no entry.
    assert.strictEqual(
      commandTableEntry('hgetex', resolveCompatibilityProfile('redis-7.4')),
      undefined,
    )
  })

  test('a command the table does not know keeps what it declares', async () => {
    const custom = defineCommand({
      name: 'mycmd',
      schema: t.object({ key: t.key() }),
      flags: ['readonly'],
      introspection: {
        flags: ['readonly', 'fast'],
        categories: ['@read', '@fast'],
        keySpecs: [
          { flags: ['RO'], beginSearchIndex: 1, lastKey: 0, keyStep: 1 },
        ],
      },
      keys: args => [args.key],
      execute: () => RedisResult.ok(),
    })
    const client = session('redis-8.0', [custom as CommandDefinition])

    const info = await client.execute('command', [
      Buffer.from('info'),
      Buffer.from('mycmd'),
    ])
    const [entry] = items(info.value)
    const fields = items(entry)
    assert.deepStrictEqual(items(fields[2]).map(text), ['readonly', 'fast'])
    assert.deepStrictEqual(items(fields[6]).map(text), ['@read', '@fast'])
    assert.strictEqual(items(fields[8]).length, 1)

    const keys = await client.execute('command', [
      Buffer.from('getkeysandflags'),
      Buffer.from('mycmd'),
      Buffer.from('k'),
    ])
    const [key] = items(keys.value)
    assert.deepStrictEqual(items(items(key)[1]).map(text), ['RO'])
  })
})
