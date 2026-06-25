import { describe, test } from 'node:test'
import assert from 'node:assert'

import {
  ClientSession,
  REDIS_CLUSTER_SLOT_COUNT,
  RedisClusterTopology,
  RedisResult,
  RedisServerState,
  RedisValue,
  createClusterCommands,
  createClusterPolicy,
  createRedisCommandExecutor,
  type CompatibilitySpec,
} from '../../src/internal'

function createSession(compatibility: CompatibilitySpec): ClientSession {
  const server = new RedisServerState({ compatibility, databaseCount: 16 })
  const executor = createRedisCommandExecutor({ compatibility: server.profile })
  return new ClientSession({ server, executor })
}

function createClusterSession(compatibility: CompatibilitySpec): ClientSession {
  const topology = new RedisClusterTopology([
    {
      id: 'local',
      role: 'master',
      host: '127.0.0.1',
      port: 7000,
      slots: [[0, REDIS_CLUSTER_SLOT_COUNT - 1]],
    },
  ])
  const server = new RedisServerState({
    compatibility,
    clusterTopology: topology,
    databaseCount: 16,
  })
  const executor = createRedisCommandExecutor({
    compatibility: server.profile,
    extraCommands: createClusterCommands('local'),
    policies: [createClusterPolicy({ localNodeId: 'local', topology })],
  })
  return new ClientSession({ server, executor })
}

function buf(...values: string[]): Buffer[] {
  return values.map(value => Buffer.from(value))
}

function assertError(result: RedisResult, pattern: RegExp): void {
  assert.strictEqual(result.value.kind, 'error')
  assert.match(result.value.message, pattern)
}

describe('compatibility behavior gates', () => {
  test('EXPIRE conditions are rejected before Redis 7.0', async () => {
    const redis62 = createSession('redis-6.2')
    await redis62.execute('set', buf('k', 'v'))
    assertError(
      (await redis62.execute('expire', buf('k', '10', 'NX'))) as RedisResult,
      /wrong number of arguments/i,
    )

    const redis70 = createSession('redis-7.0')
    await redis70.execute('set', buf('k', 'v'))
    const result = (await redis70.execute(
      'expire',
      buf('k', '10', 'NX'),
    )) as RedisResult
    assert.deepStrictEqual(result.value, { kind: 'integer', value: 1 })
  })

  test('SET newer options follow their feature gates', async () => {
    const redis60 = createSession({ flavor: 'redis', version: '6.0.0' })
    assertError(
      (await redis60.execute('set', buf('k', 'v', 'GET'))) as RedisResult,
      /syntax/i,
    )

    const redis62 = createSession('redis-6.2')
    const result = (await redis62.execute(
      'set',
      buf('k', 'v', 'GET'),
    )) as RedisResult
    assert.deepStrictEqual(result.value, { kind: 'bulk-string', value: null })
  })

  test('COMMAND subcommands follow their feature gates', async () => {
    const redis62 = createSession('redis-6.2')
    assertError(
      (await redis62.execute('command', buf('docs'))) as RedisResult,
      /unknown subcommand/i,
    )

    const redis70 = createSession('redis-7.0')
    const result = (await redis70.execute(
      'command',
      buf('docs'),
    )) as RedisResult
    assert.strictEqual(result.value.kind, 'map')
  })

  test('COMMAND introspection hides gated subcommands', async () => {
    const redis62 = createSession('redis-6.2')
    const redis62Info = (await redis62.execute(
      'command',
      buf('info', 'command'),
    )) as RedisResult
    assert.deepStrictEqual(commandSubcommandNames(redis62Info), [
      'command|getkeys',
      'command|info',
      'command|count',
      'command|list',
      'command|help',
    ])

    const redis70 = createSession('redis-7.0')
    const redis70Info = (await redis70.execute(
      'command',
      buf('info', 'command'),
    )) as RedisResult
    assert.ok(commandSubcommandNames(redis70Info).includes('command|docs'))
    assert.ok(
      commandSubcommandNames(redis70Info).includes('command|getkeysandflags'),
    )
  })

  test('CLIENT SETINFO follows its feature gate', async () => {
    const redis70 = createSession('redis-7.0')
    assertError(
      (await redis70.execute(
        'client',
        buf('setinfo', 'lib-name', 'test'),
      )) as RedisResult,
      /unknown subcommand/i,
    )

    const redis72 = createSession('redis-7.2')
    assert.deepStrictEqual(
      await redis72.execute('client', buf('setinfo', 'lib-name', 'test')),
      RedisResult.ok(),
    )
  })

  test('Valkey cluster profile allows non-zero SELECT', async () => {
    const redis = createClusterSession('redis-7.4')
    assert.deepStrictEqual(
      await redis.execute('select', buf('1')),
      RedisResult.error('SELECT is not allowed in cluster mode', 'ERR'),
    )

    const valkey = createClusterSession('valkey-9.0')
    assert.deepStrictEqual(
      await valkey.execute('select', buf('1')),
      RedisResult.ok(),
    )
  })

  test('INFO and HELLO report the selected profile', async () => {
    const redis62 = createSession('redis-6.2')
    const info = (await redis62.execute('info', buf('server'))) as RedisResult
    assert.strictEqual(info.value.kind, 'bulk-string')
    assert.ok(info.value.value)
    assert.match(info.value.value.toString(), /^redis_version:6\.2\.14$/m)

    const redisHello = (await redis62.execute('hello', buf('2'))) as RedisResult
    assert.strictEqual(helloField(redisHello, 'server'), 'redis')
    assert.strictEqual(helloField(redisHello, 'version'), '6.2.14')

    const valkey = createSession('valkey-9.0')
    const valkeyInfo = (await valkey.execute(
      'info',
      buf('server'),
    )) as RedisResult
    assert.strictEqual(valkeyInfo.value.kind, 'bulk-string')
    assert.ok(valkeyInfo.value.value)
    assert.match(valkeyInfo.value.value.toString(), /^server_name:valkey$/m)
    assert.match(valkeyInfo.value.value.toString(), /^valkey_version:9\.0\.0$/m)

    const valkeyHello = (await valkey.execute('hello', buf('2'))) as RedisResult
    assert.strictEqual(helloField(valkeyHello, 'server'), 'valkey')
    assert.strictEqual(helloField(valkeyHello, 'version'), '9.0.0')
  })
})

function commandSubcommandNames(result: RedisResult): string[] {
  assert.strictEqual(result.value.kind, 'array')
  const [commandInfo] = result.value.items
  assert.strictEqual(commandInfo.kind, 'array')
  const subcommands = commandInfo.items[9]
  assert.strictEqual(subcommands.kind, 'array')
  return subcommands.items.map(subcommand => {
    assert.strictEqual(subcommand.kind, 'array')
    const name = subcommand.items[0]
    assert.strictEqual(name.kind, 'bulk-string')
    assert.ok(name.value)
    return name.value.toString()
  })
}

function helloField(result: RedisResult, key: string): string {
  assert.strictEqual(result.value.kind, 'array')
  for (let index = 0; index < result.value.items.length; index += 2) {
    const field = result.value.items[index]
    const value = result.value.items[index + 1]
    if (bulkStringText(field) === key) {
      return bulkStringText(value)
    }
  }
  throw new Error(`Missing HELLO field ${key}`)
}

function bulkStringText(value: RedisValue): string {
  assert.strictEqual(value.kind, 'bulk-string')
  assert.ok(value.value)
  return value.value.toString()
}
