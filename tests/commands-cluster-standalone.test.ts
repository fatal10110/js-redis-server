import { describe, test } from 'node:test'
import assert from 'node:assert'

import {
  ClientSession,
  REDIS_CLUSTER_SLOT_COUNT,
  RedisClusterTopology,
  RedisServerState,
  createClusterCommands,
  createClusterPolicy,
  createRedisCommandExecutor,
  type CompatibilitySpec,
} from '../src/internal'
import type { RedisResult } from '../src/core/redis-result'
import type { RedisValue } from '../src/core/redis-value'

const PRESETS = [
  'redis-6.2',
  'redis-7.0',
  'redis-7.2',
  'redis-7.4',
  'redis-8.0',
  'valkey-8.0',
  'valkey-9.0',
] as const

const DISABLED = 'This instance has cluster support disabled'

function standalone(compatibility: CompatibilitySpec): ClientSession {
  const server = new RedisServerState({ compatibility })
  const executor = createRedisCommandExecutor({ compatibility: server.profile })
  return new ClientSession({ server, executor })
}

function clusterNode(compatibility: CompatibilitySpec): ClientSession {
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
  })
  const executor = createRedisCommandExecutor({
    compatibility: server.profile,
    extraCommands: createClusterCommands('local'),
    policies: [createClusterPolicy({ localNodeId: 'local', topology })],
  })
  return new ClientSession({ server, executor })
}

function run(session: ClientSession, ...args: string[]): Promise<RedisResult> {
  return session.execute(
    args[0],
    args.slice(1).map(arg => Buffer.from(arg)),
  )
}

function errorMessage(result: RedisResult): string {
  assert.strictEqual(result.value.kind, 'error', JSON.stringify(result.value))
  return result.value.message
}

function isOk(result: RedisResult): boolean {
  return result.value.kind === 'simple-string' && result.value.value === 'OK'
}

function names(value: RedisValue): string[] {
  assert.strictEqual(value.kind, 'array')
  return value.items.map(entry => {
    assert.strictEqual(entry.kind, 'array')
    const name = entry.items[0]
    assert.strictEqual(name.kind, 'bulk-string')
    return name.value?.toString() ?? ''
  })
}

describe('standalone CLUSTER / READONLY / READWRITE (#537)', () => {
  for (const preset of PRESETS) {
    const legacy = preset === 'redis-6.2'
    const valkey8 = preset === 'valkey-8.0' || preset === 'valkey-9.0'

    test(`CLUSTER is refused as disabled on ${preset}`, async () => {
      const session = standalone(preset)
      for (const sub of ['INFO', 'MYID', 'SLOTS', 'NODES', 'HELP']) {
        assert.strictEqual(
          errorMessage(await run(session, 'CLUSTER', sub)),
          DISABLED,
        )
      }
      assert.strictEqual(
        errorMessage(await run(session, 'CLUSTER', 'BOGUS')),
        legacy ? DISABLED : "unknown subcommand 'BOGUS'. Try CLUSTER HELP.",
      )
      assert.strictEqual(
        errorMessage(await run(session, 'CLUSTER', 'INFO', 'x')),
        legacy
          ? DISABLED
          : "wrong number of arguments for 'cluster|info' command",
      )
    })

    test(`READONLY / READWRITE on ${preset}`, async () => {
      const session = standalone(preset)
      const readonly = await run(session, 'READONLY')
      const readwrite = await run(session, 'READWRITE')
      if (valkey8) {
        assert.ok(isOk(readonly))
      } else {
        assert.strictEqual(errorMessage(readonly), DISABLED)
      }
      if (legacy || valkey8) {
        assert.ok(isOk(readwrite))
      } else {
        assert.strictEqual(errorMessage(readwrite), DISABLED)
      }
    })

    test(`COMMAND describes them in standalone mode on ${preset}`, async () => {
      const session = standalone(preset)
      const info = await run(
        session,
        'COMMAND',
        'INFO',
        'cluster',
        'readonly',
        'readwrite',
      )
      assert.deepStrictEqual(names(info.value), [
        'cluster',
        'readonly',
        'readwrite',
      ])
      const listed = names((await run(session, 'COMMAND')).value)
      for (const name of ['cluster', 'readonly', 'readwrite']) {
        assert.ok(listed.includes(name), name)
      }
      const count = await run(session, 'COMMAND', 'COUNT')
      assert.deepStrictEqual(count.value, {
        kind: 'integer',
        value: listed.length,
      })
      assert.strictEqual(
        errorMessage(
          await run(session, 'COMMAND', 'GETKEYS', 'CLUSTER', 'INFO'),
        ),
        'The command has no key arguments',
      )
    })
  }

  test('cluster mode is unchanged', async () => {
    for (const preset of PRESETS) {
      const session = clusterNode(preset)
      const info = await run(session, 'CLUSTER', 'MYID')
      assert.strictEqual(info.value.kind, 'bulk-string')
      assert.strictEqual(info.value.value?.toString(), 'local')
      assert.ok(isOk(await run(session, 'READONLY')))
      assert.ok(isOk(await run(session, 'READWRITE')))
    }
  })

  test('MULTI queues CLUSTER and EXEC answers it as disabled', async () => {
    const session = standalone('redis-8.0')
    assert.ok(isOk(await run(session, 'MULTI')))
    await run(session, 'CLUSTER', 'INFO')
    await run(session, 'READONLY')
    const exec = await run(session, 'EXEC')
    assert.strictEqual(exec.value.kind, 'array')
    assert.deepStrictEqual(
      exec.value.items.map(item => item.kind === 'error' && item.message),
      [DISABLED, DISABLED],
    )
  })

  test("a script's redis.call gets the disabled error", async () => {
    // Real redis-server 7.0.15 answers both exactly so.
    const session = standalone('redis-8.0')
    assert.strictEqual(
      errorMessage(
        await run(session, 'EVAL', "return redis.call('cluster','info')", '0'),
      ),
      `${DISABLED} script: 080f2e8de25b3caf214921ec6c8c5af604cc9e4a, on @user_script:1.`,
    )
    assert.strictEqual(
      errorMessage(
        await run(session, 'EVAL', "return redis.pcall('readonly')", '0'),
      ),
      DISABLED,
    )
  })
})
