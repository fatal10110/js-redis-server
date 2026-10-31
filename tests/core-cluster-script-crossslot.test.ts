import { describe, test } from 'node:test'
import assert from 'node:assert'
import { createHash } from 'node:crypto'
import {
  ClientSession,
  RedisClusterTopology,
  RedisServerState,
  createClusterPolicy,
  createClusterCommands,
  createRedisCommandExecutor,
} from '../src/internal'
import { encodeRedisResult } from '../src/core/resp-encoder'

// #618: a command run from a script whose own keys hash to different slots
// answers the scripting layer's error (scriptVerifyClusterState's CROSSSLOT
// branch), not -CROSSSLOT. Real replies: redis 6.2.14, 7.0.15, 7.2.4, 7.4.4,
// 8.0.0 and valkey 8.0.0, 9.0.0, as single-node clusters with
// `--cluster-require-full-coverage no` and a few slots assigned.

type ProfileName =
  | 'redis-6.2'
  | 'redis-7.0'
  | 'redis-7.2'
  | 'redis-7.4'
  | 'redis-8.0'
  | 'valkey-8.0'
  | 'valkey-9.0'

const profiles: ProfileName[] = [
  'redis-6.2',
  'redis-7.0',
  'redis-7.2',
  'redis-7.4',
  'redis-8.0',
  'valkey-8.0',
  'valkey-9.0',
]

const nonLocal = 'Script attempted to access a non local key in a cluster node'
const legacyNonLocal =
  'Lua script attempted to access a non local key in a cluster node'
const notServed = 'Script attempted to access a slot not served'
const crossSlot = (command: string) =>
  `Command '${command}' in script attempted to access keys that don't hash to the same slot`

/** redis.pcall's cross-slot error, as it reaches the client. */
function pcallCrossSlot(profile: ProfileName, command: string): string {
  switch (profile) {
    case 'redis-6.2':
      return `-@user_script: 1: ${legacyNonLocal}\r\n`
    case 'redis-7.0':
    case 'redis-7.2':
      return `-ERR ${nonLocal}\r\n`
    default:
      return `-ERR ${crossSlot(command)}\r\n`
  }
}

describe('a script command whose keys span slots (#618)', () => {
  for (const profile of profiles) {
    test(`redis.pcall, ${profile}`, async () => {
      const { send, keys } = createHarness(profile)
      // Two slots of the local node.
      assert.strictEqual(
        await send(
          'eval',
          "return redis.pcall('MSET', ARGV[1], '1', ARGV[2], '1')",
          '0',
          keys.local,
          keys.localOther,
        ),
        pcallCrossSlot(profile, 'mset'),
      )
      // A served slot first, another node's slot second.
      assert.strictEqual(
        await send(
          'eval',
          "return redis.pcall('MGET', ARGV[1], ARGV[2])",
          '0',
          keys.local,
          keys.remote,
        ),
        pcallCrossSlot(profile, 'mget'),
      )
      // Another node's slot first.
      assert.strictEqual(
        await send(
          'eval',
          "return redis.pcall('ZUNIONSTORE', ARGV[1], '2', ARGV[1], ARGV[2])",
          '0',
          keys.remote,
          keys.local,
        ),
        pcallCrossSlot(profile, 'zunionstore'),
      )
      // Keys sharing one slot run.
      assert.strictEqual(
        await send(
          'eval',
          "return redis.pcall('MSET', ARGV[1], '1', ARGV[2], '1')",
          '0',
          keys.local,
          keys.localTwin,
        ),
        '+OK\r\n',
      )
    })
  }

  test('an unserved first key keeps its own error, except on valkey 9.0', async () => {
    const script = "return redis.pcall('MSET', ARGV[1], '1', ARGV[2], '1')"
    const expected: Record<ProfileName, string> = {
      'redis-6.2': `-@user_script: 1: ${legacyNonLocal}\r\n`,
      'redis-7.0': `-ERR ${nonLocal}\r\n`,
      'redis-7.2': `-ERR ${nonLocal}\r\n`,
      'redis-7.4': `-ERR ${notServed}\r\n`,
      'redis-8.0': `-ERR ${notServed}\r\n`,
      'valkey-8.0': `-ERR ${notServed}\r\n`,
      // Valkey 9.0 compares the slots before it looks the first one up.
      'valkey-9.0': `-ERR ${crossSlot('mset')}\r\n`,
    }
    for (const profile of profiles) {
      const { send, keys } = createHarness(profile)
      assert.strictEqual(
        await send('eval', script, '0', keys.unassigned, keys.local),
        expected[profile],
        profile,
      )
    }
  })

  test('redis.call aborts the script with the same text', async () => {
    const call = "return redis.call('MSET', ARGV[1], '1', ARGV[2], '1')"
    const sha = createHash('sha1').update(call).digest('hex')
    const run = (profile: ProfileName) => {
      const { send, keys } = createHarness(profile)
      return send('eval', call, '0', keys.local, keys.localOther)
    }
    assert.strictEqual(
      await run('redis-6.2'),
      `-ERR Error running script (call to f_${sha}): @user_script:1: @user_script: 1: ${legacyNonLocal}\r\n`,
    )
    assert.strictEqual(
      await run('redis-7.2'),
      `-ERR ${nonLocal} script: ${sha}, on @user_script:1.\r\n`,
    )
    for (const profile of ['redis-7.4', 'valkey-8.0'] as const) {
      assert.strictEqual(
        await run(profile),
        `-ERR ${crossSlot('mset')} script: ${sha}, on @user_script:1.\r\n`,
        profile,
      )
    }
  })

  test('declared keys spanning slots stay a top-level CROSSSLOT', async () => {
    for (const profile of profiles) {
      const { send, keys } = createHarness(profile)
      assert.strictEqual(
        await send(
          'eval',
          "return redis.pcall('GET', KEYS[1])",
          '2',
          keys.local,
          keys.localOther,
        ),
        "-CROSSSLOT Keys in request don't hash to the same slot\r\n",
        profile,
      )
    }
  })
})

/**
 * Node `local` owns 0-8191, `remote` (port 7001) 8193-16383; slot 8192 is
 * unassigned.
 */
function createHarness(compatibility: ProfileName) {
  const topology = new RedisClusterTopology([
    {
      id: 'local',
      role: 'master',
      host: '127.0.0.1',
      port: 7000,
      slots: [[0, 8191]],
    },
    {
      id: 'remote',
      role: 'master',
      host: '127.0.0.1',
      port: 7001,
      slots: [[8193, 16383]],
    },
  ])
  const server = new RedisServerState({
    clusterTopology: topology,
    compatibility,
  })
  const executor = createRedisCommandExecutor({
    extraCommands: createClusterCommands('local'),
    policies: [createClusterPolicy({ localNodeId: 'local' })],
    compatibility,
  })
  const session = new ClientSession({ server, executor })

  const find = (prefix: string, owns: (slot: number) => boolean): string => {
    for (let index = 0; index < 1000000; index++) {
      const key = `${prefix}-${index}`
      if (owns(topology.calculateSlot(Buffer.from(key)))) {
        return key
      }
    }
    throw new Error(`no key for ${prefix}`)
  }
  const localTag = find('local', slot => topology.nodeOwnsSlot('local', slot))
  const localSlot = topology.calculateSlot(Buffer.from(localTag))
  const otherLocalTag = find(
    'other',
    slot => slot !== localSlot && topology.nodeOwnsSlot('local', slot),
  )
  const remoteTag = find('tag', slot => topology.nodeOwnsSlot('remote', slot))
  const unassignedTag = find(
    'unassigned',
    slot => topology.getSlotOwner(slot) === undefined,
  )

  /** The reply's wire bytes (RESP2). */
  const send = async (command: string, ...args: string[]): Promise<string> =>
    encodeRedisResult(
      await session.execute(
        command,
        args.map(arg => Buffer.from(arg)),
      ),
    ).toString()

  return {
    send,
    keys: {
      local: `{${localTag}}:a`,
      localTwin: `{${localTag}}:b`,
      localOther: `{${otherLocalTag}}:a`,
      remote: `{${remoteTag}}:a`,
      unassigned: `{${unassignedTag}}:a`,
    },
  }
}
