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

// #604: per-version CLUSTER INFO lines, a script touching an unserved slot,
// and the order of the unserved / CROSSSLOT checks over a command's keys.
// Real replies: redis 6.2.14, 7.0.15, 7.2.4, 7.2.16, 7.4.0, 7.4.4, 8.0.0 and
// valkey 8.0.0, 8.1.0, 9.0.0, as single-node clusters with
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

const crossSlot = "-CROSSSLOT Keys in request don't hash to the same slot\r\n"
const clusterDown = '-CLUSTERDOWN Hash slot not served\r\n'

describe('CLUSTER INFO lines per version (#604)', () => {
  const base = [
    'cluster_state',
    'cluster_slots_assigned',
    'cluster_slots_ok',
    'cluster_slots_pfail',
    'cluster_slots_fail',
    'cluster_known_nodes',
    'cluster_size',
    'cluster_current_epoch',
    'cluster_my_epoch',
    'cluster_stats_messages_sent',
    'cluster_stats_messages_received',
  ]
  const expected: Record<ProfileName, string[]> = {
    'redis-6.2': base,
    'redis-7.0': [...base, 'total_cluster_links_buffer_limit_exceeded'],
    'redis-7.2': [...base, 'total_cluster_links_buffer_limit_exceeded'],
    'redis-7.4': [...base, 'total_cluster_links_buffer_limit_exceeded'],
    'redis-8.0': [...base, 'total_cluster_links_buffer_limit_exceeded'],
    'valkey-8.0': [...base, 'total_cluster_links_buffer_limit_exceeded'],
    'valkey-9.0': [
      ...base.slice(0, 5),
      'cluster_nodes_pfail',
      'cluster_nodes_fail',
      'cluster_voting_nodes_pfail',
      'cluster_voting_nodes_fail',
      ...base.slice(5),
      'total_cluster_links_buffer_limit_exceeded',
    ],
  }

  for (const profile of profiles) {
    test(profile, async () => {
      const { session } = createHarness(profile)
      const result = await session.execute('cluster', [Buffer.from('info')])
      const text = String(
        (result.value as { value: Buffer | null }).value?.toString(),
      )
      assert.ok(text.endsWith('\r\n'), text)
      const lines = text.slice(0, -2).split('\r\n')
      assert.deepStrictEqual(
        lines.map(line => line.split(':')[0]),
        expected[profile],
      )
      for (const line of lines) {
        if (
          /^(cluster_(voting_)?nodes_p?fail|total_cluster_links)/.test(line)
        ) {
          assert.ok(line.endsWith(':0'), line)
        }
      }
    })
  }
})

describe('a script touching an unserved slot (#604)', () => {
  const nonLocal =
    'Script attempted to access a non local key in a cluster node'
  const legacyNonLocal =
    'Lua script attempted to access a non local key in a cluster node'
  const notServed = 'Script attempted to access a slot not served'
  const pcall = "return redis.pcall('GET', ARGV[1])"
  const call = "return redis.call('GET', ARGV[1])"

  // redis.pcall's error, as it reaches the client.
  const pcallReply: Record<ProfileName, { unserved: string; moved: string }> = {
    'redis-6.2': {
      unserved: `-@user_script: 1: ${legacyNonLocal}\r\n`,
      moved: `-@user_script: 1: ${legacyNonLocal}\r\n`,
    },
    'redis-7.0': {
      unserved: `-ERR ${nonLocal}\r\n`,
      moved: `-ERR ${nonLocal}\r\n`,
    },
    'redis-7.2': {
      unserved: `-ERR ${nonLocal}\r\n`,
      moved: `-ERR ${nonLocal}\r\n`,
    },
    'redis-7.4': {
      unserved: `-ERR ${notServed}\r\n`,
      moved: `-ERR ${nonLocal}\r\n`,
    },
    'redis-8.0': {
      unserved: `-ERR ${notServed}\r\n`,
      moved: `-ERR ${nonLocal}\r\n`,
    },
    'valkey-8.0': {
      unserved: `-ERR ${notServed}\r\n`,
      moved: `-ERR ${nonLocal}\r\n`,
    },
    'valkey-9.0': {
      unserved: `-ERR ${notServed}\r\n`,
      moved: `-ERR ${nonLocal}\r\n`,
    },
  }

  for (const profile of profiles) {
    test(`redis.pcall, ${profile}`, async () => {
      const { eval0, keys } = createHarness(profile)
      assert.strictEqual(
        await eval0(pcall, keys.unassigned),
        pcallReply[profile].unserved,
      )
      assert.strictEqual(
        await eval0(pcall, keys.remote),
        pcallReply[profile].moved,
      )
      assert.strictEqual(await eval0(pcall, keys.local), '$-1\r\n')
    })
  }

  test('redis.call aborts the script with the same text', async () => {
    const sha = createHash('sha1').update(call).digest('hex')
    const legacy = createHarness('redis-6.2')
    assert.strictEqual(
      await legacy.eval0(call, legacy.keys.unassigned),
      `-ERR Error running script (call to f_${sha}): @user_script:1: @user_script: 1: ${legacyNonLocal}\r\n`,
    )
    const r70 = createHarness('redis-7.0')
    assert.strictEqual(
      await r70.eval0(call, r70.keys.unassigned),
      `-ERR ${nonLocal} script: ${sha}, on @user_script:1.\r\n`,
    )
    const r80 = createHarness('redis-8.0')
    assert.strictEqual(
      await r80.eval0(call, r80.keys.unassigned),
      `-ERR ${notServed} script: ${sha}, on @user_script:1.\r\n`,
    )
  })

  test('a declared unserved key is refused before the script runs', async () => {
    for (const profile of profiles) {
      const { send, keys } = createHarness(profile)
      assert.strictEqual(
        await send('eval', pcall, '1', keys.unassigned),
        clusterDown,
        profile,
      )
    }
  })
})

describe('unserved first key vs CROSSSLOT (#604)', () => {
  // Redis's getNodeByQuery walks the keys in order: an unserved first key
  // answers CLUSTERDOWN at once, a later key in another slot CROSSSLOT.
  // Valkey 9.0 checks every key's slot first, so CROSSSLOT wins there.
  for (const profile of profiles) {
    test(profile, async () => {
      const { send, keys } = createHarness(profile)
      const {
        unassigned: u,
        unassignedTwin: u2,
        local: l,
        localTwin: l2,
      } = keys
      const unservedFirst = profile === 'valkey-9.0' ? crossSlot : clusterDown

      const rows: [string[], string][] = [
        [['mset', u, '1', l, '1'], unservedFirst],
        [['mset', l, '1', u, '1'], crossSlot],
        [['mget', u, l], unservedFirst],
        [['mget', l, u], crossSlot],
        [['mset', u, '1', u2, '1'], clusterDown],
        [['get', u], clusterDown],
        // ZUNIONSTORE & co. list the sources first, the destination last.
        [['zunionstore', l, '1', u], unservedFirst],
        [['zinterstore', l, '1', u], unservedFirst],
        [['zdiffstore', l, '1', u], unservedFirst],
        [['zunionstore', l, '1x', u], unservedFirst],
        [['zunion', '1', u], clusterDown],
        [['zunionstore', l, '2', l2, u], crossSlot],
        [['zunionstore', u, '1', l], crossSlot],
        [['zinterstore', l, '2', l2, u], crossSlot],
        [['zdiffstore', u, '2', l, l2], crossSlot],
      ]
      for (const [command, reply] of rows) {
        assert.strictEqual(
          await send(...command),
          reply,
          `${profile}: ${command.join(' ')}`,
        )
      }
    })

    test(`${profile}: refused at queue time inside MULTI`, async () => {
      const { send, keys } = createHarness(profile)
      assert.strictEqual(await send('multi'), '+OK\r\n')
      assert.strictEqual(
        await send('zunionstore', keys.local, '1', keys.unassigned),
        profile === 'valkey-9.0' ? crossSlot : clusterDown,
      )
      assert.strictEqual(
        await send('exec'),
        '-EXECABORT Transaction discarded because of previous errors.\r\n',
      )
    })
  }
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
    session,
    send,
    eval0: (script: string, key: string) => send('eval', script, '0', key),
    keys: {
      local: `{${localTag}}:a`,
      localTwin: `{${localTag}}:b`,
      remote: `{${remoteTag}}:a`,
      unassigned: `{${unassignedTag}}:a`,
      unassignedTwin: `{${unassignedTag}}:b`,
    },
  }
}
