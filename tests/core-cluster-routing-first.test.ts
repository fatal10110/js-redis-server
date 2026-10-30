import { describe, test } from 'node:test'
import assert from 'node:assert'
import {
  ClientSession,
  RedisClusterTopology,
  RedisResult,
  RedisServerState,
  createClusterPolicy,
  createClusterCommands,
  createRedisCommandExecutor,
} from '../src/internal'
import { computeSlotRange } from '../src/cluster'
import type { CompatibilitySpec } from '../src/core/compatibility/profile'

// #519: in a cluster, Redis's processCommand routes a keyed command
// (getNodeByQuery: MOVED / CROSSSLOT / CLUSTERDOWN) after the arity check and
// before the command parses its own arguments. Keys come from the key specs
// over the raw arguments (the getkeys proc for numkeys commands, which reads
// numkeys with atoi). Real replies: redis 6.2.14-8.0.0, valkey 8.0.0/9.0.0.

const notInteger = RedisResult.error(
  'value is not an integer or out of range',
  'ERR',
)
const moveRefused = RedisResult.error(
  'MOVE is not allowed in cluster mode',
  'ERR',
)
const clusterDown = RedisResult.error('Hash slot not served', 'CLUSTERDOWN')
const sha = '0123456789012345678901234567890123456789'

describe('cluster routes a keyed command before its own argument check (#519)', () => {
  test('a key on another node answers MOVED, whatever the arguments', async () => {
    const { session, keys } = createHarness()
    const r = keys.remote
    const moved = RedisResult.error(
      `${keys.remoteSlot} 127.0.0.1:7001`,
      'MOVED',
    )
    const rows: string[][] = [
      ['eval', 'return 1', '1x', r],
      ['evalsha', sha, '1x', r],
      ['eval_ro', 'return 1', '1x', r],
      ['fcall', 'f', '1x', r],
      ['set', r, '1', 'EX', 'x'],
      ['set', r, '1', 'NX', 'XX'],
      ['incrby', r, 'x'],
      ['blpop', r, 'x'],
      ['xread', 'COUNT', 'x', 'STREAMS', r, '0'],
      ['sintercard', '1x', r],
      ['lmpop', '1x', r, 'LEFT'],
      ['zunionstore', keys.remoteTwin, '1x', r],
      ['mset', r, '1', keys.remoteTwin],
      ['move', r, '1'],
      ['move', r, 'x'],
    ]

    for (const [command, ...args] of rows) {
      assert.deepStrictEqual(
        await session.execute(
          command,
          args.map(arg => Buffer.from(arg)),
        ),
        moved,
        `${command} ${args.join(' ')}`,
      )
    }
  })

  test('an unserved slot answers CLUSTERDOWN before the argument check', async () => {
    const { session, keys } = createHarness()
    const u = keys.unassigned

    for (const [command, ...args] of [
      ['eval', 'return 1', '1x', u],
      ['set', u, '1', 'EX', 'x'],
      ['move', u, 'x'],
    ]) {
      assert.deepStrictEqual(
        await session.execute(
          command,
          args.map(arg => Buffer.from(arg)),
        ),
        clusterDown,
        `${command} ${args.join(' ')}`,
      )
    }
  })

  test('keys in different slots answer CROSSSLOT before the argument check', async () => {
    const { session, keys } = createHarness()

    assert.deepStrictEqual(
      await session.execute(
        'eval',
        ['return 1', '2x', keys.local, keys.remote].map(arg =>
          Buffer.from(arg),
        ),
      ),
      RedisResult.error(
        "Keys in request don't hash to the same slot",
        'CROSSSLOT',
      ),
    )
  })

  test('a local or keyless command answers its own argument error', async () => {
    const { session, keys } = createHarness()
    const rows: [string[], RedisResult][] = [
      [['eval', 'return 1', '1x', keys.local], notInteger],
      [['set', keys.local, '1', 'EX', 'x'], notInteger],
      [
        ['sintercard', '1x', keys.local],
        RedisResult.error('numkeys should be greater than 0', 'ERR'),
      ],
      // numkeys past the arguments, or not a number at all: no keys.
      [
        ['eval', 'return 1', '5', keys.remote],
        RedisResult.error(
          "Number of keys can't be greater than number of args",
          'ERR',
        ),
      ],
      // atoi reads `1.5` as 1, so only a local key reaches the parser.
      [['eval', 'return 1', '1.5', keys.local], notInteger],
      [['zintercard', 'x', keys.remote], notInteger],
      // The arity check still comes first.
      [
        ['get', keys.remote, 'b'],
        RedisResult.error("wrong number of arguments for 'get' command", 'ERR'),
      ],
    ]

    for (const [[command, ...args], expected] of rows) {
      assert.deepStrictEqual(
        await session.execute(
          command,
          args.map(arg => Buffer.from(arg)),
        ),
        expected,
        `${command} ${args.join(' ')}`,
      )
    }
  })

  test('MOVE refuses a local key before parsing the database', async () => {
    const { session, keys } = createHarness()

    for (const db of ['1', 'x']) {
      assert.deepStrictEqual(
        await session.execute('move', [
          Buffer.from(keys.local),
          Buffer.from(db),
        ]),
        moveRefused,
        `MOVE local ${db}`,
      )
    }
  })

  test('valkey 9 cluster databases: MOVE routes, then parses', async () => {
    const { session, keys } = createHarness('valkey-9.0')

    assert.deepStrictEqual(
      await session.execute('move', [
        Buffer.from(keys.local),
        Buffer.from('x'),
      ]),
      notInteger,
    )
    assert.deepStrictEqual(
      await session.execute('move', [
        Buffer.from(keys.remote),
        Buffer.from('x'),
      ]),
      RedisResult.error(`${keys.remoteSlot} 127.0.0.1:7001`, 'MOVED'),
    )
  })

  test('a script call is routed before its own argument check', async () => {
    const { session, keys } = createHarness()
    // A script's error reply carries its raw bytes too; compare the text.
    const call = async (script: string, key: string) => {
      const { value } = await session.execute('eval', [
        Buffer.from(script),
        Buffer.from('0'),
        Buffer.from(key),
      ])
      assert.strictEqual(value.kind, 'error')
      const { code, message } = value as { code: string; message: string }
      return RedisResult.error(message, code)
    }

    assert.deepStrictEqual(
      await call(
        "return redis.pcall('SET', ARGV[1], '1', 'EX', 'x')",
        keys.remote,
      ),
      RedisResult.error(
        'Script attempted to access a non local key in a cluster node',
        'ERR',
      ),
    )
    assert.deepStrictEqual(
      await call(
        "return redis.pcall('SET', ARGV[1], '1', 'EX', 'x')",
        keys.local,
      ),
      notInteger,
    )
    assert.deepStrictEqual(
      await call("return redis.pcall('MOVE', ARGV[1], 'x')", keys.local),
      moveRefused,
    )
  })
})

describe('CLUSTER INFO (#519)', () => {
  test('has no cluster_enabled line', async () => {
    const { session } = createHarness()
    const result = await session.execute('cluster', [Buffer.from('info')])
    assert.strictEqual(result.value.kind, 'bulk-string')
    const text = String(
      (result.value as { value: Buffer | null }).value?.toString(),
    )
    assert.ok(text.startsWith('cluster_state:ok\r\n'), text)
    assert.ok(!text.includes('cluster_enabled'), text)
  })
})

describe('computeSlotRange follows redis-cli --cluster create (#519)', () => {
  // Captured from `redis-cli --cluster create` on redis 6.2.24 and 8.0.0.
  const expected: Record<number, [number, number][]> = {
    1: [[0, 16383]],
    2: [
      [0, 8191],
      [8192, 16383],
    ],
    3: [
      [0, 5460],
      [5461, 10922],
      [10923, 16383],
    ],
    5: [
      [0, 3276],
      [3277, 6553],
      [6554, 9829],
      [9830, 13106],
      [13107, 16383],
    ],
    6: [
      [0, 2730],
      [2731, 5460],
      [5461, 8191],
      [8192, 10922],
      [10923, 13652],
      [13653, 16383],
    ],
    7: [
      [0, 2340],
      [2341, 4680],
      [4681, 7021],
      [7022, 9361],
      [9362, 11702],
      [11703, 14042],
      [14043, 16383],
    ],
  }

  for (const [masters, ranges] of Object.entries(expected)) {
    test(`${masters} masters`, () => {
      assert.deepStrictEqual(
        ranges.map((_, index) => computeSlotRange(index, Number(masters))),
        ranges,
      )
    })
  }

  test('any master count covers every slot once', () => {
    for (let masters = 1; masters <= 64; masters++) {
      let next = 0
      for (let index = 0; index < masters; index++) {
        const [start, end] = computeSlotRange(index, masters)
        assert.strictEqual(start, next, `${masters} masters, #${index}`)
        assert.ok(end >= start, `${masters} masters, #${index}`)
        next = end + 1
      }
      assert.strictEqual(next, 16384, `${masters} masters`)
    }
  })
})

/**
 * Node `local` owns 0-8191, `remote` (port 7001) 8193-16383; slot 8192 is
 * unassigned.
 */
function createHarness(compatibility: CompatibilitySpec = 'redis-8.0') {
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
  const local = find('local', slot => topology.nodeOwnsSlot('local', slot))
  const remoteTag = find('tag', slot => topology.nodeOwnsSlot('remote', slot))
  const remote = `{${remoteTag}}:a`
  const unassigned = find(
    'unassigned',
    slot => topology.getSlotOwner(slot) === undefined,
  )

  return {
    session,
    keys: {
      local,
      remote,
      remoteTwin: `{${remoteTag}}:b`,
      remoteSlot: topology.calculateSlot(Buffer.from(remote)),
      unassigned,
    },
  }
}
