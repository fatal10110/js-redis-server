import { describe, test } from 'node:test'
import assert from 'node:assert'
import {
  ClientSession,
  RedisClusterTopology,
  RedisResult,
  RedisServerState,
  RedisValue,
  createClusterPolicy,
  createClusterCommands,
  createRedisCommandExecutor,
} from '../src'

describe('new cluster execution policy', () => {
  test('allows commands whose parsed keys belong to the local node', async () => {
    const { session, topology } = createClusterHarness()
    const key = findKeyOwnedBy(topology, 'local')

    assert.deepStrictEqual(
      await session.execute('set', [key, Buffer.from('value')]),
      RedisResult.ok(),
    )
    assert.deepStrictEqual(
      await session.execute('get', [key]),
      RedisResult.create(RedisValue.bulkString(Buffer.from('value'))),
    )
  })

  test('returns MOVED for a key owned by another node', async () => {
    const { session, topology } = createClusterHarness()
    const key = findKeyOwnedBy(topology, 'remote')
    const slot = topology.calculateSlot(key)

    assert.deepStrictEqual(
      await session.execute('get', [key]),
      RedisResult.error(`${slot} 127.0.0.1:7001`, 'MOVED'),
    )
  })

  test('rejects multi-key commands whose keys span slots', async () => {
    const { session, topology } = createClusterHarness()
    const localKey = findKeyOwnedBy(topology, 'local')
    const remoteKey = findKeyOwnedBy(topology, 'remote')

    assert.deepStrictEqual(
      await session.execute('mget', [localKey, remoteKey]),
      RedisResult.error(
        `Keys in request don't hash to the same slot`,
        'CROSSSLOT',
      ),
    )
  })

  test('allows multi-key commands when hash tags force one local slot', async () => {
    const { session, topology } = createClusterHarness()
    const [first, second] = findSameSlotKeysOwnedBy(topology, 'local')

    assert.deepStrictEqual(
      await session.execute('set', [first, Buffer.from('one')]),
      RedisResult.ok(),
    )
    assert.deepStrictEqual(
      await session.execute('set', [second, Buffer.from('two')]),
      RedisResult.ok(),
    )
    assert.deepStrictEqual(
      await session.execute('mget', [first, second]),
      RedisResult.create(
        RedisValue.array([
          RedisValue.bulkString(Buffer.from('one')),
          RedisValue.bulkString(Buffer.from('two')),
        ]),
      ),
    )
  })

  test('pins transaction slot before commands are queued', async () => {
    const { session, server, topology } = createClusterHarness()
    const [first, second] = findDifferentSlotKeysOwnedBy(topology, 'local')

    assert.deepStrictEqual(await session.execute('multi', []), RedisResult.ok())
    assert.deepStrictEqual(
      await session.execute('set', [first, Buffer.from('one')]),
      RedisResult.create(RedisValue.simpleString('QUEUED')),
    )
    assert.deepStrictEqual(
      await session.execute('set', [second, Buffer.from('two')]),
      RedisResult.error(
        `Keys in request don't hash to the same slot`,
        'CROSSSLOT',
      ),
    )
    assert.deepStrictEqual(
      await session.execute('exec', []),
      RedisResult.error(
        'Transaction discarded because of previous errors.',
        'EXECABORT',
      ),
    )
    assert.strictEqual(server.getDatabase(0).getString(first), null)

    assert.deepStrictEqual(await session.execute('multi', []), RedisResult.ok())
    assert.deepStrictEqual(
      await session.execute('set', [second, Buffer.from('two')]),
      RedisResult.create(RedisValue.simpleString('QUEUED')),
    )
    assert.deepStrictEqual(
      await session.execute('exec', []),
      RedisResult.create(RedisValue.array([RedisValue.simpleString('OK')])),
    )
    assert.deepStrictEqual(
      server.getDatabase(0).getString(second),
      Buffer.from('two'),
    )
  })

  test('matches Redis Cluster errors for CLUSTER arity and subcommands', async () => {
    const { session } = createClusterHarness()

    assert.deepStrictEqual(
      await session.execute('cluster', []),
      RedisResult.error(
        "wrong number of arguments for 'cluster' command",
        'ERR',
      ),
    )
    assert.deepStrictEqual(
      await session.execute('cluster', [Buffer.from('nope')]),
      RedisResult.error("unknown subcommand 'nope'. Try CLUSTER HELP.", 'ERR'),
    )

    for (const subcommand of ['slots', 'shards', 'nodes', 'info', 'myid']) {
      assert.deepStrictEqual(
        await session.execute('cluster', [
          Buffer.from(subcommand),
          Buffer.from('extra'),
        ]),
        RedisResult.error(
          `wrong number of arguments for 'cluster|${subcommand}' command`,
          'ERR',
        ),
      )
    }
  })

  test('rejects SELECT in cluster mode like Redis Cluster', async () => {
    const { session } = createClusterHarness()

    assert.deepStrictEqual(
      await session.execute('select', [Buffer.from('1')]),
      RedisResult.error('SELECT is not allowed in cluster mode', 'ERR'),
    )
  })

  test('accepts SELECT 0 in cluster mode', async () => {
    const { session } = createClusterHarness()

    assert.deepStrictEqual(
      await session.execute('select', [Buffer.from('0')]),
      RedisResult.ok(),
    )
  })

  test('rejects MOVE in cluster mode like Redis Cluster', async () => {
    const { session, topology } = createClusterHarness()
    const key = findKeyOwnedBy(topology, 'local')

    assert.deepStrictEqual(
      await session.execute('move', [key, Buffer.from('1')]),
      RedisResult.error('MOVE is not allowed in cluster mode', 'ERR'),
    )
  })

  test('returns CLUSTERDOWN without the slot number for an unassigned slot', async () => {
    const { session, topology } = createClusterHarnessWithUnassignedSlot()
    const key = findKeyInUnassignedSlot(topology)

    assert.deepStrictEqual(
      await session.execute('get', [key]),
      RedisResult.error('Hash slot not served', 'CLUSTERDOWN'),
    )
  })

  test('DISCARD clears the pinned transaction slot', async () => {
    const { session, server, topology } = createClusterHarness()
    const [first, second] = findDifferentSlotKeysOwnedBy(topology, 'local')

    // Pin the slot to `first`, then abandon the transaction.
    assert.deepStrictEqual(await session.execute('multi', []), RedisResult.ok())
    assert.deepStrictEqual(
      await session.execute('set', [first, Buffer.from('one')]),
      RedisResult.create(RedisValue.simpleString('QUEUED')),
    )
    assert.deepStrictEqual(
      await session.execute('discard', []),
      RedisResult.ok(),
    )

    // A fresh transaction must be able to pin a different slot.
    assert.deepStrictEqual(await session.execute('multi', []), RedisResult.ok())
    assert.deepStrictEqual(
      await session.execute('set', [second, Buffer.from('two')]),
      RedisResult.create(RedisValue.simpleString('QUEUED')),
    )
    assert.deepStrictEqual(
      await session.execute('exec', []),
      RedisResult.create(RedisValue.array([RedisValue.simpleString('OK')])),
    )
    assert.deepStrictEqual(
      server.getDatabase(0).getString(second),
      Buffer.from('two'),
    )
  })
})

function createClusterHarness() {
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
      slots: [[8192, 16383]],
    },
  ])
  const server = new RedisServerState({ clusterTopology: topology })
  const executor = createRedisCommandExecutor({
    extraCommands: createClusterCommands('local'),
    policies: [createClusterPolicy({ localNodeId: 'local' })],
  })
  const session = new ClientSession({ server, executor })

  return { executor, server, session, topology }
}

function createClusterHarnessWithUnassignedSlot() {
  // Leave slot 8192 unassigned so a key hashing to it triggers CLUSTERDOWN.
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
  const server = new RedisServerState({ clusterTopology: topology })
  const executor = createRedisCommandExecutor({
    extraCommands: createClusterCommands('local'),
    policies: [createClusterPolicy({ localNodeId: 'local' })],
  })
  const session = new ClientSession({ server, executor })

  return { executor, server, session, topology }
}

function findKeyInUnassignedSlot(topology: RedisClusterTopology): Buffer {
  for (let index = 0; index < 1000000; index++) {
    const key = Buffer.from(`unassigned-${index}`)
    if (topology.getSlotOwner(topology.calculateSlot(key)) === undefined) {
      return key
    }
  }

  throw new Error('Could not find key hashing to an unassigned slot')
}

function findKeyOwnedBy(
  topology: RedisClusterTopology,
  nodeId: string,
): Buffer {
  for (let index = 0; index < 100000; index++) {
    const key = Buffer.from(`key-${nodeId}-${index}`)
    const slot = topology.calculateSlot(key)
    if (topology.nodeOwnsSlot(nodeId, slot)) {
      return key
    }
  }

  throw new Error(`Could not find key owned by ${nodeId}`)
}

function findSameSlotKeysOwnedBy(
  topology: RedisClusterTopology,
  nodeId: string,
): [Buffer, Buffer] {
  for (let index = 0; index < 100000; index++) {
    const tag = `tag-${index}`
    const first = Buffer.from(`first:{${tag}}`)
    const slot = topology.calculateSlot(first)

    if (topology.nodeOwnsSlot(nodeId, slot)) {
      return [first, Buffer.from(`second:{${tag}}`)]
    }
  }

  throw new Error(`Could not find same-slot keys owned by ${nodeId}`)
}

function findDifferentSlotKeysOwnedBy(
  topology: RedisClusterTopology,
  nodeId: string,
): [Buffer, Buffer] {
  const first = findKeyOwnedBy(topology, nodeId)
  const firstSlot = topology.calculateSlot(first)

  for (let index = 0; index < 100000; index++) {
    const second = Buffer.from(`other-${nodeId}-${index}`)
    const secondSlot = topology.calculateSlot(second)
    if (secondSlot !== firstSlot && topology.nodeOwnsSlot(nodeId, secondSlot)) {
      return [first, second]
    }
  }

  throw new Error(`Could not find different-slot keys owned by ${nodeId}`)
}
