import { test, describe, afterEach } from 'node:test'
import assert from 'node:assert'
// The root barrel is the curated consumer surface: the test-mock facade, the
// `create*` builders, seeding, the socketless client, and the error classes.
import * as root from '../src'
import {
  createRedisMock,
  createRedisServer,
  createInMemoryClient,
  InMemoryRedisClient,
  seedStandalone,
  seedCluster,
  createRedisCluster,
  buildRedisCluster,
  RedisCluster,
  computeSlotRange,
  RedisCommandError,
  WrongTypeRedisError,
  type RedisMock,
} from '../src'
// Hand-wiring building blocks live on the `js-redis-server/core` subpath, NOT
// on the root.
import {
  Resp2Server,
  RedisServerState,
  createRedisCommandExecutor,
} from '../src/internal'

describe('public interface — exported symbols', () => {
  test('facade, builders, and helpers are all exported as the right kind', () => {
    for (const fn of [
      createRedisMock,
      createRedisServer,
      createInMemoryClient,
      seedStandalone,
      seedCluster,
      createRedisCluster,
      buildRedisCluster,
      computeSlotRange,
    ]) {
      assert.strictEqual(typeof fn, 'function')
    }
    // Constructors / classes.
    for (const ctor of [
      InMemoryRedisClient,
      RedisCluster,
      RedisCommandError,
      WrongTypeRedisError,
    ]) {
      assert.strictEqual(typeof ctor, 'function')
    }
  })

  test('buildRedisCluster is a deprecated alias of createRedisCluster', () => {
    assert.strictEqual(buildRedisCluster, createRedisCluster)
  })

  test('hand-wiring building blocks ARE exported from the core subpath', () => {
    assert.strictEqual(typeof Resp2Server, 'function')
    assert.strictEqual(typeof RedisServerState, 'function')
    assert.strictEqual(typeof createRedisCommandExecutor, 'function')
  })

  test('hand-wiring building blocks are NOT on the root barrel', () => {
    for (const name of [
      'Resp2Server',
      'RedisServerState',
      'createRedisCommandExecutor',
      'defineCommand',
      'CommandRegistry',
      'RedisDatabase',
    ]) {
      assert.strictEqual(
        name in root,
        false,
        `${name} should only be exported from js-redis-server/core`,
      )
    }
  })

  test('error classes form a RedisCommandError hierarchy', () => {
    assert.ok(new WrongTypeRedisError() instanceof RedisCommandError)
  })

  test('computeSlotRange partitions the slot space', () => {
    assert.deepStrictEqual(computeSlotRange(0, 1), [0, 16383])
  })
})

describe('public interface — createRedisServer', () => {
  test('returns a handle that does not leak the executor', async () => {
    const handle = await createRedisServer({ databaseCount: 4 })
    try {
      assert.strictEqual(handle.state.databases.length, 4)
      assert.strictEqual(handle.host, '127.0.0.1')
      assert.ok(handle.port > 0)
      assert.ok(handle.server instanceof Resp2Server)
      assert.strictEqual('executor' in handle, false)
    } finally {
      await handle.close()
    }
  })

  test('the cluster option builds and starts a listening cluster', async () => {
    const cluster = await createRedisServer({ cluster: { masters: 3 } })
    try {
      assert.ok(cluster instanceof RedisCluster)
      assert.strictEqual(cluster.nodes.length, 3)
      // Already listening — every node bound a real port.
      for (const node of cluster.nodes) {
        assert.ok(node.port > 0)
      }
    } finally {
      await cluster.close()
    }
  })
})

describe('public interface — createRedisMock', () => {
  let mock: RedisMock

  afterEach(async () => {
    await mock?.close()
  })

  test('tcp standalone exposes the documented surface', async () => {
    mock = await createRedisMock()
    assert.strictEqual(mock.url, `redis://127.0.0.1:${mock.port}`)
    assert.deepStrictEqual(mock.addresses(), [
      { host: '127.0.0.1', port: mock.port },
    ])
    assert.ok(mock.state)
    assert.strictEqual(mock.nodes, undefined)

    // A socketless client over the mock's own state (the documented power-user
    // escape hatch) sees the seed and the subsequent flush.
    await mock.seed([{ key: 'k', type: 'string', value: 'v' }])
    const client = new InMemoryRedisClient({
      server: mock.state!,
      executor: createRedisCommandExecutor(),
    })
    assert.strictEqual(await client.command('GET', 'k'), 'v')
    await mock.flush()
    assert.strictEqual(await client.command('GET', 'k'), null)
    client.close()
  })

  test('cluster mock exposes nodes and a slot-routed seed', async () => {
    mock = await createRedisMock({ cluster: { masters: 3 } })
    assert.strictEqual(mock.addresses().length, 3)
    assert.ok(mock.nodes)
    assert.strictEqual(mock.state, undefined)
    await mock.seed([{ key: 'user:1', type: 'string', value: 'alice' }])
    const owner = mock.nodes!.find(
      node =>
        node.server.getDatabase(0).getString(Buffer.from('user:1')) !== null,
    )
    assert.ok(owner)
  })
})
