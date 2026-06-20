import { test, describe, afterEach } from 'node:test'
import assert from 'node:assert'
// Import exclusively through the package barrel — this is the curated public
// surface consumers see, not the deep module paths.
import {
  createRedisMock,
  createRedisServer,
  createInMemoryClient,
  InMemoryRedisClient,
  seedStandalone,
  seedCluster,
  buildRedisCluster,
  RedisCluster,
  computeSlotRange,
  Resp2Server,
  RedisServerState,
  createRedisCommandExecutor,
  RedisCommandError,
  WrongTypeRedisError,
  type RedisMock,
} from '../src'

describe('public interface — exported symbols', () => {
  test('facade, builders, and helpers are all exported as the right kind', () => {
    for (const fn of [
      createRedisMock,
      createRedisServer,
      createInMemoryClient,
      seedStandalone,
      seedCluster,
      buildRedisCluster,
      computeSlotRange,
      createRedisCommandExecutor,
    ]) {
      assert.strictEqual(typeof fn, 'function')
    }
    // Constructors / classes.
    for (const ctor of [
      InMemoryRedisClient,
      RedisCluster,
      Resp2Server,
      RedisServerState,
      RedisCommandError,
      WrongTypeRedisError,
    ]) {
      assert.strictEqual(typeof ctor, 'function')
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
})

describe('public interface — createRedisMock', () => {
  let mock: RedisMock

  afterEach(async () => {
    await mock?.close()
  })

  test('tcp standalone exposes the documented surface', async () => {
    mock = await createRedisMock()
    assert.strictEqual(mock.url, `redis://127.0.0.1:${mock.port}`)
    assert.deepStrictEqual(mock.connectionOptions(), {
      host: '127.0.0.1',
      port: mock.port,
    })
    assert.deepStrictEqual(mock.clusterNodes(), [
      { host: '127.0.0.1', port: mock.port },
    ])
    assert.ok(mock.state)
    assert.strictEqual(mock.nodes, undefined)

    await mock.seed([{ key: 'k', type: 'string', value: 'v' }])
    const client = mock.client()
    assert.strictEqual(await client.command('GET', 'k'), 'v')
    await mock.flush()
    assert.strictEqual(await client.command('GET', 'k'), null)
  })

  test('memory transport has no endpoint but a working client', async () => {
    mock = await createRedisMock({ transport: 'memory' })
    assert.throws(() => mock.connectionOptions(), /no TCP endpoint/)
    const client = mock.client()
    assert.strictEqual(await client.command('PING'), 'PONG')
  })

  test('cluster mock exposes nodes and a slot-routed seed', async () => {
    mock = await createRedisMock({ cluster: { masters: 3 } })
    assert.strictEqual(mock.clusterNodes().length, 3)
    assert.ok(mock.nodes)
    assert.strictEqual(mock.state, undefined)
    await mock.seed([{ key: 'user:1', type: 'string', value: 'alice' }])
    const owner = mock.nodes!.find(
      node =>
        node.server.getDatabase(0).getString(Buffer.from('user:1')) !== null,
    )
    assert.ok(owner)
  })

  test('error replies surface as RedisCommandError', async () => {
    mock = await createRedisMock({ transport: 'memory' })
    const client = mock.client()
    await client.command('SET', 'k', 'v')
    await assert.rejects(
      client.command('INCR', 'k'),
      (err: unknown) => err instanceof RedisCommandError,
    )
  })
})
