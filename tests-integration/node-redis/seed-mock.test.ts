import { test, describe, before, after } from 'node:test'
import assert from 'node:assert'
import {
  createClient,
  createCluster,
  type RedisClientType,
  type RedisClusterType,
} from 'redis'
import { createRedisMock, type RedisMock } from '../../src/mock'

// node-redis counterpart of the ioredis seed-mock suite: seed the public
// `createRedisMock().seed()` facade, read every type back through a real
// node-redis client over the wire — standalone and cluster mock modes both.

describe('createRedisMock seed readback via node-redis (standalone)', () => {
  let mock: RedisMock
  let client: RedisClientType
  let db3Client: RedisClientType

  before(async () => {
    mock = await createRedisMock()
    await mock.seed([
      { key: 'user:1', type: 'string', value: 'alice' },
      { key: 'counter', type: 'string', value: 42 },
      { key: 'h:1', type: 'hash', value: { name: 'bob', age: 30 } },
      { key: 'l:1', type: 'list', value: ['a', 'b', 1] },
      { key: 's:1', type: 'set', value: ['x', 'y'] },
      { key: 'z:1', type: 'zset', value: { a: 1, b: 2 } },
      { key: 'ttl:1', type: 'string', value: 'temp', ttlMs: 50_000 },
      { key: 'in-db-3', type: 'string', value: 'scoped', db: 3 },
    ])
    client = createClient({ url: mock.url }) as RedisClientType
    client.on('error', () => {})
    await client.connect()
    db3Client = createClient({ url: mock.url, database: 3 }) as RedisClientType
    db3Client.on('error', () => {})
    await db3Client.connect()
  })

  after(async () => {
    client.destroy()
    db3Client.destroy()
    await mock.close()
  })

  test('reads back a string', async () => {
    assert.strictEqual(await client.get('user:1'), 'alice')
  })

  test('coerces a numeric string value to its decimal form', async () => {
    assert.strictEqual(await client.get('counter'), '42')
  })

  test('reads back a hash', async () => {
    assert.deepStrictEqual(await client.hGetAll('h:1'), {
      name: 'bob',
      age: '30',
    })
  })

  test('reads back a list in order', async () => {
    assert.deepStrictEqual(await client.lRange('l:1', 0, -1), ['a', 'b', '1'])
  })

  test('reads back a set', async () => {
    assert.deepStrictEqual(
      new Set(await client.sMembers('s:1')),
      new Set(['x', 'y']),
    )
  })

  test('reads back a zset with scores', async () => {
    assert.deepStrictEqual(await client.zRangeWithScores('z:1', 0, -1), [
      { value: 'a', score: 1 },
      { value: 'b', score: 2 },
    ])
  })

  test('applies ttlMs as a live expiration', async () => {
    const pttl = await client.pTTL('ttl:1')
    assert.ok(pttl > 0 && pttl <= 50_000, `expected live ttl, got ${pttl}`)
  })

  test('routes an entry to the selected logical database', async () => {
    assert.strictEqual(await client.get('in-db-3'), null)
    assert.strictEqual(await db3Client.get('in-db-3'), 'scoped')
  })

  test('flush() clears seeded data', async () => {
    await mock.flush()
    assert.strictEqual(await client.get('user:1'), null)
  })
})

describe('createRedisMock seed readback via node-redis (cluster)', () => {
  let mock: RedisMock
  let cluster: RedisClusterType

  before(async () => {
    mock = await createRedisMock({ cluster: { masters: 3 } })
    await mock.seed([
      { key: 'user:1', type: 'string', value: 'alice' },
      { key: 'h:1', type: 'hash', value: { name: 'bob' } },
      { key: 'l:1', type: 'list', value: ['a', 'b'] },
      { key: 's:1', type: 'set', value: ['x', 'y'] },
      { key: 'z:1', type: 'zset', value: { a: 1, b: 2 } },
    ])
    cluster = createCluster({
      rootNodes: mock.clusterNodes().map(node => ({
        url: `redis://${node.host}:${node.port}`,
      })),
    }) as RedisClusterType
    cluster.on('error', () => {})
    await cluster.connect()
  })

  after(async () => {
    await cluster.close()
    await mock.close()
  })

  test('routes each seeded key to its slot owner, readable by the client', async () => {
    assert.strictEqual(await cluster.get('user:1'), 'alice')
    assert.deepStrictEqual(await cluster.hGetAll('h:1'), { name: 'bob' })
    assert.deepStrictEqual(await cluster.lRange('l:1', 0, -1), ['a', 'b'])
    assert.deepStrictEqual(
      new Set(await cluster.sMembers('s:1')),
      new Set(['x', 'y']),
    )
    assert.deepStrictEqual(await cluster.zRangeWithScores('z:1', 0, -1), [
      { value: 'a', score: 1 },
      { value: 'b', score: 2 },
    ])
  })
})
