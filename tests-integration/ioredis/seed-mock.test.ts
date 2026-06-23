import { test, describe, before, after } from 'node:test'
import assert from 'node:assert'
import { Redis, Cluster } from 'ioredis'
import { createRedisMock, type RedisMock } from '../../src/mock'

// Exercises the public `createRedisMock().seed()` facade end-to-end: seed every
// supported type, then read it back through a real ioredis client over the
// wire — standalone and cluster mock modes both.

describe('createRedisMock seed readback via ioredis (standalone)', () => {
  let mock: RedisMock
  let client: Redis

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
    client = new Redis({ ...mock.connectionOptions(), lazyConnect: true })
    await client.connect()
  })

  after(async () => {
    client.disconnect()
    await mock.close()
  })

  test('reads back a string', async () => {
    assert.strictEqual(await client.get('user:1'), 'alice')
  })

  test('coerces a numeric string value to its decimal form', async () => {
    assert.strictEqual(await client.get('counter'), '42')
  })

  test('reads back a hash', async () => {
    assert.deepStrictEqual(await client.hgetall('h:1'), {
      name: 'bob',
      age: '30',
    })
  })

  test('reads back a list in order', async () => {
    assert.deepStrictEqual(await client.lrange('l:1', 0, -1), ['a', 'b', '1'])
  })

  test('reads back a set', async () => {
    assert.deepStrictEqual(
      new Set(await client.smembers('s:1')),
      new Set(['x', 'y']),
    )
  })

  test('reads back a zset with scores', async () => {
    assert.deepStrictEqual(await client.zrange('z:1', 0, -1, 'WITHSCORES'), [
      'a',
      '1',
      'b',
      '2',
    ])
  })

  test('applies ttlMs as a live expiration', async () => {
    const pttl = await client.pttl('ttl:1')
    assert.ok(pttl > 0 && pttl <= 50_000, `expected live ttl, got ${pttl}`)
  })

  test('routes an entry to the selected logical database', async () => {
    assert.strictEqual(await client.get('in-db-3'), null)
    await client.select(3)
    assert.strictEqual(await client.get('in-db-3'), 'scoped')
    await client.select(0)
  })

  test('flush() clears seeded data', async () => {
    await mock.flush()
    assert.strictEqual(await client.get('user:1'), null)
  })
})

describe('createRedisMock seed readback via ioredis (cluster)', () => {
  let mock: RedisMock
  let cluster: Cluster

  before(async () => {
    mock = await createRedisMock({ cluster: { masters: 3 } })
    await mock.seed([
      { key: 'user:1', type: 'string', value: 'alice' },
      { key: 'h:1', type: 'hash', value: { name: 'bob' } },
      { key: 'l:1', type: 'list', value: ['a', 'b'] },
      { key: 's:1', type: 'set', value: ['x', 'y'] },
      { key: 'z:1', type: 'zset', value: { a: 1, b: 2 } },
    ])
    cluster = new Redis.Cluster(mock.clusterNodes(), {
      lazyConnect: true,
      slotsRefreshTimeout: 10_000_000,
    })
    await cluster.connect()
  })

  after(async () => {
    await cluster.quit()
    await mock.close()
  })

  test('routes each seeded key to its slot owner, readable by the client', async () => {
    assert.strictEqual(await cluster.get('user:1'), 'alice')
    assert.deepStrictEqual(await cluster.hgetall('h:1'), { name: 'bob' })
    assert.deepStrictEqual(await cluster.lrange('l:1', 0, -1), ['a', 'b'])
    assert.deepStrictEqual(
      new Set(await cluster.smembers('s:1')),
      new Set(['x', 'y']),
    )
    assert.deepStrictEqual(await cluster.zrange('z:1', 0, -1, 'WITHSCORES'), [
      'a',
      '1',
      'b',
      '2',
    ])
  })
})
