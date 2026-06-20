import { after, before, describe, test } from 'node:test'
import assert from 'node:assert'
import { Redis, Cluster } from 'ioredis'
import { createRedisMock, type RedisMock } from '../../src/mock'
import type { SeedEntry } from '../../src/seed'

const SEED: SeedEntry[] = [
  { key: 'user:1', type: 'string', value: 'alice' },
  { key: 'counter', type: 'string', value: 42 },
  { key: 'h:1', type: 'hash', value: { name: 'bob', age: 30 } },
  { key: 'l:1', type: 'list', value: ['a', 'b', 'c'] },
  { key: 's:1', type: 'set', value: ['x', 'y'] },
  { key: 'z:1', type: 'zset', value: { a: 1, b: 2 } },
  { key: 'ttl:1', type: 'string', value: 'soon', ttlMs: 50_000 },
]

async function assertSeedReadable(client: Redis | Cluster): Promise<void> {
  assert.strictEqual(await client.get('user:1'), 'alice')
  assert.strictEqual(await client.get('counter'), '42')
  assert.deepStrictEqual(await client.hgetall('h:1'), {
    name: 'bob',
    age: '30',
  })
  assert.deepStrictEqual(await client.lrange('l:1', 0, -1), ['a', 'b', 'c'])
  assert.deepStrictEqual((await client.smembers('s:1')).sort(), ['x', 'y'])
  assert.deepStrictEqual(await client.zrange('z:1', 0, -1, 'WITHSCORES'), [
    'a',
    '1',
    'b',
    '2',
  ])
  const pttl = await client.pttl('ttl:1')
  assert.ok(pttl > 0 && pttl <= 50_000, `expected live TTL, got ${pttl}`)
}

describe('createRedisMock standalone seed → ioredis read-back', () => {
  let mock: RedisMock
  let client: Redis

  before(async () => {
    mock = await createRedisMock()
    await mock.seed(SEED)
    client = new Redis(mock.connectionOptions())
  })

  after(async () => {
    client?.disconnect()
    await mock?.close()
  })

  test('reads every seeded type back through ioredis', async () => {
    await assertSeedReadable(client)
  })

  test('flush() clears seeded data', async () => {
    await mock.flush()
    assert.strictEqual(await client.get('user:1'), null)
  })
})

describe('createRedisMock cluster seed → ioredis read-back', () => {
  let mock: RedisMock
  let client: Cluster

  before(async () => {
    mock = await createRedisMock({ cluster: { masters: 3 } })
    await mock.seed(SEED)
    client = new Redis.Cluster(mock.clusterNodes(), {
      lazyConnect: true,
      slotsRefreshTimeout: 10_000_000,
    })
    await client.connect()
  })

  after(async () => {
    client?.disconnect()
    await mock?.close()
  })

  test('reads every seeded type back through an ioredis cluster client', async () => {
    await assertSeedReadable(client)
  })
})
