import { describe, test, after } from 'node:test'
import assert from 'node:assert'
import type { Redis, Cluster } from 'ioredis'
import { createIoredisMock } from '../../src/index'

describe('createIoredisMock — seed (standalone)', () => {
  let redis: Redis

  after(async () => {
    await redis?.quit()
  })

  test('seeds every value type and reads it back through the client', async () => {
    redis = (await createIoredisMock({
      seed: [
        { key: 'user:1', type: 'string', value: 'alice' },
        { key: 'counter', type: 'string', value: 42 },
        { key: 'h:1', type: 'hash', value: { name: 'bob', age: 30 } },
        { key: 'l:1', type: 'list', value: ['a', 'b', 1] },
        { key: 's:1', type: 'set', value: ['x', 'y'] },
        { key: 'z:1', type: 'zset', value: { a: 1, b: 2 } },
      ],
    })) as Redis

    assert.strictEqual(await redis.get('user:1'), 'alice')
    assert.strictEqual(await redis.get('counter'), '42')
    assert.deepStrictEqual(await redis.hgetall('h:1'), {
      name: 'bob',
      age: '30',
    })
    assert.deepStrictEqual(await redis.lrange('l:1', 0, -1), ['a', 'b', '1'])
    assert.deepStrictEqual((await redis.smembers('s:1')).sort(), ['x', 'y'])
    assert.deepStrictEqual(await redis.zrange('z:1', 0, -1, 'WITHSCORES'), [
      'a',
      '1',
      'b',
      '2',
    ])
  })

  test('honours ttlMs and db placement', async () => {
    redis = (await createIoredisMock({
      seed: [
        { key: 'temp', type: 'string', value: 'x', ttlMs: 50_000 },
        { key: 'scoped', type: 'string', value: 'in-db-3', db: 3 },
      ],
    })) as Redis

    const ttl = await redis.pttl('temp')
    assert.ok(ttl > 40_000 && ttl <= 50_000, `pttl was ${ttl}`)

    assert.strictEqual(await redis.get('scoped'), null) // not in db 0
    await redis.select(3)
    assert.strictEqual(await redis.get('scoped'), 'in-db-3')
  })
})

describe('createIoredisMock — seed (cluster)', () => {
  let cluster: Cluster

  after(async () => {
    await cluster?.quit()
  })

  test('routes seeded keys to their owning master', async () => {
    cluster = (await createIoredisMock({
      cluster: { masters: 3 },
      seed: [
        { key: 'alpha', type: 'string', value: '1' },
        { key: 'beta', type: 'string', value: '2' },
        { key: 'gamma', type: 'string', value: '3' },
        { key: 'h:1', type: 'hash', value: { f: 'v' } },
      ],
    })) as Cluster

    assert.strictEqual(await cluster.get('alpha'), '1')
    assert.strictEqual(await cluster.get('beta'), '2')
    assert.strictEqual(await cluster.get('gamma'), '3')
    assert.deepStrictEqual(await cluster.hgetall('h:1'), { f: 'v' })
  })
})
