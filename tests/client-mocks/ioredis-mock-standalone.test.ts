import { describe, test, before, after, beforeEach } from 'node:test'
import assert from 'node:assert'
import type { Redis } from 'ioredis'
import { createIoredisMock } from '../../src/index'

describe('createIoredisMock — standalone', () => {
  let redis: Redis

  before(async () => {
    redis = (await createIoredisMock()) as Redis
  })

  after(async () => {
    await redis.quit()
  })

  beforeEach(async () => {
    await redis.flushall()
  })

  test('string round-trip via typed methods', async () => {
    assert.strictEqual(await redis.set('k', 'v'), 'OK')
    assert.strictEqual(await redis.get('k'), 'v')
    assert.strictEqual(await redis.get('missing'), null)
  })

  test('incr / append', async () => {
    assert.strictEqual(await redis.incr('counter'), 1)
    assert.strictEqual(await redis.incrby('counter', 4), 5)
  })

  test('hash round-trip', async () => {
    await redis.hset('h', 'f1', 'a', 'f2', 'b')
    assert.strictEqual(await redis.hget('h', 'f1'), 'a')
    assert.deepStrictEqual(await redis.hgetall('h'), { f1: 'a', f2: 'b' })
  })

  test('list round-trip', async () => {
    await redis.rpush('list', 'a', 'b', 'c')
    assert.deepStrictEqual(await redis.lrange('list', 0, -1), ['a', 'b', 'c'])
    assert.strictEqual(await redis.lpop('list'), 'a')
  })

  test('sorted-set round-trip with WITHSCORES', async () => {
    await redis.zadd('z', 1, 'a', 2, 'b', 3, 'c')
    assert.deepStrictEqual(await redis.zrange('z', 0, -1), ['a', 'b', 'c'])
    assert.deepStrictEqual(await redis.zrange('z', 0, -1, 'WITHSCORES'), [
      'a',
      '1',
      'b',
      '2',
      'c',
      '3',
    ])
  })

  test('multi / exec runs queued commands atomically', async () => {
    const results = await redis.multi().set('a', '1').incr('a').get('a').exec()
    assert.deepStrictEqual(results, [
      [null, 'OK'],
      [null, 2],
      [null, '2'],
    ])
  })

  test('expire / ttl', async () => {
    await redis.set('temp', 'v')
    assert.strictEqual(await redis.expire('temp', 100), 1)
    const ttl = await redis.ttl('temp')
    assert.ok(ttl > 90 && ttl <= 100, `ttl was ${ttl}`)
  })

  test('WRONGTYPE wording surfaces through the client', async () => {
    await redis.set('str', 'v')
    await assert.rejects(
      () => redis.lpush('str', 'x'),
      /WRONGTYPE Operation against a key holding the wrong kind of value/,
    )
  })
})
