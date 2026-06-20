import { test, describe, afterEach } from 'node:test'
import assert from 'node:assert'
import { createRedisMock, type RedisMock } from '../src/mock'
import { RedisCommandError } from '../src/core/redis-error'

describe('InMemoryRedisClient (via createRedisMock)', () => {
  let mock: RedisMock

  afterEach(async () => {
    await mock?.close()
  })

  test('round-trips a string through the socketless client', async () => {
    mock = await createRedisMock({ transport: 'memory' })
    const client = mock.client()
    assert.strictEqual(await client.command('SET', 'k', 'v'), 'OK')
    assert.strictEqual(await client.command('GET', 'k'), 'v')
  })

  test('returns integers as numbers, not bigint', async () => {
    mock = await createRedisMock({ transport: 'memory' })
    const client = mock.client()
    const incremented = await client.command('INCR', 'counter')
    assert.strictEqual(incremented, 1)
    assert.strictEqual(typeof incremented, 'number')
  })

  test('decodes a hash reply into an object', async () => {
    mock = await createRedisMock({ transport: 'memory' })
    const client = mock.client()
    await client.command('HSET', 'h', 'name', 'bob', 'age', '30')
    assert.deepStrictEqual(await client.command('HGETALL', 'h'), {
      name: 'bob',
      age: '30',
    })
  })

  test('decodes a list reply into an array', async () => {
    mock = await createRedisMock({ transport: 'memory' })
    const client = mock.client()
    await client.command('RPUSH', 'l', 'a', 'b', 'c')
    assert.deepStrictEqual(await client.command('LRANGE', 'l', 0, -1), [
      'a',
      'b',
      'c',
    ])
  })

  test('throws a RedisCommandError on an error reply', async () => {
    mock = await createRedisMock({ transport: 'memory' })
    const client = mock.client()
    await client.command('SET', 'k', 'v')
    await assert.rejects(
      client.command('INCR', 'k'),
      (err: unknown) => err instanceof RedisCommandError,
    )
  })

  test('returnBuffers yields Buffer bulk replies', async () => {
    mock = await createRedisMock({ transport: 'memory' })
    const client = mock.client({ returnBuffers: true })
    await client.command('SET', 'k', 'v')
    const value = await client.command('GET', 'k')
    assert.ok(Buffer.isBuffer(value))
    assert.strictEqual((value as Buffer).toString(), 'v')
  })

  test('database option selects a logical db', async () => {
    mock = await createRedisMock({ transport: 'memory' })
    const onDb2 = mock.client({ database: 2 })
    await onDb2.command('SET', 'k', 'v')
    const onDb0 = mock.client()
    assert.strictEqual(await onDb0.command('GET', 'k'), null)
    assert.strictEqual(await onDb2.command('GET', 'k'), 'v')
  })

  test('rejects commands after close', async () => {
    mock = await createRedisMock({ transport: 'memory' })
    const client = mock.client()
    client.close()
    await assert.rejects(client.command('PING'), /closed/)
  })
})

describe('createRedisMock memory transport', () => {
  let mock: RedisMock

  afterEach(async () => {
    await mock?.close()
  })

  test('has no TCP endpoint', async () => {
    mock = await createRedisMock({ transport: 'memory' })
    assert.throws(() => mock.connectionOptions(), /no TCP endpoint/)
    assert.throws(() => mock.clusterNodes(), /no TCP endpoint/)
    assert.throws(() => mock.host, /no TCP endpoint/)
    assert.throws(() => mock.port, /no TCP endpoint/)
  })
})

describe('createRedisMock client() on other modes', () => {
  test('tcp standalone mock exposes a working socketless client', async () => {
    const mock = await createRedisMock()
    try {
      const client = mock.client()
      await client.command('SET', 'k', 'v')
      assert.strictEqual(await client.command('GET', 'k'), 'v')
    } finally {
      await mock.close()
    }
  })

  test('cluster mock client() throws', async () => {
    const mock = await createRedisMock({ cluster: { masters: 3 } })
    try {
      assert.throws(() => mock.client(), /not supported for cluster/)
    } finally {
      await mock.close()
    }
  })

  test('memory transport is rejected for cluster mocks', async () => {
    await assert.rejects(
      createRedisMock({ cluster: { masters: 3 }, transport: 'memory' }),
      /not supported for cluster/,
    )
  })
})
