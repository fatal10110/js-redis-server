import { test, describe, afterEach } from 'node:test'
import assert from 'node:assert'
import { createInMemoryClient } from '../src'
import type { InMemoryRedisClient } from '../src'
import { RedisCommandError } from '../src/core/redis-error'

describe('createInMemoryClient', () => {
  let client: InMemoryRedisClient

  afterEach(() => {
    client?.close()
  })

  test('round-trips a string through the socketless client', async () => {
    client = await createInMemoryClient()
    assert.strictEqual(await client.command('SET', 'k', 'v'), 'OK')
    assert.strictEqual(await client.command('GET', 'k'), 'v')
  })

  test('returns integers as numbers, not bigint', async () => {
    client = await createInMemoryClient()
    const incremented = await client.command('INCR', 'counter')
    assert.strictEqual(incremented, 1)
    assert.strictEqual(typeof incremented, 'number')
  })

  test('decodes a hash reply into an object', async () => {
    client = await createInMemoryClient()
    await client.command('HSET', 'h', 'name', 'bob', 'age', '30')
    assert.deepStrictEqual(await client.command('HGETALL', 'h'), {
      name: 'bob',
      age: '30',
    })
  })

  test('decodes a list reply into an array', async () => {
    client = await createInMemoryClient()
    await client.command('RPUSH', 'l', 'a', 'b', 'c')
    assert.deepStrictEqual(await client.command('LRANGE', 'l', 0, -1), [
      'a',
      'b',
      'c',
    ])
  })

  test('throws a RedisCommandError on an error reply', async () => {
    client = await createInMemoryClient()
    await client.command('SET', 'k', 'v')
    await assert.rejects(
      client.command('INCR', 'k'),
      (err: unknown) => err instanceof RedisCommandError,
    )
  })

  test('returnBuffers yields Buffer bulk replies', async () => {
    client = await createInMemoryClient({ returnBuffers: true })
    await client.command('SET', 'k', 'v')
    const value = await client.command('GET', 'k')
    assert.ok(Buffer.isBuffer(value))
    assert.strictEqual((value as Buffer).toString(), 'v')
  })

  test('database option selects a logical db', async () => {
    client = await createInMemoryClient({ database: 2 })
    await client.command('SET', 'k', 'v')
    await client.command('SELECT', '0')
    assert.strictEqual(await client.command('GET', 'k'), null)
    await client.command('SELECT', '2')
    assert.strictEqual(await client.command('GET', 'k'), 'v')
  })

  test('seed pre-populates the keyspace', async () => {
    client = await createInMemoryClient({
      seed: [{ key: 'k', type: 'string', value: 'v' }],
    })
    assert.strictEqual(await client.command('GET', 'k'), 'v')
  })

  test('rejects commands after close', async () => {
    client = await createInMemoryClient()
    client.close()
    await assert.rejects(client.command('PING'), /closed/)
  })
})
