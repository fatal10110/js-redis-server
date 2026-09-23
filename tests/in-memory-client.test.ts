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

  test('decodes a hash reply the way the negotiated protocol shapes it', async () => {
    client = await createInMemoryClient()
    await client.command('HSET', 'h', 'name', 'bob', 'age', '30')

    // RESP2 has no map type — HGETALL is a flat array there, and only becomes
    // an object after HELLO 3. Same as real node-redis' raw reply path (#414).
    assert.deepStrictEqual(await client.command('HGETALL', 'h'), [
      'name',
      'bob',
      'age',
      '30',
    ])

    await client.command('HELLO', 3)
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

  test('compatibility option gates commands by version', async () => {
    // LMPOP arrived in Redis 7.0, so it is unknown under a 6.2 profile but
    // available on the default (redis-8.0). 6.2 quotes the name with
    // backticks (#384).
    client = await createInMemoryClient({ compatibility: 'redis-6.2' })
    await assert.rejects(
      client.command('LMPOP', '1', 'k', 'LEFT'),
      /unknown command `LMPOP`/,
    )
    client.close()

    client = await createInMemoryClient()
    assert.strictEqual(await client.command('LMPOP', '1', 'k', 'LEFT'), null)
  })

  test('spells a RESP2 double the way the profile does (#451)', async () => {
    // The socketless client never encodes: it decodes the reply's double
    // itself, so it needs the served profile as much as the encoder does.
    client = await createInMemoryClient({ compatibility: 'redis-6.2' })
    await client.command('ZADD', 'z', '0.1', 'm')
    assert.strictEqual(
      await client.command('ZSCORE', 'z', 'm'),
      '0.10000000000000001',
    )
    await client.command('GEOADD', 'g', '13.361389', '38.115556', 'p')
    assert.deepStrictEqual(await client.command('GEOPOS', 'g', 'p'), [
      ['13.36138933897018433', '38.11555639549629859'],
    ])
    client.close()

    client = await createInMemoryClient()
    await client.command('ZADD', 'z', '0.1', 'm')
    assert.strictEqual(await client.command('ZSCORE', 'z', 'm'), '0.1')
    await client.command('GEOADD', 'g', '13.361389', '38.115556', 'p')
    assert.deepStrictEqual(await client.command('GEOPOS', 'g', 'p'), [
      ['13.361389338970184', '38.1155563954963'],
    ])
  })
})
