import { after, before, describe, test } from 'node:test'
import assert from 'node:assert'
import { Redis } from 'ioredis'
import { createRedisMock, type RedisMock } from '../../src/mock'
import type { SeedEntry } from '../../src/seed'

const SEED: SeedEntry[] = [
  { key: 'user:1', type: 'string', value: 'alice' },
  { key: 'counter', type: 'string', value: 42 },
  { key: 'h:1', type: 'hash', value: { name: 'bob', age: 30 } },
  { key: 'l:1', type: 'list', value: ['a', 'b', 'c'] },
]

describe('socketless client matches the wire client on a tcp mock', () => {
  let mock: RedisMock
  let wire: Redis

  before(async () => {
    mock = await createRedisMock()
    await mock.seed(SEED)
    wire = new Redis(mock.connectionOptions())
  })

  after(async () => {
    wire?.disconnect()
    await mock?.close()
  })

  test('reads seeded data identically over both paths', async () => {
    const socketless = mock.client()

    assert.strictEqual(await socketless.command('GET', 'user:1'), 'alice')
    assert.strictEqual(await wire.get('user:1'), 'alice')

    assert.strictEqual(await socketless.command('INCR', 'counter'), 43)
    assert.strictEqual(await wire.get('counter'), '43')

    assert.deepStrictEqual(await socketless.command('HGETALL', 'h:1'), {
      name: 'bob',
      age: '30',
    })
    assert.deepStrictEqual(await wire.hgetall('h:1'), {
      name: 'bob',
      age: '30',
    })

    assert.deepStrictEqual(await socketless.command('LRANGE', 'l:1', 0, -1), [
      'a',
      'b',
      'c',
    ])
    assert.deepStrictEqual(await wire.lrange('l:1', 0, -1), ['a', 'b', 'c'])
  })
})

describe('memory-transport mock seed → socketless read-back', () => {
  let mock: RedisMock

  after(async () => {
    await mock?.close()
  })

  test('round-trips seeded data with no TCP listener', async () => {
    mock = await createRedisMock({ transport: 'memory' })
    await mock.seed(SEED)
    const client = mock.client()

    assert.strictEqual(await client.command('GET', 'user:1'), 'alice')
    assert.deepStrictEqual(await client.command('HGETALL', 'h:1'), {
      name: 'bob',
      age: '30',
    })
    assert.throws(() => mock.connectionOptions(), /no TCP endpoint/)
  })
})
