import { test, describe, afterEach } from 'node:test'
import assert from 'node:assert'
import { RedisServerState } from '../src/state'
import { seedStandalone } from '../src/seed'

describe('seedStandalone', () => {
  let state: RedisServerState

  function newState(): RedisServerState {
    state = new RedisServerState({ databaseCount: 16 })
    return state
  }

  afterEach(() => {
    state?.close()
  })

  test('seeds a string value', async () => {
    const s = newState()
    await seedStandalone(s, [{ key: 'user:1', type: 'string', value: 'alice' }])
    assert.strictEqual(
      s.getDatabase(0).getString(Buffer.from('user:1'))?.toString(),
      'alice',
    )
  })

  test('coerces numeric string values to their decimal form', async () => {
    const s = newState()
    await seedStandalone(s, [{ key: 'counter', type: 'string', value: 42 }])
    assert.strictEqual(
      s.getDatabase(0).getString(Buffer.from('counter'))?.toString(),
      '42',
    )
  })

  test('seeds a hash value', async () => {
    const s = newState()
    await seedStandalone(s, [
      { key: 'h:1', type: 'hash', value: { name: 'bob', age: 30 } },
    ])
    const hash = s.getDatabase(0).getHash(Buffer.from('h:1'))
    const fields = new Map(
      Array.from(hash!.fields.values()).map(f => [
        f.field.toString(),
        f.value.toString(),
      ]),
    )
    assert.deepStrictEqual(Object.fromEntries(fields), {
      name: 'bob',
      age: '30',
    })
  })

  test('seeds a list value in order', async () => {
    const s = newState()
    await seedStandalone(s, [
      { key: 'l:1', type: 'list', value: ['a', 'b', 1] },
    ])
    const list = s.getDatabase(0).getList(Buffer.from('l:1'))
    assert.deepStrictEqual(
      list!.values.map(v => v.toString()),
      ['a', 'b', '1'],
    )
  })

  test('seeds a set value', async () => {
    const s = newState()
    await seedStandalone(s, [{ key: 's:1', type: 'set', value: ['x', 'y'] }])
    const set = s.getDatabase(0).getSet(Buffer.from('s:1'))
    const members = new Set(
      Array.from(set!.members.values()).map(m => m.toString()),
    )
    assert.deepStrictEqual(members, new Set(['x', 'y']))
  })

  test('seeds a zset value with scores', async () => {
    const s = newState()
    await seedStandalone(s, [
      { key: 'z:1', type: 'zset', value: { a: 1, b: 2 } },
    ])
    const zset = s.getDatabase(0).getSortedSet(Buffer.from('z:1'))
    const members = new Map(
      Array.from(zset!.members.values()).map(m => [
        m.member.toString(),
        m.score,
      ]),
    )
    assert.deepStrictEqual(Object.fromEntries(members), { a: 1, b: 2 })
  })

  test('applies ttlMs as a key expiration', async () => {
    const s = newState()
    await seedStandalone(s, [
      { key: 'h:ttl', type: 'hash', value: { name: 'bob' }, ttlMs: 5000 },
    ])
    const expiration = s.getDatabase(0).getExpiration(Buffer.from('h:ttl'))
    assert.strictEqual(expiration.kind, 'expires')
  })

  test('rejects a non-positive ttlMs', async () => {
    const s = newState()
    await assert.rejects(
      seedStandalone(s, [{ key: 'k', type: 'string', value: 'v', ttlMs: 0 }]),
      /Invalid ttlMs/,
    )
  })

  test('routes an entry to the selected logical database', async () => {
    const s = newState()
    await seedStandalone(s, [{ key: 'k', type: 'string', value: 'v', db: 3 }])
    assert.strictEqual(s.getDatabase(0).getString(Buffer.from('k')), null)
    assert.strictEqual(
      s.getDatabase(3).getString(Buffer.from('k'))?.toString(),
      'v',
    )
  })
})
