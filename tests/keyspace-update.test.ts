import { test, describe } from 'node:test'
import assert from 'node:assert'
import { RedisKeyspace } from '../src/state/keyspace'
import {
  RedisMutationBus,
  type RedisMutationEvent,
} from '../src/state/mutation-events'
import {
  createHashData,
  createListData,
  createSetData,
  createSortedSetData,
  createStreamData,
  createStringData,
  type RedisHashData,
  type RedisListData,
  type RedisSetData,
  type RedisSortedSetData,
  type RedisStreamData,
  type RedisStringData,
} from '../src/state/data-types'

function setup() {
  const bus = new RedisMutationBus()
  const events: RedisMutationEvent[] = []
  bus.subscribe(event => events.push(event))
  const keyspace = new RedisKeyspace(0, bus)
  return { keyspace, events }
}

describe('RedisKeyspace.update — ghost entries and empty-collection cleanup (#124)', () => {
  test('mutator throwing on a fresh key leaves no ghost entry and emits no event', () => {
    const { keyspace, events } = setup()
    const key = Buffer.from('h')

    assert.throws(() => {
      keyspace.update<RedisHashData, void>(key, 'hash', createHashData, () => {
        throw new Error('boom')
      })
    }, /boom/)

    assert.strictEqual(keyspace.get(key), null)
    assert.strictEqual(keyspace.getType(key), null)
    assert.strictEqual(events.length, 0)
  })

  test('a mutation that leaves a freshly-created collection empty creates no key and emits no event', () => {
    const { keyspace, events } = setup()
    const key = Buffer.from('h')

    // e.g. HDEL on a non-existent key: there is nothing to remove, the
    // collection stays empty, so the key must never appear.
    keyspace.update<RedisHashData, void>(key, 'hash', createHashData, () => {})

    assert.strictEqual(keyspace.get(key), null)
    assert.strictEqual(keyspace.getType(key), null)
    assert.strictEqual(events.length, 0)
  })

  test('emptying an existing collection deletes the key and emits a single delete event', () => {
    const { keyspace, events } = setup()
    const key = Buffer.from('h')

    keyspace.update<RedisHashData, void>(key, 'hash', createHashData, hash => {
      hash.fields.set('f', { field: Buffer.from('f'), value: Buffer.from('v') })
    })
    assert.strictEqual(keyspace.getType(key), 'hash')
    events.length = 0

    keyspace.update<RedisHashData, void>(key, 'hash', createHashData, hash => {
      hash.fields.delete('f')
    })

    assert.strictEqual(keyspace.get(key), null)
    assert.strictEqual(keyspace.getType(key), null)
    assert.strictEqual(events.length, 1)
    assert.strictEqual(events[0]!.type, 'delete')
  })

  test('emptying an existing list deletes the key and emits a single delete event', () => {
    const { keyspace, events } = setup()
    const key = Buffer.from('l')

    keyspace.update<RedisListData, void>(key, 'list', createListData, list => {
      list.values.push(Buffer.from('a'))
    })
    assert.strictEqual(keyspace.getType(key), 'list')
    events.length = 0

    // e.g. LTRIM that removes every element
    keyspace.update<RedisListData, void>(key, 'list', createListData, list => {
      list.values.length = 0
    })

    assert.strictEqual(keyspace.get(key), null)
    assert.strictEqual(keyspace.getType(key), null)
    assert.strictEqual(events.length, 1)
    assert.strictEqual(events[0]!.type, 'delete')
  })

  test('emptying an existing zset deletes the key and emits a single delete event', () => {
    const { keyspace, events } = setup()
    const key = Buffer.from('z')

    keyspace.update<RedisSortedSetData, void>(
      key,
      'zset',
      createSortedSetData,
      zset => {
        zset.members.set('m', { member: Buffer.from('m'), score: 1 })
      },
    )
    assert.strictEqual(keyspace.getType(key), 'zset')
    events.length = 0

    // e.g. ZREM that removes the last member
    keyspace.update<RedisSortedSetData, void>(
      key,
      'zset',
      createSortedSetData,
      zset => {
        zset.members.delete('m')
      },
    )

    assert.strictEqual(keyspace.get(key), null)
    assert.strictEqual(keyspace.getType(key), null)
    assert.strictEqual(events.length, 1)
    assert.strictEqual(events[0]!.type, 'delete')
  })

  test('a populating mutation emits a write event and keeps the key', () => {
    const { keyspace, events } = setup()
    const key = Buffer.from('s')

    keyspace.update<RedisSetData, void>(key, 'set', createSetData, set => {
      set.members.set('m', Buffer.from('m'))
    })

    assert.strictEqual(keyspace.getType(key), 'set')
    assert.strictEqual(events.length, 1)
    assert.strictEqual(events[0]!.type, 'write')
  })

  test('an empty string value is a real value and is never auto-deleted', () => {
    const { keyspace } = setup()
    const key = Buffer.from('str')

    keyspace.update<RedisStringData, void>(
      key,
      'string',
      () => createStringData(Buffer.alloc(0)),
      str => {
        str.value = Buffer.alloc(0)
      },
    )

    assert.strictEqual(keyspace.getType(key), 'string')
  })

  test('an empty stream is preserved (matches real Redis keeping empty streams)', () => {
    const { keyspace } = setup()
    const key = Buffer.from('stream')

    keyspace.update<RedisStreamData, void>(
      key,
      'stream',
      createStreamData,
      () => {
        // Create the stream without adding entries (e.g. XGROUP CREATE MKSTREAM).
      },
    )

    assert.strictEqual(keyspace.getType(key), 'stream')
  })
})
