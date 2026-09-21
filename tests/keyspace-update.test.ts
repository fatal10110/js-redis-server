import { test, describe } from 'node:test'
import assert from 'node:assert'
import { RedisDatabase } from '../src/state/database'
import { type RedisMutationEvent } from '../src/state/mutation-events'
import { WrongTypeRedisError } from '../src/core/redis-error'
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
  const db = new RedisDatabase(0)
  const events: RedisMutationEvent[] = []
  db.subscribe(event => events.push(event))
  return { db, events }
}

describe('RedisDatabase.update — ghost entries and empty-collection cleanup (#124)', () => {
  test('mutator throwing on a fresh key leaves no ghost entry and emits no event', () => {
    const { db, events } = setup()
    const key = Buffer.from('h')

    assert.throws(() => {
      db.update<RedisHashData, void>(key, 'hash', createHashData, () => {
        throw new Error('boom')
      })
    }, /boom/)

    assert.strictEqual(db.get(key), null)
    assert.strictEqual(db.getType(key), null)
    assert.strictEqual(events.length, 0)
  })

  test('a mutation that leaves a freshly-created collection empty creates no key and emits no event', () => {
    const { db, events } = setup()
    const key = Buffer.from('h')

    // e.g. HDEL on a non-existent key: there is nothing to remove, the
    // collection stays empty, so the key must never appear.
    db.update<RedisHashData, void>(key, 'hash', createHashData, () => {
      // no change
    })

    assert.strictEqual(db.get(key), null)
    assert.strictEqual(db.getType(key), null)
    assert.strictEqual(events.length, 0)
  })

  test('a no-op mutation on an existing collection emits no event', () => {
    const { db, events } = setup()
    const key = Buffer.from('h')

    db.update<RedisHashData, void>(
      key,
      'hash',
      createHashData,
      (hash, tracker) => {
        hash.fields.set('f', {
          field: Buffer.from('f'),
          value: Buffer.from('v'),
        })
        tracker.markChanged()
      },
    )
    events.length = 0

    db.update<RedisHashData, number>(key, 'hash', createHashData, hash => {
      const deleted = hash.fields.delete('missing') ? 1 : 0
      return deleted
    })

    assert.strictEqual(db.getType(key), 'hash')
    assert.strictEqual(events.length, 0)
  })

  test('emptying an existing collection deletes the key and emits a single delete event', () => {
    const { db, events } = setup()
    const key = Buffer.from('h')

    db.update<RedisHashData, void>(
      key,
      'hash',
      createHashData,
      (hash, tracker) => {
        hash.fields.set('f', {
          field: Buffer.from('f'),
          value: Buffer.from('v'),
        })
        tracker.markChanged()
      },
    )
    assert.strictEqual(db.getType(key), 'hash')
    events.length = 0

    db.update<RedisHashData, void>(
      key,
      'hash',
      createHashData,
      (hash, tracker) => {
        hash.fields.delete('f')
        tracker.markChanged()
      },
    )

    assert.strictEqual(db.get(key), null)
    assert.strictEqual(db.getType(key), null)
    assert.strictEqual(events.length, 1)
    assert.strictEqual(events[0]!.type, 'delete')
  })

  test('emptying an existing list deletes the key and emits a single delete event', () => {
    const { db, events } = setup()
    const key = Buffer.from('l')

    db.update<RedisListData, void>(
      key,
      'list',
      createListData,
      (list, tracker) => {
        list.values.push(Buffer.from('a'))
        tracker.markChanged()
      },
    )
    assert.strictEqual(db.getType(key), 'list')
    events.length = 0

    // e.g. LTRIM that removes every element
    db.update<RedisListData, void>(
      key,
      'list',
      createListData,
      (list, tracker) => {
        list.values.length = 0
        tracker.markChanged()
      },
    )

    assert.strictEqual(db.get(key), null)
    assert.strictEqual(db.getType(key), null)
    assert.strictEqual(events.length, 1)
    assert.strictEqual(events[0]!.type, 'delete')
  })

  test('emptying an existing zset deletes the key and emits a single delete event', () => {
    const { db, events } = setup()
    const key = Buffer.from('z')

    db.update<RedisSortedSetData, void>(
      key,
      'zset',
      createSortedSetData,
      (zset, tracker) => {
        zset.members.set('m', { member: Buffer.from('m'), score: 1 })
        tracker.markChanged()
      },
    )
    assert.strictEqual(db.getType(key), 'zset')
    events.length = 0

    // e.g. ZREM that removes the last member
    db.update<RedisSortedSetData, void>(
      key,
      'zset',
      createSortedSetData,
      (zset, tracker) => {
        zset.members.delete('m')
        tracker.markChanged()
      },
    )

    assert.strictEqual(db.get(key), null)
    assert.strictEqual(db.getType(key), null)
    assert.strictEqual(events.length, 1)
    assert.strictEqual(events[0]!.type, 'delete')
  })

  test('a populating mutation emits a write event and keeps the key', () => {
    const { db, events } = setup()
    const key = Buffer.from('s')

    db.update<RedisSetData, void>(key, 'set', createSetData, (set, tracker) => {
      set.members.set('m', Buffer.from('m'))
      tracker.markChanged()
    })

    assert.strictEqual(db.getType(key), 'set')
    assert.strictEqual(events.length, 1)
    assert.strictEqual(events[0]!.type, 'write')
  })

  test('an empty string value is a real value and is never auto-deleted', () => {
    const { db } = setup()
    const key = Buffer.from('str')

    db.update<RedisStringData, void>(
      key,
      'string',
      () => createStringData(Buffer.alloc(0)),
      (str, tracker) => {
        str.value = Buffer.alloc(0)
        tracker.markChanged()
      },
    )

    assert.strictEqual(db.getType(key), 'string')
  })

  test('an empty stream is preserved (matches real Redis keeping empty streams)', () => {
    const { db } = setup()
    const key = Buffer.from('stream')

    db.update<RedisStreamData, void>(
      key,
      'stream',
      createStreamData,
      (_stream, tracker) => {
        // Create the stream without adding entries (e.g. XGROUP CREATE MKSTREAM).
        tracker.markChanged()
      },
    )

    assert.strictEqual(db.getType(key), 'stream')
  })

  test('updating a key held at another type throws the client-visible WRONGTYPE error', () => {
    const { db, events } = setup()
    const key = Buffer.from('str')

    db.setString(key, Buffer.from('v'))
    events.length = 0

    assert.throws(
      () => {
        db.update<RedisHashData, void>(
          key,
          'hash',
          createHashData,
          (hash, tracker) => {
            hash.fields.set('f', {
              field: Buffer.from('f'),
              value: Buffer.from('v'),
            })
            tracker.markChanged()
          },
        )
      },
      (err: unknown) => {
        assert.ok(err instanceof WrongTypeRedisError)
        assert.strictEqual(err.code, 'WRONGTYPE')
        return true
      },
    )

    assert.strictEqual(db.getType(key), 'string')
    assert.strictEqual(events.length, 0)
  })
})
