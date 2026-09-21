import { test, describe } from 'node:test'
import assert from 'node:assert'
import { RedisDatabase } from '../src/state/database'
import { type RedisMutationEvent } from '../src/state/mutation-events'
import { WrongTypeRedisError } from '../src/core/redis-error'

function setup() {
  const db = new RedisDatabase(0)
  const events: RedisMutationEvent[] = []
  db.subscribe(event => events.push(event))
  return { db, events }
}

// These exercise the shared read-modify-write path behind updateHash/
// updateList/... — `RedisDatabase.update` — through the typed wrappers, which
// is the only way production reaches it.
describe('RedisDatabase.update — ghost entries and empty-collection cleanup (#124)', () => {
  test('mutator throwing on a fresh key leaves no ghost entry and emits no event', () => {
    const { db, events } = setup()
    const key = Buffer.from('h')

    assert.throws(() => {
      db.updateHash(key, () => {
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
    db.updateHash(key, hash => {
      hash.deleteField(Buffer.from('missing'))
    })

    assert.strictEqual(db.get(key), null)
    assert.strictEqual(db.getType(key), null)
    assert.strictEqual(events.length, 0)
  })

  test('a no-op mutation on an existing collection emits no event', () => {
    const { db, events } = setup()
    const key = Buffer.from('h')

    db.updateHash(key, hash => {
      hash.setField(Buffer.from('f'), Buffer.from('v'))
    })
    events.length = 0

    const deleted = db.updateHash(key, hash =>
      hash.deleteField(Buffer.from('missing')) ? 1 : 0,
    )

    assert.strictEqual(deleted, 0)
    assert.strictEqual(db.getType(key), 'hash')
    assert.strictEqual(events.length, 0)
  })

  test('emptying an existing collection deletes the key and emits a single delete event', () => {
    const { db, events } = setup()
    const key = Buffer.from('h')

    db.updateHash(key, hash => {
      hash.setField(Buffer.from('f'), Buffer.from('v'))
    })
    assert.strictEqual(db.getType(key), 'hash')
    events.length = 0

    db.updateHash(key, hash => {
      hash.deleteField(Buffer.from('f'))
    })

    assert.strictEqual(db.get(key), null)
    assert.strictEqual(db.getType(key), null)
    assert.strictEqual(events.length, 1)
    assert.strictEqual(events[0]!.type, 'delete')
  })

  test('emptying an existing list deletes the key and emits a single delete event', () => {
    const { db, events } = setup()
    const key = Buffer.from('l')

    db.updateList(key, list => {
      list.pushRight([Buffer.from('a')])
    })
    assert.strictEqual(db.getType(key), 'list')
    events.length = 0

    // e.g. LTRIM that removes every element
    db.updateList(key, list => {
      list.trim(1, 0)
    })

    assert.strictEqual(db.get(key), null)
    assert.strictEqual(db.getType(key), null)
    assert.strictEqual(events.length, 1)
    assert.strictEqual(events[0]!.type, 'delete')
  })

  test('emptying an existing zset deletes the key and emits a single delete event', () => {
    const { db, events } = setup()
    const key = Buffer.from('z')

    db.updateSortedSet(key, zset => {
      zset.setScore(Buffer.from('m'), 1)
    })
    assert.strictEqual(db.getType(key), 'zset')
    events.length = 0

    // e.g. ZREM that removes the last member
    db.updateSortedSet(key, zset => {
      zset.deleteMember(Buffer.from('m'))
    })

    assert.strictEqual(db.get(key), null)
    assert.strictEqual(db.getType(key), null)
    assert.strictEqual(events.length, 1)
    assert.strictEqual(events[0]!.type, 'delete')
  })

  test('a populating mutation emits a write event and keeps the key', () => {
    const { db, events } = setup()
    const key = Buffer.from('s')

    db.updateSet(key, set => {
      set.addMember(Buffer.from('m'))
    })

    assert.strictEqual(db.getType(key), 'set')
    assert.strictEqual(events.length, 1)
    assert.strictEqual(events[0]!.type, 'write')
  })

  test('an empty stream is preserved (matches real Redis keeping empty streams)', () => {
    const { db, events } = setup()
    const key = Buffer.from('stream')

    // XGROUP CREATE ... MKSTREAM: creates the key with zero entries. The group
    // is committed rather than marked changed, but creating the key still
    // dirties a WATCH, so this emits a write.
    db.updateStream(key, stream => {
      stream.addGroup('g', {
        name: Buffer.from('g'),
        lastDeliveredId: { ms: 0, seq: 0 },
        entriesRead: 0,
        consumers: new Map(),
        pending: new Map(),
      })
    })

    assert.strictEqual(db.getType(key), 'stream')
    assert.strictEqual(db.getStream(key)!.entries.length, 0)
    assert.strictEqual(events.length, 1)
    assert.strictEqual(events[0]!.type, 'write')
  })

  test('updating a key held at another type throws the client-visible WRONGTYPE error', () => {
    const { db, events } = setup()
    const key = Buffer.from('str')

    db.setString(key, Buffer.from('v'))
    events.length = 0

    assert.throws(
      () => {
        db.updateHash(key, hash => {
          hash.setField(Buffer.from('f'), Buffer.from('v'))
        })
      },
      (err: unknown) => {
        assert.ok(err instanceof WrongTypeRedisError)
        assert.strictEqual(err.code, 'WRONGTYPE')
        return true
      },
    )

    assert.strictEqual(db.getType(key), 'string')
    assert.deepStrictEqual(db.getString(key), Buffer.from('v'))
    assert.strictEqual(events.length, 0)
  })
})

describe('RedisDatabase.set — empty values', () => {
  // Not an `update` test: strings never reach `update` in production. There is
  // no `updateString` wrapper, so they are written whole via `set`/`setString`,
  // which has no empty-collection rule at all. This pins the user-visible
  // invariant (`SET k ""` keeps the key) on the path that actually serves it.
  //
  // Note this does NOT cover `isEmptyCollection`'s `case 'string'` arm, which
  // is unreachable while `update` is private — see the comment on that arm in
  // src/state/database.ts.
  test('an empty string value is a real value and is never auto-deleted', () => {
    const { db } = setup()
    const key = Buffer.from('str')

    db.setString(key, Buffer.alloc(0))

    assert.strictEqual(db.getType(key), 'string')
    assert.deepStrictEqual(db.getString(key), Buffer.alloc(0))
  })
})
