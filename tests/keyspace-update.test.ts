import { test, describe } from 'node:test'
import assert from 'node:assert'
import { RedisDatabase } from '../src/state/database'
import { type RedisMutationEvent } from '../src/state/mutation-events'
import { WrongTypeRedisError } from '../src/core/redis-error'

// Real Redis emits the type-specific event (hdel, lpop, zrem, ...) and then
// `del` when a removal empties a collection. The removal is notification-only;
// the `delete` is the one modified-key (WATCH) signal.
function assertRemovalThenDelete(
  events: RedisMutationEvent[],
  key: Buffer,
  valueType: 'hash' | 'list' | 'zset',
): void {
  assert.deepStrictEqual(events, [
    { type: 'notify', database: 0, key, valueType },
    { type: 'delete', database: 0, key },
  ])
}

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

  test('emptying an existing collection deletes the key, announcing the removal before the delete (#379)', () => {
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
    assertRemovalThenDelete(events, key, 'hash')
  })

  test('emptying an existing list deletes the key, announcing the removal before the delete (#379)', () => {
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
    assertRemovalThenDelete(events, key, 'list')
  })

  test('emptying an existing zset deletes the key, announcing the removal before the delete (#379)', () => {
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
    assertRemovalThenDelete(events, key, 'zset')
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

  test('markCommitted on an existing key notifies without signalling key listeners (#379)', () => {
    // Real Redis announces consumer-group changes (notifyKeyspaceEvent) but
    // leaves a WATCH on the stream intact (no signalModifiedKey). Per-key
    // listeners — WATCH, blocked clients — must not see the notification.
    const { db, events } = setup()
    const key = Buffer.from('stream')
    db.updateStream(key, stream => {
      stream.appendEntry({ ms: 1, seq: 1 }, [
        Buffer.from('f'),
        Buffer.from('v'),
      ])
    })
    const keyEvents: RedisMutationEvent[] = []
    db.subscribeKey(key, event => keyEvents.push(event))
    events.length = 0

    db.updateStream(key, stream => {
      stream.addGroup('g', {
        name: Buffer.from('g'),
        lastDeliveredId: { ms: 0, seq: 0 },
        entriesRead: 0,
        consumers: new Map(),
        pending: new Map(),
      })
    })

    assert.deepStrictEqual(events, [
      { type: 'notify', database: 0, key, valueType: 'stream' },
    ])
    assert.deepStrictEqual(keyEvents, [])
    assert.strictEqual(db.getStream(key)!.groups.size, 1)
  })

  test('emptying a collection signals key listeners with the delete only (#379)', () => {
    const { db } = setup()
    const key = Buffer.from('h')
    db.updateHash(key, hash => {
      hash.setField(Buffer.from('f'), Buffer.from('v'))
    })
    const keyEvents: RedisMutationEvent[] = []
    db.subscribeKey(key, event => keyEvents.push(event))

    db.updateHash(key, hash => {
      hash.deleteField(Buffer.from('f'))
    })

    assert.deepStrictEqual(keyEvents, [{ type: 'delete', database: 0, key }])
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

describe('RedisDatabase.withOrigin — a prototype-linked view (#444)', () => {
  test('writes through a view stamp its origin and leave origin as its only own property', () => {
    // The view reads all state through to the database; a RedisDatabase
    // method that assigned `this.x` would silently land on the view instead.
    const { db, events } = setup()
    const view = db.withOrigin('x')

    view.setString(Buffer.from('s'), Buffer.from('v'))
    view.expire(Buffer.from('s'), Date.now() + 60_000)
    view.persist(Buffer.from('s'))
    view.updateHash(Buffer.from('h'), hash => {
      hash.setField(Buffer.from('f'), Buffer.from('v'), { keepTtl: false })
    })
    view.updateHash(Buffer.from('h'), hash => {
      hash.setFieldExpiration(Buffer.from('f'), Date.now() - 1)
    })
    view.updateHash(Buffer.from('h'), hash => hash.size)
    view.updateList(Buffer.from('l'), list => {
      list.pushRight([Buffer.from('a')])
    })
    view.updateList(Buffer.from('l'), list => {
      list.pop('left')
    })
    view.delete(Buffer.from('s'))
    view.flush()

    assert.deepStrictEqual(Object.getOwnPropertyNames(view), ['origin'])
    assert.strictEqual(db.origin, undefined)
    assert.strictEqual(db.size(), 0)
    // Everything is stamped with the view's origin, except the lazily purged
    // hash field, which is always published as `hexpired`.
    assert.deepStrictEqual(
      events.map(event => [event.type, event.command]),
      [
        ['write', 'x'], // SET s
        ['expire', 'x'],
        ['persist', 'x'],
        ['write', 'x'], // HSET h f
        ['write', 'x'], // HPEXPIRE h f (in the past)
        ['notify', 'hexpired'], // purge on next access empties h
        ['delete', 'hexpired'],
        ['write', 'x'], // RPUSH l
        ['notify', 'x'], // LPOP l empties it
        ['delete', 'x'],
        ['delete', 'x'], // DEL s
        ['flush', 'x'],
      ],
    )
  })
})

describe('RedisDatabase.sweepExpired — active hash-field expiry (#486 review)', () => {
  test('purges expired fields of hashes written through updateHash or set, as hexpired', () => {
    const { db, events } = setup()
    const now = Date.now()
    const field = (name: string, expiresAt?: number) => ({
      field: Buffer.from(name),
      value: Buffer.from('v'),
      expiresAt,
    })

    // Field TTL set through updateHash; the whole hash expires.
    db.updateHash(Buffer.from('whole'), hash => {
      hash.setField(Buffer.from('f'), Buffer.from('v'))
      hash.setFieldExpiration(Buffer.from('f'), now + 50)
    })
    // Field TTL arriving through set (RESTORE / COPY / replication); one of
    // two fields expires.
    db.set(Buffer.from('partial'), {
      type: 'hash',
      fields: new Map([
        ['66', field('f', now + 50)],
        ['67', field('g')],
      ]),
    })
    // No field TTL at all: never visited.
    db.updateHash(Buffer.from('plain'), hash => {
      hash.setField(Buffer.from('f'), Buffer.from('v'))
    })
    events.length = 0

    assert.strictEqual(db.sweepExpired(now), 0)
    assert.deepStrictEqual(events, [])

    assert.strictEqual(db.sweepExpired(now + 100), 1)
    assert.deepStrictEqual(
      events.map(event => [
        event.type,
        'key' in event ? event.key.toString() : null,
        event.command,
      ]),
      [
        ['notify', 'whole', 'hexpired'],
        ['delete', 'whole', 'hexpired'],
        ['write', 'partial', 'hexpired'],
      ],
    )
    assert.strictEqual(db.getType(Buffer.from('whole')), null)
    assert.deepStrictEqual(
      Array.from(db.getHash(Buffer.from('partial'))!.fields.keys()),
      ['67'],
    )

    // Nothing left to expire: a later sweep publishes nothing.
    events.length = 0
    assert.strictEqual(db.sweepExpired(now + 200), 0)
    assert.deepStrictEqual(events, [])
  })
})
