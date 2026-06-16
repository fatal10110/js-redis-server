import { describe, test } from 'node:test'
import assert from 'node:assert'
import { RedisServerState, WrongTypeRedisError, createStringData } from '../src'
import type {
  RedisDataValue,
  RedisMonitorCommandEvent,
  RedisMutationEvent,
} from '../src'

describe('new Redis state core', () => {
  test('keeps script cache separate from database flush', () => {
    const server = new RedisServerState({ databaseCount: 2 })
    const db = server.getDatabase(0)
    const sha = server.scriptCache.load(Buffer.from('return 1'))

    db.setString(Buffer.from('key'), Buffer.from('value'))
    db.flush()

    assert.strictEqual(db.get(Buffer.from('key')), null)
    assert.strictEqual(server.scriptCache.exists(sha), true)
    assert.deepStrictEqual(server.scriptCache.get(sha), Buffer.from('return 1'))
  })

  test('stores keys and values byte-safely', () => {
    const server = new RedisServerState()
    const db = server.getDatabase(0)
    const binaryKey = Buffer.from([0x00, 0xff])
    const textKey = Buffer.from('00ff')
    const binaryValue = Buffer.from([0x80, 0x00, 0x81])

    db.setString(binaryKey, binaryValue)
    db.setString(textKey, Buffer.from('text'))

    assert.deepStrictEqual(db.getString(binaryKey), binaryValue)
    assert.deepStrictEqual(db.getString(textKey), Buffer.from('text'))
    assert.strictEqual(db.size(), 2)
  })

  test('emits centralized mutation events for direct writes and in-place updates', () => {
    const server = new RedisServerState()
    const db = server.getDatabase(0)
    const key = Buffer.from('list')
    const events: RedisMutationEvent[] = []
    const keyEvents: RedisMutationEvent[] = []

    db.subscribe(event => events.push(event))
    db.subscribeKey(key, event => keyEvents.push(event))

    db.setString(Buffer.from('string'), Buffer.from('value'))
    db.updateList(key, list => {
      list.values.push(Buffer.from('a'))
      list.values.push(Buffer.from('b'))
      return { result: list.values.length, changed: true }
    })

    assert.deepStrictEqual(
      events.map(event => event.type),
      ['write', 'write'],
    )
    assert.strictEqual(keyEvents.length, 1)
    assert.strictEqual(keyEvents[0].type, 'write')

    const stored = db.get(key)
    assertListValues(stored, ['a', 'b'])
  })

  test('clears, preserves, and lazily evicts expirations through central APIs', async () => {
    const server = new RedisServerState()
    const db = server.getDatabase(0)
    const key = Buffer.from('expiring')
    const events: RedisMutationEvent[] = []
    db.subscribeKey(key, event => events.push(event))

    db.setString(key, Buffer.from('old'), { expiresAt: Date.now() + 10_000 })
    assert.strictEqual(db.getExpiration(key).kind, 'expires')

    db.setString(key, Buffer.from('new'))
    assert.deepStrictEqual(db.getExpiration(key), { kind: 'persistent' })

    db.setString(key, Buffer.from('ttl'), { expiresAt: Date.now() + 10_000 })
    const previousExpiration = db.getExpiration(key)
    db.setString(key, Buffer.from('kept'), { keepTtl: true })
    assert.deepStrictEqual(db.getExpiration(key), previousExpiration)

    db.expire(key, Date.now() - 1)
    assert.strictEqual(db.get(key), null)
    assert.strictEqual(events.at(-1)?.type, 'evict')
  })

  test('returns defensive clones from reads and mutation events', () => {
    const server = new RedisServerState()
    const db = server.getDatabase(0)
    const key = Buffer.from('hash')
    let eventValue: RedisDataValue | null = null
    db.subscribeKey(key, event => {
      if (event.type === 'write') {
        eventValue = event.value
      }
    })

    db.updateHash(key, hash => {
      hash.fields.set('field', {
        field: Buffer.from('field'),
        value: Buffer.from('value'),
      })
      return { result: undefined, changed: true }
    })

    const firstRead = db.get(key)
    if (!firstRead || firstRead.type !== 'hash') {
      assert.fail('Expected hash value')
    }

    firstRead.fields.get('field')!.value.write('x')
    assertHashValue(db.get(key), 'value')

    if (!eventValue || eventValue.type !== 'hash') {
      assert.fail('Expected hash event value')
    }

    eventValue.fields.get('field')!.value.write('y')
    assertHashValue(db.get(key), 'value')
  })

  test('throws explicit wrong-type errors from update helpers', () => {
    const server = new RedisServerState()
    const db = server.getDatabase(0)
    const key = Buffer.from('key')
    db.set(key, createStringData(Buffer.from('value')))

    assert.throws(
      () =>
        db.updateList(key, list => {
          list.values.push(Buffer.from('x'))
          return { result: undefined, changed: true }
        }),
      WrongTypeRedisError,
    )
  })

  test('mutation listeners are snapshotted during emit', () => {
    const server = new RedisServerState()
    const db = server.getDatabase(0)
    const calls: string[] = []
    let unsubscribeSecond: (() => void) | undefined

    db.subscribe(() => {
      calls.push('first')
      unsubscribeSecond?.()
    })
    unsubscribeSecond = db.subscribe(() => {
      calls.push('second')
    })

    db.setString(Buffer.from('key'), Buffer.from('value'))

    assert.deepStrictEqual(calls, ['first', 'second'])
  })

  test('monitor command events are cloned and listener subscriptions are tracked', () => {
    const server = new RedisServerState()
    const firstEvents: RedisMonitorCommandEvent[] = []
    const secondEvents: RedisMonitorCommandEvent[] = []
    let secondCalls = 0
    let unsubscribeSecond: (() => void) | undefined

    const unsubscribeFirst = server.monitorFeed.subscribe(event => {
      firstEvents.push(cloneMonitorEventForTest(event))
      event.command.write('x')
      event.args[0].write('y')
      unsubscribeSecond?.()
    })
    unsubscribeSecond = server.monitorFeed.subscribe(event => {
      secondCalls++
      secondEvents.push(cloneMonitorEventForTest(event))
      event.args[1].write('z')
    })

    assert.strictEqual(server.monitorFeed.subscriberCount, 2)

    const command = Buffer.from('SET')
    const key = Buffer.from('key')
    const value = Buffer.from('value')

    server.monitorFeed.publish({
      timestampMs: 1234,
      database: 2,
      clientId: 'client-1',
      clientAddress: '127.0.0.1:5000',
      command,
      args: [key, value],
    })

    assert.strictEqual(secondCalls, 1)
    assert.strictEqual(server.monitorFeed.subscriberCount, 1)
    assert.deepStrictEqual(command, Buffer.from('SET'))
    assert.deepStrictEqual(key, Buffer.from('key'))
    assert.deepStrictEqual(value, Buffer.from('value'))
    assert.deepStrictEqual(firstEvents[0].command, Buffer.from('SET'))
    assert.deepStrictEqual(firstEvents[0].args, [
      Buffer.from('key'),
      Buffer.from('value'),
    ])
    assert.deepStrictEqual(secondEvents[0].command, Buffer.from('SET'))
    assert.deepStrictEqual(secondEvents[0].args, [
      Buffer.from('key'),
      Buffer.from('value'),
    ])

    unsubscribeFirst()
    assert.strictEqual(server.monitorFeed.subscriberCount, 0)
  })
})

function cloneMonitorEventForTest(
  event: RedisMonitorCommandEvent,
): RedisMonitorCommandEvent {
  return {
    ...event,
    command: Buffer.from(event.command),
    args: event.args.map(arg => Buffer.from(arg)),
  }
}

function assertListValues(value: RedisDataValue | null, expected: string[]) {
  if (!value || value.type !== 'list') {
    assert.fail('Expected list value')
  }

  assert.deepStrictEqual(
    value.values.map(item => item.toString()),
    expected,
  )
}

function assertHashValue(value: RedisDataValue | null, expected: string) {
  if (!value || value.type !== 'hash') {
    assert.fail('Expected hash value')
  }

  assert.strictEqual(value.fields.get('field')?.value.toString(), expected)
}
