import { describe, test } from 'node:test'
import assert from 'node:assert'
import {
  Resp2Server,
  RedisServerState,
  WrongTypeRedisError,
  createRedisCommandExecutor,
  createStringData,
} from '../src/internal'
import type {
  RedisDataValue,
  RedisMonitorCommandEvent,
  RedisMutationEvent,
} from '../src/internal'

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
    db.updateList(key, list =>
      list.pushRight([Buffer.from('a'), Buffer.from('b')]),
    )

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
    server.close()
  })

  test('actively sweeps expired entries without a read', async () => {
    const server = new RedisServerState({ activeExpiryIntervalMs: 5 })
    const db = server.getDatabase(0)
    const key = Buffer.from('active-expiring')
    const events: RedisMutationEvent[] = []
    db.subscribeKey(key, event => events.push(event))

    try {
      db.setString(key, Buffer.from('value'), { expiresAt: Date.now() + 10 })

      await waitUntil(() => events.some(event => event.type === 'evict'))

      assert.strictEqual(db.size(), 0)
    } finally {
      server.close()
    }
  })

  test('active expiry swallows listener failures without an unhandled rejection', async () => {
    const unhandled: unknown[] = []
    const onUnhandled = (reason: unknown) => unhandled.push(reason)
    process.on('unhandledRejection', onUnhandled)

    const server = new RedisServerState({ activeExpiryIntervalMs: 5 })
    const db = server.getDatabase(0)
    const key = Buffer.from('active-expiry-listener-error')
    db.subscribe(event => {
      if (event.type === 'evict') {
        throw new Error('listener boom')
      }
    })

    try {
      db.setString(key, Buffer.from('value'), { expiresAt: Date.now() + 10 })

      await new Promise(resolve => setTimeout(resolve, 50))

      assert.strictEqual(
        unhandled.length,
        0,
        'active expiry must catch sweep failures',
      )
    } finally {
      server.close()
      process.off('unhandledRejection', onUnhandled)
    }
  })

  test('Resp2Server.close closes state even when the TCP server never listened', async () => {
    const state = new RedisServerState({ activeExpiryIntervalMs: 5 })
    const server = new Resp2Server({
      server: state,
      executor: createRedisCommandExecutor(),
    })
    const db = state.getDatabase(0)
    const key = Buffer.from('close-before-listen')
    const events: RedisMutationEvent[] = []
    db.subscribeKey(key, event => events.push(event))

    db.setString(key, Buffer.from('value'), { expiresAt: Date.now() + 10 })

    await assert.rejects(() => server.close(), {
      code: 'ERR_SERVER_NOT_RUNNING',
    })
    await new Promise(resolve => setTimeout(resolve, 50))

    assert.deepStrictEqual(
      events.map(event => event.type),
      ['write'],
      'state timer must be closed even when server.close rejects',
    )
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
      hash.setField(Buffer.from('field'), Buffer.from('value'))
    })

    const firstRead = db.get(key)
    if (!firstRead || firstRead.type !== 'hash') {
      assert.fail('Expected hash value')
    }

    firstRead.fields.get(fieldKey('field'))!.value.write('x')
    assertHashValue(db.get(key), 'value')

    if (!eventValue || eventValue.type !== 'hash') {
      assert.fail('Expected hash event value')
    }

    eventValue.fields.get(fieldKey('field'))!.value.write('y')
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
          list.pushRight([Buffer.from('x')])
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

  assert.strictEqual(
    value.fields.get(fieldKey('field'))?.value.toString(),
    expected,
  )
}

function fieldKey(field: string): string {
  return Buffer.from(field).toString('hex')
}

async function waitUntil(
  predicate: () => boolean,
  timeoutMs = 500,
): Promise<void> {
  const deadline = Date.now() + timeoutMs
  while (Date.now() < deadline) {
    if (predicate()) {
      return
    }
    await new Promise(resolve => setTimeout(resolve, 5))
  }

  assert.fail('Timed out waiting for condition')
}
