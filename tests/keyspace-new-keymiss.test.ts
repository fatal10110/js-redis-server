import { describe, test } from 'node:test'
import assert from 'node:assert'
import { RedisDatabase } from '../src/state/database'
import type { RedisMutationEvent } from '../src/state/mutation-events'
import {
  KeyspaceNotifier,
  parseKeyspaceNotifyFlags,
} from '../src/state/keyspace-notifier'
import type { RedisPubSubBroker } from '../src/state/pubsub-broker'

function record(db: RedisDatabase): string[] {
  const events: string[] = []
  db.subscribe((event: RedisMutationEvent) => {
    if ('key' in event) events.push(`${event.type} ${event.key.toString()}`)
  })
  return events
}

describe('new and keymiss mutation events (#514)', () => {
  test('a key is new when added, not when overwritten', () => {
    const db = new RedisDatabase(0)
    const events = record(db)
    db.setString(Buffer.from('k'), Buffer.from('v'))
    db.setString(Buffer.from('k'), Buffer.from('w'))
    db.setString(Buffer.from('k'), Buffer.from('x'), { announceNew: true })
    db.setString(Buffer.from('j'), Buffer.from('v'), { announceNew: false })
    assert.deepStrictEqual(events, [
      'new k',
      'write k',
      'write k',
      'new k',
      'write k',
      'write j',
    ])
  })

  test('a collection is new when created, and again after it emptied', () => {
    const db = new RedisDatabase(0)
    const events = record(db)
    const key = Buffer.from('list')
    db.updateList(key, list => list.pushRight([Buffer.from('a')]))
    db.updateList(key, list => list.pushRight([Buffer.from('b')]))
    db.updateList(key, list => list.popMany('left', 2))
    db.updateList(key, list => list.pushRight([Buffer.from('c')]))
    assert.deepStrictEqual(events, [
      'new list',
      'write list',
      'write list',
      'notify list',
      'delete list',
      'new list',
      'write list',
    ])
  })

  test('new and keymiss never reach per-key listeners', () => {
    const db = new RedisDatabase(0)
    const key = Buffer.from('k')
    const keyEvents: string[] = []
    db.subscribeKey(key, event => keyEvents.push(event.type))
    db.announceKeyMiss(key)
    db.setString(key, Buffer.from('v'))
    assert.deepStrictEqual(keyEvents, ['write'])
  })

  test('the notifier publishes new under n and keymiss under m', () => {
    const published: string[] = []
    const broker = {
      publish: (channel: Buffer, message: Buffer) => {
        published.push(`${channel.toString()} ${message.toString()}`)
        return 0
      },
    } as unknown as RedisPubSubBroker
    const notifier = new KeyspaceNotifier(broker)
    const key = Buffer.from('k')
    const events: RedisMutationEvent[] = [
      { type: 'keymiss', database: 0, key },
      { type: 'new', database: 0, key },
    ]
    for (const flags of ['KE', 'KEm', 'En']) {
      for (const event of events) {
        notifier.handle(event, parseKeyspaceNotifyFlags(flags)!)
      }
    }
    assert.deepStrictEqual(published, [
      '__keyspace@0__:k keymiss',
      '__keyevent@0__:keymiss k',
      '__keyevent@0__:new k',
    ])
  })
})
