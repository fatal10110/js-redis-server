import { after, before, describe, test } from 'node:test'
import assert from 'node:assert'

import { TestRunner } from '../test-config'
import { activeProfile, commandFrame, randomKey } from '../utils'
import {
  RawRedisConnection,
  respText,
  type RespWireValue,
} from '../raw-tcp/raw-connection'

// A deadline already past (#527): Valkey 8.0 makes SET delete an existing key
// and never create one; Valkey 8.1+ publishes every such deletion (SET,
// GETEX, the EXPIRE family) as `expired` rather than `del`. Redis 6.2-8.0 and
// Valkey 7.2 write the key with SET and let it expire on the next access.

const testRunner = new TestRunner()
const profile = activeProfile
const setDeletes = profile === 'valkey-8.0' || profile === 'valkey-9.0'
const deletion = profile === 'valkey-9.0' ? 'expired' : 'del'

describe(
  `past-deadline expiry (${testRunner.getBackendName()}, ${profile})`,
  { skip: testRunner.backend === 'real' && 'profiles are mock-only' },
  () => {
    let actor: RawRedisConnection
    let subscriber: RawRedisConnection

    before(async () => {
      const port = await testRunner.setupRawStandalone()
      actor = await RawRedisConnection.connect('127.0.0.1', port)
      subscriber = await RawRedisConnection.connect('127.0.0.1', port)
    })

    after(async () => {
      actor.close()
      subscriber.close()
      await testRunner.cleanup()
    })

    async function send(...args: string[]): Promise<string> {
      actor.write(commandFrame(...args))
      return (await actor.readRawFrame()).toString()
    }

    test('SET / EXPIREAT / PEXPIRE / GETEX publish what the profile does', async () => {
      const run = randomKey()
      const key = (name: string) => `${name}:${run}`
      const sentinel = `__keyevent@0__:sentinel-${run}`

      subscriber.write(commandFrame('PSUBSCRIBE', '__keyevent@0__:*'))
      await subscriber.readFrame()
      assert.strictEqual(
        await send('CONFIG', 'SET', 'notify-keyspace-events', 'KEA'),
        '+OK\r\n',
      )
      try {
        assert.strictEqual(
          await send('SET', key('absent'), 'v', 'EXAT', '1'),
          '+OK\r\n',
        )
        assert.strictEqual(await send('EXISTS', key('absent')), ':0\r\n')

        await send('SET', key('set'), 'v')
        assert.strictEqual(
          await send('SET', key('set'), 'w', 'PXAT', '1', 'GET'),
          '$1\r\nv\r\n',
        )
        assert.strictEqual(await send('EXISTS', key('set')), ':0\r\n')

        await send('SET', key('expireat'), 'v')
        assert.strictEqual(
          await send('EXPIREAT', key('expireat'), '1'),
          ':1\r\n',
        )
        await send('SET', key('pexpire'), 'v')
        assert.strictEqual(
          await send('PEXPIRE', key('pexpire'), '-5'),
          ':1\r\n',
        )
        await send('SET', key('getex'), 'v')
        assert.strictEqual(
          await send('GETEX', key('getex'), 'EXAT', '1'),
          '$1\r\nv\r\n',
        )
        assert.strictEqual(await send('EXISTS', key('getex')), ':0\r\n')

        await send('PUBLISH', sentinel, 'done')
        const events: string[] = []
        for (;;) {
          const frame = (await subscriber.readFrame()) as RespWireValue[]
          const channel = respText(frame[2])
          if (channel === sentinel) break
          const name = channel.slice('__keyevent@0__:'.length)
          events.push(`${name} ${respText(frame[3]).replace(`:${run}`, '')}`)
        }

        const setEvents = setDeletes
          ? ['set set', `${deletion} set`]
          : [
              'set absent',
              'expire absent',
              'expired absent',
              'set set',
              'set set',
              'expire set',
              'expired set',
            ]
        assert.deepStrictEqual(events, [
          ...setEvents,
          'set expireat',
          `${deletion} expireat`,
          'set pexpire',
          `${deletion} pexpire`,
          'set getex',
          `${deletion} getex`,
        ])
      } finally {
        await send('CONFIG', 'SET', 'notify-keyspace-events', '')
      }
    })
  },
)
