import { after, before, describe, test } from 'node:test'
import assert from 'node:assert'

import { TestRunner } from '../test-config'
import { activeProfile, commandFrame, randomKey } from '../utils'
import { RawRedisConnection } from '../raw-tcp/raw-connection'

// Double replies are spelled `%.17g` on Redis 6.2 / 7.0 and by `d2string()` /
// `fpconv_dtoa` on Redis 7.2+ and every Valkey (the `reply.double-fpconv`
// gate, #451). Expected bytes below were read off real redis-server 6.2.14,
// 7.0.15, 7.2.4, 7.4.4, 8.0.6 and valkey-server 8.0.0 / 9.0.0.
const testRunner = new TestRunner()
const profile = activeProfile
const usesFpconv = !['redis-6.2', 'redis-7.0'].includes(profile)

describe(
  `double reply formatting by profile (${testRunner.getBackendName()}, ${profile})`,
  { skip: testRunner.backend === 'real' && 'profiles are mock-only' },
  () => {
    let connection: RawRedisConnection

    before(async () => {
      const port = await testRunner.setupRawStandalone()
      connection = await RawRedisConnection.connect('127.0.0.1', port)
    })

    after(async () => {
      connection.close()
      await testRunner.cleanup()
    })

    async function send(...args: string[]): Promise<string> {
      connection.write(commandFrame(...args))
      return (await connection.readRawFrame()).toString()
    }

    function bulk(text: string): string {
      return `$${Buffer.byteLength(text)}\r\n${text}\r\n`
    }

    test('ZSCORE / ZINCRBY / WITHSCORES over RESP2', async () => {
      const key = `compat:${profile}:${randomKey()}:dbl`
      await send('ZADD', key, '0.1', 'tenth', '0.0000123', 'small')
      await send('ZADD', key, '4611686018427387904', 'twoTo62')

      assert.strictEqual(
        await send('ZSCORE', key, 'tenth'),
        bulk(usesFpconv ? '0.1' : '0.10000000000000001'),
      )
      assert.strictEqual(
        await send('ZSCORE', key, 'small'),
        bulk(usesFpconv ? '1.23e-5' : '1.2300000000000001e-05'),
      )
      assert.strictEqual(
        await send('ZSCORE', key, 'twoTo62'),
        bulk(usesFpconv ? '4611686018427387904' : '4.6116860184273879e+18'),
      )
      assert.strictEqual(
        await send('ZINCRBY', key, '0.2', 'tenth'),
        bulk('0.30000000000000004'),
      )
      assert.strictEqual(
        await send('ZINCRBY', key, '1e17', 'big'),
        bulk(usesFpconv ? '100000000000000000' : '1e+17'),
      )

      const small = usesFpconv ? '1.23e-5' : '1.2300000000000001e-05'
      assert.strictEqual(
        await send('ZRANGE', key, '0', '0', 'WITHSCORES'),
        `*2\r\n${bulk('small')}${bulk(small)}`,
      )
      await send('DEL', key)
    })

    test('RESP3 doubles and scores read inside Lua', async () => {
      const key = `compat:${profile}:${randomKey()}:dbl3`
      await send('ZADD', key, '0.1', 'tenth')

      assert.strictEqual(
        await send(
          'EVAL',
          "return redis.call('ZSCORE', KEYS[1], 'tenth')",
          '1',
          key,
        ),
        bulk(usesFpconv ? '0.1' : '0.10000000000000001'),
      )

      await send('HELLO', '3')
      try {
        assert.strictEqual(
          await send('ZSCORE', key, 'tenth'),
          usesFpconv ? ',0.1\r\n' : ',0.10000000000000001\r\n',
        )
      } finally {
        await send('HELLO', '2')
      }
      await send('DEL', key)
    })
  },
)
