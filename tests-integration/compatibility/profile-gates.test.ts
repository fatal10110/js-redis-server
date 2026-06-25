import { after, before, describe, test } from 'node:test'
import assert from 'node:assert'

import { TestRunner } from '../test-config'
import { commandFrame } from '../utils'
import { RawRedisConnection } from '../raw-tcp/raw-connection'

type ProfileName =
  | 'redis-6.2'
  | 'redis-7.0'
  | 'redis-7.2'
  | 'redis-7.4'
  | 'redis-8.0'
  | 'valkey-9.0'

const testRunner = new TestRunner()
const profile = (process.env.REDIS_COMPAT ?? 'redis-8.0') as ProfileName
const expectedVersion: Record<ProfileName, string> = {
  'redis-6.2': '6.2.14',
  'redis-7.0': '7.0.15',
  'redis-7.2': '7.2.4',
  'redis-7.4': '7.4.4',
  'redis-8.0': '8.0.0',
  'valkey-9.0': '9.0.0',
}

describe(
  `compatibility profile integration (${testRunner.getBackendName()}, ${profile})`,
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

    test('reports the selected profile over INFO and HELLO', async () => {
      const info = await send('INFO', 'server')
      if (profile === 'valkey-9.0') {
        assert.match(info, /\r\nserver_name:valkey\r\n/)
        assert.match(info, /\r\nvalkey_version:9\.0\.0\r\n/)
      } else {
        assert.match(
          info,
          new RegExp(
            `\\r\\nredis_version:${escapeRegExp(expectedVersion[profile])}\\r\\n`,
          ),
        )
      }

      const hello = await send('HELLO', '2')
      assert.match(hello, bulkStringFrame(expectedVersion[profile]))
    })

    test('applies representative command and subcommand gates', async () => {
      const key = `compat:${profile}:key`
      const hash = `compat:${profile}:hash`

      await send('SET', key, 'v')
      await expectGate(supportsExpireConditions(), 'EXPIRE', key, '10', 'NX')

      await expectGate(supportsCommandDocs(), 'COMMAND', 'DOCS')
      await expectGate(
        supportsClientSetinfo(),
        'CLIENT',
        'SETINFO',
        'lib-name',
        'compat',
      )
      await expectGate(supportsShardedPubSub(), 'PUBSUB', 'SHARDCHANNELS')
      await expectGate(
        supportsShardedPubSub(),
        'SPUBLISH',
        `compat:{${profile}}`,
        'message',
      )
      await expectGate(
        supportsZintercard(),
        'ZINTERCARD',
        '1',
        `compat:${profile}:zset`,
      )

      await send('HSET', hash, 'field', 'value', 'delete-me', 'gone')
      await expectGate(
        supportsHashFieldExpiration(),
        'HEXPIRE',
        hash,
        '10',
        'FIELDS',
        '1',
        'field',
      )
      await expectGate(supportsHgetex(), 'HGETEX', hash, 'FIELDS', '1', 'field')
      await expectGate(
        supportsHgetdel(),
        'HGETDEL',
        hash,
        'FIELDS',
        '1',
        'delete-me',
      )
    })

    async function send(...args: string[]): Promise<string> {
      connection.write(commandFrame(...args))
      return (await connection.readRawFrame()).toString()
    }

    async function expectGate(
      available: boolean,
      ...args: string[]
    ): Promise<void> {
      const reply = await send(...args)
      if (available) {
        assert.ok(
          !reply.startsWith('-'),
          `${args.join(' ')} should be available for ${profile}, got ${JSON.stringify(reply)}`,
        )
        return
      }

      assert.ok(
        reply.startsWith('-'),
        `${args.join(' ')} should be gated for ${profile}, got ${JSON.stringify(reply)}`,
      )
    }
  },
)

function supportsExpireConditions(): boolean {
  return profile !== 'redis-6.2'
}

function supportsCommandDocs(): boolean {
  return profile !== 'redis-6.2'
}

function supportsClientSetinfo(): boolean {
  return !['redis-6.2', 'redis-7.0'].includes(profile)
}

function supportsShardedPubSub(): boolean {
  return profile !== 'redis-6.2'
}

function supportsZintercard(): boolean {
  return profile !== 'redis-6.2'
}

function supportsHashFieldExpiration(): boolean {
  return ['redis-7.4', 'redis-8.0', 'valkey-9.0'].includes(profile)
}

function supportsHgetex(): boolean {
  return ['redis-8.0', 'valkey-9.0'].includes(profile)
}

function supportsHgetdel(): boolean {
  return profile === 'redis-8.0'
}

function bulkStringFrame(value: string): RegExp {
  return new RegExp(
    `\\$${Buffer.byteLength(value)}\\r\\n${escapeRegExp(value)}\\r\\n`,
  )
}

function escapeRegExp(value: string): string {
  return value.replace(/[.*+?^${}()|[\]\\]/g, '\\$&')
}
