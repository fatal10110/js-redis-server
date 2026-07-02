import { after, before, describe, test } from 'node:test'
import assert from 'node:assert'

import { TestRunner } from '../test-config'
import { commandFrame } from '../utils'
import {
  RawRedisConnection,
  respMapGet,
  respNumber,
  type RespWireValue,
} from '../raw-tcp/raw-connection'

type ProfileName =
  | 'redis-6.2'
  | 'redis-7.0'
  | 'redis-7.2'
  | 'redis-7.4'
  | 'redis-8.0'
  | 'valkey-8.0'
  | 'valkey-9.0'

const testRunner = new TestRunner()
const profile = (process.env.REDIS_COMPAT ?? 'redis-8.0') as ProfileName
const expectedVersion: Record<ProfileName, string> = {
  'redis-6.2': '6.2.14',
  'redis-7.0': '7.0.15',
  'redis-7.2': '7.2.4',
  'redis-7.4': '7.4.4',
  'redis-8.0': '8.0.0',
  'valkey-8.0': '8.0.0',
  'valkey-9.0': '9.0.0',
}

const redis62RootCommands = [
  'GETEX',
  'GETDEL',
  'COPY',
  'HRANDFIELD',
  'LMOVE',
  'BLMOVE',
  'RESET',
  'SMISMEMBER',
  'XAUTOCLAIM',
  'ZMSCORE',
]

const redis70RootCommands = [
  'EXPIRETIME',
  'PEXPIRETIME',
  'LMPOP',
  'BLMPOP',
  'SINTERCARD',
  'SSUBSCRIBE',
  'SUNSUBSCRIBE',
  'SPUBLISH',
  'SORT_RO',
  'ZINTERCARD',
  'ZMPOP',
  'BZMPOP',
]

const hashFieldExpirationCommands = [
  'HEXPIRE',
  'HPEXPIRE',
  'HEXPIREAT',
  'HPEXPIREAT',
  'HPERSIST',
  'HTTL',
  'HPTTL',
]

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
      if (profile.startsWith('valkey-')) {
        assert.match(info, /\r\nserver_name:valkey\r\n/)
        assert.match(
          info,
          new RegExp(
            `\\r\\nvalkey_version:${escapeRegExp(expectedVersion[profile])}\\r\\n`,
          ),
        )
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

    test('applies root command availability gates', async () => {
      for (const command of redis62RootCommands) {
        await expectRootCommand(command, true)
      }

      for (const command of redis70RootCommands) {
        await expectRootCommand(command, supportsRedis70Commands())
      }

      for (const command of hashFieldExpirationCommands) {
        await expectRootCommand(command, supportsHashFieldExpiration())
      }

      await expectRootCommand('HGETEX', supportsHgetex())
      await expectRootCommand('HGETDEL', supportsHgetdel())
      await expectRootCommand('HSETEX', supportsHsetex())
    })

    test('applies parser, subcommand, and behavior gates', async () => {
      const key = `compat:${profile}:key`
      const hash = `compat:${profile}:hash`
      const stream = `compat:${profile}:stream`

      await send('SET', key, 'v')
      await expectGate(supportsExpireConditions(), 'EXPIRE', key, '10', 'NX')
      await expectGate(supportsRedis70Commands(), 'INFO', 'server', 'clients')
      await expectGate(true, 'SET', key, 'next', 'GET')
      await expectGate(supportsSetNxGet(), 'SET', key, 'guarded', 'NX', 'GET')
      await expectGate(true, 'SET', key, 'expires', 'EXAT', '4102444800')
      await expectGate(true, 'SLOWLOG', 'GET', '-1')

      // BITCOUNT/BITPOS BYTE|BIT range modifier is Redis 7.0+ (Valkey 7.2+).
      await expectGate(
        supportsBitByteBitRange(),
        'BITCOUNT',
        key,
        '0',
        '0',
        'BYTE',
      )
      await expectGate(
        supportsBitByteBitRange(),
        'BITPOS',
        key,
        '1',
        '0',
        '-1',
        'BIT',
      )

      await expectGate(supportsCommandDocs(), 'COMMAND', 'DOCS')
      await expectGate(
        supportsCommandDocs(),
        'COMMAND',
        'GETKEYSANDFLAGS',
        'GET',
        key,
      )
      await expectGate(
        supportsClientSetinfo(),
        'CLIENT',
        'SETINFO',
        'lib-name',
        'compat',
      )
      await expectGate(
        supportsClientKillMaxAge(),
        'CLIENT',
        'KILL',
        'MAXAGE',
        '999999',
      )
      await expectGate(supportsRedis70Commands(), 'CLIENT', 'NO-EVICT', 'ON')
      await expectGate(supportsShardedPubSub(), 'PUBSUB', 'SHARDCHANNELS')
      await expectGate(supportsShardedPubSub(), 'PUBSUB', 'SHARDNUMSUB')
      await expectGate(
        supportsShardedPubSub(),
        'SPUBLISH',
        `compat:{${profile}}`,
        'message',
      )
      await expectGate(
        supportsRedis70Commands(),
        'ACL',
        'DRYRUN',
        'default',
        'PING',
      )
      await expectGate(supportsRedis70Commands(), 'EVAL_RO', 'return 1', '0')
      if (supportsRedis70Commands()) {
        await expectGate(
          true,
          'FUNCTION',
          'LOAD',
          '#!lua name=compatlib\nredis.register_function{function_name="compat_echo", callback=function(keys, args) return args[1] end, flags={"no-writes"}}',
        )
        await expectGate(true, 'FCALL', 'compat_echo', '0', 'hello')
        await expectGate(true, 'FCALL_RO', 'compat_echo', '0', 'hello')
      } else {
        await expectGate(false, 'FUNCTION', 'HELP')
        await expectGate(false, 'FCALL', 'missing', '0')
      }
      await expectGate(
        supportsZintercard(),
        'ZINTERCARD',
        '1',
        `compat:${profile}:zset`,
      )

      await send('XADD', stream, '1-1', 'field', 'value')
      await expectGate(supportsXreadPlusId(), 'XREAD', 'STREAMS', stream, '+')

      await send('HSET', hash, 'field', 'value', 'delete-me', 'gone')
      await expectGate(supportsHscanNoValues(), 'HSCAN', hash, '0', 'NOVALUES')
      await expectGate(
        supportsHashFieldExpiration(),
        'HEXPIRE',
        hash,
        '10',
        'FIELDS',
        '1',
        'field',
      )
      await expectGate(
        supportsHashFieldExpiration(),
        'HTTL',
        hash,
        'FIELDS',
        '1',
        'field',
      )
      await expectGate(supportsHgetex(), 'HGETEX', hash, 'FIELDS', '1', 'field')
      await expectGate(
        supportsHsetex(),
        'HSETEX',
        hash,
        'FIELDS',
        '1',
        'field',
        'updated',
      )
      await expectGate(
        supportsHgetdel(),
        'HGETDEL',
        hash,
        'FIELDS',
        '1',
        'delete-me',
      )
    })

    test('writing a global is rejected by the readonly table', async () => {
      // The Lua engine blocks global writes via Lua's native readonly table, so
      // the wording is version-invariant across profiles.
      const reply = await send('EVAL', 'x = 5', '0')
      assert.ok(reply.startsWith('-'), `expected an error, got ${reply}`)
      assert.match(reply, /Attempt to modify a readonly table/)
    })

    test('Lua sandbox globals (print / os) match the profile', async () => {
      // print: only redis-6.2 still exposes it (returns nil, not an error).
      const printReply = await send('EVAL', "print('x')", '0')
      if (profile === 'redis-6.2') {
        assert.ok(
          !printReply.startsWith('-'),
          `print should be available on ${profile}, got ${printReply}`,
        )
      } else {
        assert.match(printReply, /nonexistent global variable 'print'/)
      }

      // os: exposed only from redis-7.4 / valkey-8.0 onward.
      const osReply = await send('EVAL', 'return type(os)', '0')
      if (supportsLuaOsLib()) {
        assert.match(osReply, /table/)
      } else {
        assert.match(osReply, /nonexistent global variable 'os'/)
      }
    })

    test('RESP3 subscribed PUBLISH self-reply order matches the profile', async () => {
      const channel = `compat:${profile}:self-publish`

      connection.write(commandFrame('HELLO', '3'))
      const hello = await connection.readFrame()
      assert.ok(hello instanceof Map)
      assert.strictEqual(respNumber(respMapGet(hello, 'proto')), 3)

      connection.write(commandFrame('SUBSCRIBE', channel))
      assert.deepStrictEqual(normalizeFrame(await connection.readFrame()), [
        'subscribe',
        channel,
        1,
      ])

      connection.write(commandFrame('PUBLISH', channel, 'self'))
      const first = await connection.readFrame()
      const second = await connection.readFrame()
      const message = ['message', channel, 'self']

      if (supportsResp3PublishReplyBeforeSelfMessage()) {
        assert.strictEqual(first, 1)
        assert.deepStrictEqual(normalizeFrame(second), message)
      } else {
        assert.deepStrictEqual(normalizeFrame(first), message)
        assert.strictEqual(second, 1)
      }

      connection.write(commandFrame('UNSUBSCRIBE', channel))
      assert.deepStrictEqual(normalizeFrame(await connection.readFrame()), [
        'unsubscribe',
        channel,
        0,
      ])

      connection.write(commandFrame('HELLO', '2'))
      await connection.readFrame()
    })

    test('XREADGROUP and XAUTOCLAIM create the consumer entry even when nothing is delivered or claimed', async () => {
      const key = `compat:${profile}:consumer-create-on-empty`

      connection.write(commandFrame('XADD', key, '1-1', 'f', 'v'))
      await connection.readFrame()
      connection.write(commandFrame('XGROUP', 'CREATE', key, 'g', '0'))
      await connection.readFrame()
      connection.write(
        commandFrame(
          'XREADGROUP',
          'GROUP',
          'g',
          'alice',
          'COUNT',
          '10',
          'STREAMS',
          key,
          '>',
        ),
      )
      await connection.readFrame()

      // bob delivers nothing (alice already consumed the only entry) but must
      // still be created as a consumer: ensureConsumer() runs unconditionally
      // before the empty-delivery check.
      connection.write(
        commandFrame(
          'XREADGROUP',
          'GROUP',
          'g',
          'bob',
          'COUNT',
          '10',
          'STREAMS',
          key,
          '>',
        ),
      )
      assert.strictEqual(await connection.readFrame(), null)

      const bob = await findConsumer(key, 'g', 'bob')
      assert.ok(
        bob,
        `bob should be created as a consumer for ${profile} despite an empty XREADGROUP delivery`,
      )
      assert.strictEqual(bob.pending, 0)

      // carol's min-idle-time (999999999ms) is never met, so nothing is
      // actually claimed, but ensureConsumer() still runs first.
      connection.write(
        commandFrame(
          'XAUTOCLAIM',
          key,
          'g',
          'carol',
          '999999999',
          '0',
          'COUNT',
          '10',
        ),
      )
      const autoclaim = normalizeFrame(
        await connection.readFrame(),
      ) as RespWireValue[]
      assert.deepStrictEqual(autoclaim[1], [])

      const carol = await findConsumer(key, 'g', 'carol')
      assert.ok(
        carol,
        `carol should be created as a consumer for ${profile} despite an empty XAUTOCLAIM claim`,
      )
      assert.strictEqual(carol.pending, 0)
    })

    test('XINFO CONSUMERS reports idle and inactive fields', async () => {
      const key = `compat:${profile}:consumers-idle-inactive`

      connection.write(commandFrame('XADD', key, '1-1', 'f', 'v'))
      await connection.readFrame()
      connection.write(commandFrame('XGROUP', 'CREATE', key, 'g', '0'))
      await connection.readFrame()
      connection.write(
        commandFrame(
          'XREADGROUP',
          'GROUP',
          'g',
          'alice',
          'COUNT',
          '10',
          'STREAMS',
          key,
          '>',
        ),
      )
      await connection.readFrame()

      const alice = await findConsumer(key, 'g', 'alice')
      assert.ok(alice)
      assert.strictEqual(typeof alice.idle, 'number')
      assert.ok((alice.idle as number) >= 0)
      assert.strictEqual(typeof alice.inactive, 'number')
      assert.ok((alice.inactive as number) >= 0)
    })

    async function findConsumer(
      key: string,
      group: string,
      name: string,
    ): Promise<Record<string, RespWireValue> | undefined> {
      connection.write(commandFrame('XINFO', 'CONSUMERS', key, group))
      const reply = normalizeFrame(await connection.readFrame())
      assert.ok(Array.isArray(reply))
      for (const entry of reply) {
        assert.ok(Array.isArray(entry))
        const record = flatToRecord(entry)
        if (record.name === name) return record
      }
      return undefined
    }

    function flatToRecord(
      flat: RespWireValue[],
    ): Record<string, RespWireValue> {
      const record: Record<string, RespWireValue> = {}
      for (let i = 0; i < flat.length; i += 2) {
        record[String(flat[i])] = flat[i + 1]
      }
      return record
    }

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

    async function expectRootCommand(
      command: string,
      available: boolean,
    ): Promise<void> {
      const reply = await send(command)
      if (available) {
        assert.doesNotMatch(
          reply,
          /unknown command/i,
          `${command} should be registered for ${profile}, got ${JSON.stringify(reply)}`,
        )
        return
      }

      assert.match(
        reply,
        /unknown command/i,
        `${command} should be absent for ${profile}, got ${JSON.stringify(reply)}`,
      )
    }
  },
)

function supportsLuaOsLib(): boolean {
  // The sandboxed Lua `os` library is exposed from Redis 7.4 / Valkey 8.0 on.
  return ['redis-7.4', 'redis-8.0', 'valkey-8.0', 'valkey-9.0'].includes(
    profile,
  )
}

function supportsResp3PublishReplyBeforeSelfMessage(): boolean {
  return !['redis-6.2', 'redis-7.0'].includes(profile)
}

function supportsExpireConditions(): boolean {
  return profile !== 'redis-6.2'
}

function supportsRedis70Commands(): boolean {
  return profile !== 'redis-6.2'
}

function supportsSetNxGet(): boolean {
  return profile !== 'redis-6.2'
}

function supportsCommandDocs(): boolean {
  return profile !== 'redis-6.2'
}

function supportsClientSetinfo(): boolean {
  return !['redis-6.2', 'redis-7.0'].includes(profile)
}

function supportsClientKillMaxAge(): boolean {
  return ['redis-7.4', 'redis-8.0', 'valkey-9.0'].includes(profile)
}

function supportsShardedPubSub(): boolean {
  return profile !== 'redis-6.2'
}

function supportsZintercard(): boolean {
  return profile !== 'redis-6.2'
}

function supportsBitByteBitRange(): boolean {
  return profile !== 'redis-6.2'
}

function supportsHashFieldExpiration(): boolean {
  return ['redis-7.4', 'redis-8.0', 'valkey-9.0'].includes(profile)
}

function supportsHscanNoValues(): boolean {
  return ['redis-7.4', 'redis-8.0', 'valkey-9.0'].includes(profile)
}

function supportsXreadPlusId(): boolean {
  return ['redis-7.4', 'redis-8.0'].includes(profile)
}

function supportsHgetex(): boolean {
  return ['redis-8.0', 'valkey-9.0'].includes(profile)
}

function supportsHgetdel(): boolean {
  return profile === 'redis-8.0'
}

function supportsHsetex(): boolean {
  return ['redis-8.0', 'valkey-9.0'].includes(profile)
}

function bulkStringFrame(value: string): RegExp {
  return new RegExp(
    `\\$${Buffer.byteLength(value)}\\r\\n${escapeRegExp(value)}\\r\\n`,
  )
}

function escapeRegExp(value: string): string {
  return value.replace(/[.*+?^${}()|[\]\\]/g, '\\$&')
}

function normalizeFrame(value: RespWireValue): RespWireValue {
  if (Buffer.isBuffer(value)) {
    return value.toString()
  }

  if (Array.isArray(value)) {
    return value.map(normalizeFrame)
  }

  return value
}
