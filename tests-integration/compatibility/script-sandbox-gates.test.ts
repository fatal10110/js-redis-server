import { after, before, describe, test } from 'node:test'
import assert from 'node:assert'
import { createHash } from 'node:crypto'

import { TestRunner } from '../test-config'
import { activeProfile, commandFrame, type ProfileName } from '../utils'
import { RawRedisConnection } from '../raw-tcp/raw-connection'

/**
 * What a script sees of the `redis` table, and how its own errors come back,
 * per profile (#502). Byte for byte against redis-server 6.2.24, 7.0.15,
 * 7.2.16, 7.4.11, 8.0.6 and valkey-server 8.0.11, 9.0.6; the version members
 * report this server's profile version, which is where the real patch
 * releases differ.
 */
const testRunner = new TestRunner()
const profile = activeProfile
const legacy = profile === 'redis-6.2'
const valkey = profile.startsWith('valkey-')

const profileVersion: Record<ProfileName, string> = {
  'redis-6.2': '6.2.14',
  'redis-7.0': '7.0.15',
  'redis-7.2': '7.2.4',
  'redis-7.4': '7.4.4',
  'redis-8.0': '8.0.0',
  'valkey-8.0': '8.0.0',
  'valkey-9.0': '9.0.0',
}

const sha1 = (script: string) => createHash('sha1').update(script).digest('hex')

/** A bulk string reply frame. */
const bulk = (value: string) => `$${Buffer.byteLength(value)}\r\n${value}\r\n`

/** The reply a one-line script aborting with `error` gets on this profile. */
function aborted(script: string, error: string): string {
  return legacy
    ? `-ERR Error running script (call to f_${sha1(script)}): @user_script:1: ${error}\r\n`
    : `-${error} script: ${sha1(script)}, on @user_script:1.\r\n`
}

describe(
  `script sandbox per profile (${testRunner.getBackendName()}, ${profile})`,
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
      return (await connection.readRawFrame()).toString('binary')
    }

    const evalRaw = (script: string) => send('EVAL', script, '0')

    test('redis.REPL_* and the replication stubs exist on every version', async () => {
      assert.strictEqual(
        await evalRaw(
          'return {redis.REPL_NONE, redis.REPL_AOF, redis.REPL_SLAVE, redis.REPL_REPLICA, redis.REPL_ALL}',
        ),
        '*5\r\n:0\r\n:1\r\n:2\r\n:2\r\n:3\r\n',
      )
      // set_repl returns nothing; replicate_commands returns true (`:1`).
      assert.strictEqual(
        await evalRaw('return {redis.set_repl(redis.REPL_ALL)}'),
        '*0\r\n',
      )
      assert.strictEqual(
        await evalRaw('return redis.replicate_commands()'),
        ':1\r\n',
      )
      // Outside a SCRIPT DEBUG session the debugger hooks do nothing.
      assert.strictEqual(
        await evalRaw('return tostring(redis.breakpoint())'),
        bulk('false'),
      )
      assert.strictEqual(await evalRaw("return {redis.debug('x')}"), '*0\r\n')
    })

    test('redis.REDIS_VERSION from 7.0, VALKEY_VERSION on Valkey', async () => {
      const reply = await evalRaw(
        'return {type(redis.REDIS_VERSION), type(redis.REDIS_VERSION_NUM), type(redis.SERVER_NAME), type(redis.VALKEY_VERSION), type(redis.VALKEY_VERSION_NUM)}',
      )
      const types = legacy
        ? ['nil', 'nil', 'nil', 'nil', 'nil']
        : valkey
          ? ['string', 'number', 'string', 'string', 'number']
          : ['string', 'number', 'nil', 'nil', 'nil']
      assert.strictEqual(reply, `*5\r\n${types.map(bulk).join('')}`)
      if (legacy) {
        return
      }

      const versionNum = (version: string) => {
        const [major, minor, patch] = version.split('.').map(Number)
        return major * 65536 + minor * 256 + patch
      }
      // Valkey reports the Redis version it forked from, as its INFO does.
      const redisVersion = valkey ? '7.2.4' : profileVersion[profile]
      assert.strictEqual(
        await evalRaw('return {redis.REDIS_VERSION, redis.REDIS_VERSION_NUM}'),
        `*2\r\n${bulk(redisVersion)}:${versionNum(redisVersion)}\r\n`,
      )
      if (valkey) {
        assert.strictEqual(
          await evalRaw(
            'return {redis.SERVER_NAME, redis.VALKEY_VERSION, redis.VALKEY_VERSION_NUM}',
          ),
          `*3\r\n${bulk('valkey')}${bulk(profileVersion[profile])}:${versionNum(profileVersion[profile])}\r\n`,
        )
      }
    })

    test('a compile error has no abort decoration on any version', async () => {
      const compile =
        "-ERR Error compiling script (new function): user_script:1: unexpected symbol near '+'\r\n"
      assert.strictEqual(await evalRaw('return +'), compile)
      assert.strictEqual(await send('SCRIPT', 'LOAD', 'return +'), compile)
    })

    test('EVALSHA of an uncached script: Valkey 8.0+ drops "Please use EVAL."', async () => {
      assert.strictEqual(
        await send('EVALSHA', 'ffffffffffffffffffffffffffffffffffffffff', '0'),
        valkey
          ? '-NOSCRIPT No matching script.\r\n'
          : '-NOSCRIPT No matching script. Please use EVAL.\r\n',
      )
    })

    test('redis.error_reply keeps a leading dash from 7.0 and its text on 6.2', async () => {
      assert.strictEqual(
        await evalRaw("return redis.error_reply('-MY x')"),
        legacy ? '--MY x\r\n' : '-MY x\r\n',
      )
      assert.strictEqual(
        await evalRaw("return redis.error_reply('x')"),
        legacy ? '-x\r\n' : '-ERR x\r\n',
      )
    })

    test('error() with a string keeps its whole text, a leading ERR included', async () => {
      const script = "error('ERR x', 0)"
      assert.strictEqual(
        await evalRaw(script),
        aborted(script, legacy ? 'ERR x' : 'ERR ERR x'),
      )
    })

    test('redis.sha1hex checks its arity', async () => {
      for (const script of [
        'return redis.sha1hex()',
        "return redis.sha1hex('a', 'b')",
      ]) {
        assert.strictEqual(
          await evalRaw(script),
          aborted(
            script,
            legacy
              ? 'wrong number of arguments'
              : 'ERR wrong number of arguments',
          ),
          script,
        )
      }
    })

    test(
      'error() with no value, nil or a table from 7.0',
      { skip: legacy && '6.2 is pinned below' },
      async () => {
        const cases: Array<[string, string]> = [
          ['error()', 'ERR nil'],
          ['error(nil)', 'ERR nil'],
          ["error({err='WRONGTYPE x'})", 'WRONGTYPE x'],
          ["error({err='boom'})", 'boom'],
        ]
        for (const [script, error] of cases) {
          assert.strictEqual(
            await evalRaw(script),
            aborted(script, error),
            script,
          )
        }
      },
    )

    // KNOWN GAP: 6.2's error handler concatenates the error value, so a
    // non-string one makes the handler itself fail, at its own position. The
    // engine's redis-6.2 model reports the value instead (`@user_script:1:
    // nil`, `... WRONGTYPE x`).
    test(
      'error() with no value, nil or a table on 6.2',
      {
        skip: !legacy && '6.2 only',
        todo: 'lua-redis-wasm redis-6.2 model: error handler failure',
      },
      async () => {
        const cases: Array<[string, string]> = [
          ['error()', 'nil'],
          ['error(nil)', 'nil'],
          ["error({err='WRONGTYPE x'})", 'table'],
        ]
        for (const [script, type] of cases) {
          assert.strictEqual(
            await evalRaw(script),
            `-ERR Error running script (call to f_${sha1(script)}): @err_handler_def:9: err_handler_def:9: attempt to concatenate local 'err' (a ${type} value)\r\n`,
            script,
          )
        }
      },
    )
  },
)
