import { after, before, describe, test } from 'node:test'
import assert from 'node:assert'

import { TestRunner } from '../test-config'
import { activeProfile, commandFrame, randomKey } from '../utils'
import { RawRedisConnection } from './raw-connection'

/**
 * Set replies by protocol (#501). Real Redis answers SMEMBERS, SINTER,
 * SUNION, SDIFF and SPOP with a count through `addReplySetLen`
 * (redis@8.0.0:src/t_set.c), and FUNCTION LIST a function's flags through
 * `addReplySetLen` of status strings (src/functions.c): a RESP3 set (`~`) for
 * a HELLO 3 client, a plain array (`*`) on RESP2. SRANDMEMBER, SSCAN and
 * SMISMEMBER stay arrays on both. Same bytes on every profile, redis 6.2
 * through valkey 9.0.
 *
 * Single-member or integer sets keep the member order deterministic.
 */
const testRunner = new TestRunner()
const legacy = activeProfile === 'redis-6.2'

describe(`set replies by protocol (${testRunner.getBackendName()})`, () => {
  let resp3: RawRedisConnection
  let resp2: RawRedisConnection
  const p = `{${randomKey()}}`
  const one = `${p}:one` // {a}
  const ints = `${p}:ints` // {1,2}
  const other = `${p}:other` // {2,3}
  const str = `${p}:str`
  const missing = `${p}:missing`

  async function send(
    conn: RawRedisConnection,
    ...args: string[]
  ): Promise<string> {
    conn.write(commandFrame(...args))
    return (await conn.readRawFrame()).toString('binary')
  }

  before(async () => {
    const port = await testRunner.setupRawStandalone()
    resp3 = await RawRedisConnection.connect('127.0.0.1', port)
    resp2 = await RawRedisConnection.connect('127.0.0.1', port)
    await send(resp3, 'HELLO', '3')
    await send(resp2, 'SADD', one, 'a')
    await send(resp2, 'SADD', ints, '1', '2')
    await send(resp2, 'SADD', other, '2', '3')
    await send(resp2, 'SET', str, 'v')
  })

  after(async () => {
    resp3.close()
    resp2.close()
    await testRunner.cleanup()
  })

  test('SMEMBERS is a set on RESP3, an array on RESP2', async () => {
    assert.strictEqual(await send(resp3, 'SMEMBERS', one), '~1\r\n$1\r\na\r\n')
    assert.strictEqual(
      await send(resp3, 'SMEMBERS', ints),
      '~2\r\n$1\r\n1\r\n$1\r\n2\r\n',
    )
    assert.strictEqual(await send(resp3, 'SMEMBERS', missing), '~0\r\n')
    assert.strictEqual(await send(resp2, 'SMEMBERS', one), '*1\r\n$1\r\na\r\n')
    assert.strictEqual(await send(resp2, 'SMEMBERS', missing), '*0\r\n')
    assert.strictEqual(
      await send(resp3, 'SMEMBERS', str),
      '-WRONGTYPE Operation against a key holding the wrong kind of value\r\n',
    )
  })

  test('SINTER, SUNION and SDIFF are sets on RESP3', async () => {
    assert.strictEqual(
      await send(resp3, 'SINTER', ints, other),
      '~1\r\n$1\r\n2\r\n',
    )
    assert.strictEqual(await send(resp3, 'SINTER', ints, missing), '~0\r\n')
    assert.strictEqual(await send(resp3, 'SINTER', one), '~1\r\n$1\r\na\r\n')
    assert.strictEqual(
      await send(resp3, 'SUNION', one, missing),
      '~1\r\n$1\r\na\r\n',
    )
    assert.strictEqual(await send(resp3, 'SUNION', missing), '~0\r\n')
    assert.strictEqual(
      await send(resp3, 'SDIFF', ints, other),
      '~1\r\n$1\r\n1\r\n',
    )
    assert.strictEqual(await send(resp3, 'SDIFF', missing, one), '~0\r\n')
    assert.strictEqual(await send(resp3, 'SDIFF', one, one), '~0\r\n')
    assert.strictEqual(
      await send(resp2, 'SINTER', ints, other),
      '*1\r\n$1\r\n2\r\n',
    )
    assert.strictEqual(await send(resp2, 'SDIFF', one, one), '*0\r\n')
  })

  test('SPOP with a count is a set on RESP3; without one a bulk', async () => {
    const popped = `${p}:pop`
    await send(resp2, 'SADD', popped, 'a')
    assert.strictEqual(await send(resp3, 'SPOP', popped, '0'), '~0\r\n')
    assert.strictEqual(
      await send(resp3, 'SPOP', popped, '5'),
      '~1\r\n$1\r\na\r\n',
    )
    assert.strictEqual(await send(resp3, 'SPOP', missing, '2'), '~0\r\n')
    await send(resp2, 'SADD', popped, 'b')
    assert.strictEqual(await send(resp3, 'SPOP', popped), '$1\r\nb\r\n')
    await send(resp2, 'SADD', popped, 'c')
    assert.strictEqual(
      await send(resp2, 'SPOP', popped, '1'),
      '*1\r\n$1\r\nc\r\n',
    )
  })

  test('SRANDMEMBER, SSCAN and SMISMEMBER stay arrays on RESP3', async () => {
    assert.strictEqual(
      await send(resp3, 'SRANDMEMBER', one, '2'),
      '*1\r\n$1\r\na\r\n',
    )
    assert.strictEqual(
      await send(resp3, 'SRANDMEMBER', one, '-2'),
      '*2\r\n$1\r\na\r\n$1\r\na\r\n',
    )
    assert.strictEqual(await send(resp3, 'SRANDMEMBER', missing, '2'), '*0\r\n')
    assert.strictEqual(
      await send(resp3, 'SSCAN', one, '0'),
      '*2\r\n$1\r\n0\r\n*1\r\n$1\r\na\r\n',
    )
    assert.strictEqual(
      await send(resp3, 'SMISMEMBER', one, 'a', 'b'),
      '*2\r\n:1\r\n:0\r\n',
    )
  })

  test('EXEC nests the sets', async () => {
    const popped = `${p}:execpop`
    await send(resp2, 'SADD', popped, 'a')
    assert.strictEqual(await send(resp3, 'MULTI'), '+OK\r\n')
    assert.strictEqual(await send(resp3, 'SMEMBERS', one), '+QUEUED\r\n')
    assert.strictEqual(await send(resp3, 'SPOP', popped, '1'), '+QUEUED\r\n')
    assert.strictEqual(
      await send(resp3, 'EXEC'),
      '*2\r\n~1\r\n$1\r\na\r\n~1\r\n$1\r\na\r\n',
    )
  })

  describe('a script after redis.setresp(3)', () => {
    const evalOn = (
      conn: RawRedisConnection,
      script: string,
      ...keys: string[]
    ) => send(conn, 'EVAL', script, String(keys.length), ...keys)

    test('returns the set as a set to a RESP3 caller, an array to RESP2', async () => {
      const call = 'redis.setresp(3); return redis.call("SMEMBERS", KEYS[1])'
      assert.strictEqual(await evalOn(resp3, call, one), '~1\r\n$1\r\na\r\n')
      assert.strictEqual(await evalOn(resp3, call, missing), '~0\r\n')
      assert.strictEqual(await evalOn(resp2, call, one), '*1\r\n$1\r\na\r\n')
      assert.strictEqual(await evalOn(resp2, call, missing), '*0\r\n')
      const pcall = 'redis.setresp(3); return redis.pcall("SMEMBERS", KEYS[1])'
      assert.strictEqual(await evalOn(resp3, pcall, one), '~1\r\n$1\r\na\r\n')
      assert.strictEqual(
        await evalOn(
          resp3,
          'redis.setresp(3); return redis.call("SINTER", KEYS[1], KEYS[2])',
          one,
          ints,
        ),
        '~0\r\n',
      )
      assert.strictEqual(
        await evalOn(
          resp3,
          'redis.setresp(3); return {redis.call("SMEMBERS", KEYS[1]), redis.call("SUNION", KEYS[1])}',
          one,
        ),
        '*2\r\n~1\r\n$1\r\na\r\n~1\r\n$1\r\na\r\n',
      )
    })

    test('sees the set as a {set=...} table', async () => {
      for (const command of ['SMEMBERS', 'SINTER', 'SUNION']) {
        assert.strictEqual(
          await evalOn(
            resp2,
            `redis.setresp(3); local r = redis.call("${command}", KEYS[1]); return tostring(r.set.a)`,
            one,
          ),
          '$4\r\ntrue\r\n',
          command,
        )
      }
      assert.strictEqual(
        await evalOn(
          resp2,
          'redis.setresp(3); local r = redis.call("SMEMBERS", KEYS[1]); local n = 0; for _ in pairs(r.set) do n = n + 1 end; return n',
          ints,
        ),
        ':2\r\n',
      )
      assert.strictEqual(
        await evalOn(
          resp2,
          'redis.setresp(3); return type(redis.call("SDIFF", KEYS[1], KEYS[1]).set)',
          one,
        ),
        '$5\r\ntable\r\n',
      )
      assert.strictEqual(
        await evalOn(
          resp2,
          'redis.setresp(3); return type(redis.call("SMEMBERS", KEYS[1]).set)',
          missing,
        ),
        '$5\r\ntable\r\n',
      )
      const popped = `${p}:luapop`
      await send(resp2, 'SADD', popped, 'a')
      assert.strictEqual(
        await evalOn(
          resp2,
          'redis.setresp(3); return tostring(redis.call("SPOP", KEYS[1], 3).set.a)',
          popped,
        ),
        '$4\r\ntrue\r\n',
      )
      // SRANDMEMBER stays an array: no `set` field.
      assert.strictEqual(
        await evalOn(
          resp2,
          'redis.setresp(3); return type(redis.call("SRANDMEMBER", KEYS[1], 1).set)',
          one,
        ),
        '$3\r\nnil\r\n',
      )
    })

    test('without setresp(3) the script sees a plain array', async () => {
      assert.strictEqual(
        await evalOn(
          resp3,
          'local r = redis.call("SMEMBERS", KEYS[1]); return {r[1], type(r.set)}',
          one,
        ),
        '*2\r\n$1\r\na\r\n$3\r\nnil\r\n',
      )
    })
  })

  test(
    'FUNCTION LIST: function flags are a status-string set, entries maps on RESP3',
    { skip: legacy && 'FUNCTION is 7.0+' },
    async () => {
      // One function per library: functions live in a dict, so their order
      // inside a library is not deterministic on real Redis.
      const load = async (lib: string, register: string) =>
        assert.strictEqual(
          await send(
            resp2,
            'FUNCTION',
            'LOAD',
            `#!lua name=${lib}\n${register}`,
          ),
          `$${lib.length}\r\n${lib}\r\n`,
        )
      const reply = (resp: 2 | 3, lib: string, fn: string, flags: string[]) => {
        const map = resp === 3 ? '%3' : '*6'
        const nil = resp === 3 ? '_' : '$-1'
        const set = resp === 3 ? '~' : '*'
        return (
          `*1\r\n${map}\r\n$12\r\nlibrary_name\r\n$${lib.length}\r\n${lib}\r\n` +
          `$6\r\nengine\r\n$3\r\nLUA\r\n$9\r\nfunctions\r\n` +
          `*1\r\n${map}\r\n$4\r\nname\r\n$${fn.length}\r\n${fn}\r\n` +
          `$11\r\ndescription\r\n${nil}\r\n` +
          `$5\r\nflags\r\n${set}${flags.length}\r\n` +
          flags.map(flag => `+${flag}\r\n`).join('')
        )
      }
      const list = (conn: RawRedisConnection, lib: string) =>
        send(conn, 'FUNCTION', 'LIST', 'LIBRARYNAME', lib)

      const flagged = `lib_${randomKey()}`
      const plain = `lib_${randomKey()}`
      const mixed = `lib_${randomKey()}`
      const fn = `f_${randomKey()}`
      const gn = `g_${randomKey()}`
      const hn = `h_${randomKey()}`
      await load(
        flagged,
        `redis.register_function{function_name='${fn}', callback=function() return 1 end, flags={'no-writes'}}`,
      )
      await load(
        plain,
        `redis.register_function('${gn}', function() return 1 end)`,
      )
      // Declared out of order with a duplicate: real Redis keeps a bitmask
      // and lists it in `scripts_flags_def` order (script.c), each flag once.
      await load(
        mixed,
        `redis.register_function{function_name='${hn}', callback=function() return 1 end, flags={'allow-cross-slot-keys','allow-stale','no-cluster','no-writes','allow-stale','allow-oom'}}`,
      )
      const canonical = [
        'no-writes',
        'allow-oom',
        'allow-stale',
        'no-cluster',
        'allow-cross-slot-keys',
      ]
      try {
        assert.strictEqual(
          await list(resp3, mixed),
          reply(3, mixed, hn, canonical),
        )
        assert.strictEqual(
          await list(resp2, mixed),
          reply(2, mixed, hn, canonical),
        )
        assert.strictEqual(
          await list(resp3, flagged),
          reply(3, flagged, fn, ['no-writes']),
        )
        assert.strictEqual(
          await list(resp2, flagged),
          reply(2, flagged, fn, ['no-writes']),
        )
        assert.strictEqual(await list(resp3, plain), reply(3, plain, gn, []))
        assert.strictEqual(await list(resp2, plain), reply(2, plain, gn, []))
      } finally {
        await send(resp2, 'FUNCTION', 'DELETE', flagged)
        await send(resp2, 'FUNCTION', 'DELETE', plain)
        await send(resp2, 'FUNCTION', 'DELETE', mixed)
      }
    },
  )
})
