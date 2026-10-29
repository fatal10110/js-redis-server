import { describe, test } from 'node:test'
import assert from 'node:assert'

import {
  ClientSession,
  RedisServerState,
  RedisValue,
  createRedisCommandExecutor,
  type CompatibilitySpec,
} from '../../src/internal'

// Which commands a script may call is the real command table's `noscript`
// flag of the profile's version (#500). Checked against redis-server 7.0.15
// locally and the 6.2.14 / 7.2.4 / 8.0.6 / valkey 9.0.0 sources (the
// blocking commands answer at once under CLIENT_DENY_BLOCKING).
const PROFILES = [
  'redis-6.2',
  'redis-7.0',
  'redis-7.2',
  'redis-7.4',
  'redis-8.0',
  'valkey-8.0',
  'valkey-9.0',
] as const

type Profile = (typeof PROFILES)[number]

function createSession(compatibility: CompatibilitySpec): ClientSession {
  const server = new RedisServerState({ compatibility })
  const executor = createRedisCommandExecutor({ compatibility: server.profile })
  return new ClientSession({ server, executor })
}

function buf(...values: string[]): Buffer[] {
  return values.map(value => Buffer.from(value))
}

async function evalScript(
  session: ClientSession,
  script: string,
  ...keys: string[]
): Promise<RedisValue> {
  const result = await session.execute(
    'eval',
    buf(script, String(keys.length), ...keys),
  )
  return result.value
}

/** An error reply as the wire spells it: `-<this>\r\n`. */
function errorLine(value: RedisValue): string {
  assert.strictEqual(value.kind, 'error')
  return value.code ? `${value.code} ${value.message}` : value.message
}

function notAllowed(profile: Profile): string {
  if (profile === 'redis-6.2') {
    return '@user_script: 1: This Redis command is not allowed from scripts'
  }
  return profile === 'valkey-9.0'
    ? 'ERR This Valkey command is not allowed from script'
    : 'ERR This Redis command is not allowed from script'
}

const bulk = (value: string): RedisValue =>
  RedisValue.bulkString(Buffer.from(value))

describe('command flags from scripts (#500)', () => {
  test('SPOP, SRANDMEMBER and HRANDFIELD run from scripts on every profile', async () => {
    for (const profile of PROFILES) {
      const session = createSession(profile)
      await session.execute('sadd', buf('s', 'm'))
      await session.execute('hset', buf('h', 'f', 'v'))

      assert.deepStrictEqual(
        await evalScript(
          session,
          "return redis.pcall('SRANDMEMBER', KEYS[1])",
          's',
        ),
        bulk('m'),
        profile,
      )
      assert.deepStrictEqual(
        await evalScript(
          session,
          "return redis.call('HRANDFIELD', KEYS[1])",
          'h',
        ),
        bulk('f'),
        profile,
      )
      assert.deepStrictEqual(
        await evalScript(session, "return redis.call('SPOP', KEYS[1])", 's'),
        bulk('m'),
        profile,
      )
      assert.deepStrictEqual(
        (await session.execute('exists', buf('s'))).value,
        RedisValue.integer(0),
        profile,
      )
    }
  })

  // BLPOP, BRPOP, BLMOVE, BZPOPMIN and BZPOPMAX are `noscript` up to 7.0.
  const blockingRefusedUntil72: Array<[string, string[]]> = [
    ['BLPOP', ['KEYS[1]', "'0'"]],
    ['BRPOP', ['KEYS[1]', "'0'"]],
    ['BLMOVE', ['KEYS[1]', 'KEYS[2]', "'LEFT'", "'RIGHT'", "'0'"]],
    ['BZPOPMIN', ['KEYS[1]', "'0'"]],
    ['BZPOPMAX', ['KEYS[1]', "'0'"]],
  ]

  test('BLPOP / BRPOP / BLMOVE / BZPOPMIN / BZPOPMAX are refused from scripts before 7.2', async () => {
    for (const profile of ['redis-6.2', 'redis-7.0'] as const) {
      const session = createSession(profile)
      await session.execute('rpush', buf('l', 'a'))
      await session.execute('zadd', buf('z', '1', 'a'))
      for (const [command, args] of blockingRefusedUntil72) {
        const key = command.startsWith('BZ') ? 'z' : 'l'
        assert.strictEqual(
          errorLine(
            await evalScript(
              session,
              `return redis.pcall('${command}', ${args.join(', ')})`,
              key,
              'dst',
            ),
          ),
          notAllowed(profile),
          `${profile} ${command}`,
        )
      }
      // Nothing ran.
      assert.deepStrictEqual(
        (await session.execute('llen', buf('l'))).value,
        RedisValue.integer(1),
      )
      assert.deepStrictEqual(
        (await session.execute('zcard', buf('z'))).value,
        RedisValue.integer(1),
      )
    }
  })

  test('from 7.2 the blocking commands run from scripts without blocking', async () => {
    for (const profile of PROFILES.filter(
      p => p !== 'redis-6.2' && p !== 'redis-7.0',
    )) {
      const session = createSession(profile)
      await session.execute('rpush', buf('l', 'a', 'b', 'c'))
      await session.execute('zadd', buf('z', '1', 'a', '2', 'b', '3', 'c'))

      // Something to pop: the same reply as the non-blocking pop.
      assert.deepStrictEqual(
        await evalScript(
          session,
          "return redis.call('BLPOP', KEYS[1], '0')",
          'l',
        ),
        RedisValue.array([bulk('l'), bulk('a')]),
        profile,
      )
      assert.deepStrictEqual(
        await evalScript(
          session,
          "return redis.call('BRPOP', KEYS[1], '0')",
          'l',
        ),
        RedisValue.array([bulk('l'), bulk('c')]),
        profile,
      )
      assert.deepStrictEqual(
        await evalScript(
          session,
          "return redis.call('BLMOVE', KEYS[1], KEYS[2], 'LEFT', 'RIGHT', '0')",
          'l',
          'dst',
        ),
        bulk('b'),
        profile,
      )
      assert.deepStrictEqual(
        await evalScript(
          session,
          "return redis.call('BZPOPMIN', KEYS[1], '0')",
          'z',
        ),
        RedisValue.array([bulk('z'), bulk('a'), bulk('1')]),
        profile,
      )
      assert.deepStrictEqual(
        await evalScript(
          session,
          "return redis.call('BZPOPMAX', KEYS[1], '0')",
          'z',
        ),
        RedisValue.array([bulk('z'), bulk('c'), bulk('3')]),
        profile,
      )

      // Nothing to pop: the timeout reply at once, even with timeout 0
      // (a Lua false, which EVAL answers as a nil).
      for (const [command, args] of blockingRefusedUntil72) {
        assert.deepStrictEqual(
          await evalScript(
            session,
            `return redis.call('${command}', ${args.join(', ')})`,
            'missing',
            'dst',
          ),
          RedisValue.null(),
          `${profile} ${command}`,
        )
      }
    }
  })

  test('BLMPOP and BZMPOP run from scripts without blocking from 7.0', async () => {
    for (const profile of PROFILES.filter(p => p !== 'redis-6.2')) {
      const session = createSession(profile)
      await session.execute('rpush', buf('l', 'a', 'b'))
      await session.execute('zadd', buf('z', '1', 'a', '2', 'b'))

      assert.deepStrictEqual(
        await evalScript(
          session,
          "return redis.call('BLMPOP', '0', '1', KEYS[1], 'LEFT', 'COUNT', '2')",
          'l',
        ),
        RedisValue.array([bulk('l'), RedisValue.array([bulk('a'), bulk('b')])]),
        profile,
      )
      assert.deepStrictEqual(
        await evalScript(
          session,
          "return redis.call('BZMPOP', '0', '1', KEYS[1], 'MIN')",
          'z',
        ),
        RedisValue.array([
          bulk('z'),
          RedisValue.array([RedisValue.array([bulk('a'), bulk('1')])]),
        ]),
        profile,
      )
      for (const script of [
        "return redis.call('BLMPOP', '0', '1', KEYS[1], 'LEFT')",
        "return redis.call('BZMPOP', '0', '1', KEYS[1], 'MAX')",
      ]) {
        assert.deepStrictEqual(
          await evalScript(session, script, 'missing'),
          RedisValue.null(),
          `${profile} ${script}`,
        )
      }
    }
  })

  test('a non-blocking answer from a script leaves no waiter behind', async () => {
    const session = createSession('redis-8.0')
    assert.deepStrictEqual(
      await evalScript(
        session,
        "return redis.call('BLPOP', KEYS[1], '0')",
        'q',
      ),
      RedisValue.null(),
    )
    await session.execute('rpush', buf('q', 'x'))
    assert.deepStrictEqual(
      (await session.execute('lrange', buf('q', '0', '-1'))).value,
      RedisValue.array([bulk('x')]),
    )
  })

  test('the command-table arity is checked before the noscript refusal', async () => {
    for (const profile of PROFILES) {
      const session = createSession(profile)
      const reply = errorLine(
        await evalScript(
          session,
          "return redis.pcall('CLIENT', 'GETNAME', 'x')",
        ),
      )
      if (profile === 'redis-6.2') {
        // 6.2 has one CLIENT entry (arity -2): the count passes, the whole
        // container is noscript.
        assert.strictEqual(reply, notAllowed(profile), profile)
        continue
      }
      assert.strictEqual(
        reply,
        profile.startsWith('valkey-')
          ? 'ERR Wrong number of args calling command from script'
          : 'ERR Wrong number of args calling Redis command from script',
        profile,
      )
    }
  })
})
