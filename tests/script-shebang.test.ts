import crypto from 'node:crypto'
import { describe, test } from 'node:test'
import assert from 'node:assert'
import {
  ClientSession,
  RedisClusterTopology,
  RedisResult,
  RedisServerState,
  RedisValue,
  createClusterPolicy,
  createRedisCommandExecutor,
} from '../src/internal'
import {
  resolveCompatibilityProfile,
  type CompatibilitySpec,
} from '../src/core/compatibility'
import { RedisCommandError } from '../src/core/redis-error'
import { parseScriptShebang } from '../src/core/script-shebang'

// #536: Redis 7.0+ and Valkey run a script that opens with a `#!lua
// [flags=...]` shebang. Replies checked against redis-server 7.0.15 (and the
// #536 transcripts for 8.0.6 and valkey-server 7.2.14 / 8.0.11 / 9.0.6).

const sha1 = (script: string) =>
  crypto.createHash('sha1').update(script).digest('hex')

function parse(script: string, spec: CompatibilitySpec = 'redis-7.0') {
  return parseScriptShebang(
    Buffer.from(script),
    resolveCompatibilityProfile(spec),
  )
}

function parseError(script: string, spec?: CompatibilitySpec): string {
  try {
    parse(script, spec)
  } catch (err) {
    assert.ok(err instanceof RedisCommandError)
    return err.message
  }
  assert.fail(`${JSON.stringify(script)} parsed`)
}

function createSession(spec: CompatibilitySpec = 'redis-8.0') {
  const server = new RedisServerState({ compatibility: spec })
  const executor = createRedisCommandExecutor({ compatibility: server.profile })
  return { server, session: new ClientSession({ server, executor }) }
}

async function evalScript(
  session: ClientSession,
  command: string,
  script: string,
  ...args: string[]
) {
  return session.execute(command, [
    Buffer.from(script),
    Buffer.from('0'),
    ...args.map(arg => Buffer.from(arg)),
  ])
}

/** An error reply as `<code> <message>`, whether its body is bytes or text. */
function errorText(result: unknown): string {
  assert.ok(result instanceof RedisResult)
  assert.strictEqual(result.value.kind, 'error')
  return `${result.value.code} ${result.value.message}`
}

describe('parseScriptShebang', () => {
  test('blanks the shebang line and keeps its line feed', () => {
    const parsed = parse('#!lua flags=no-writes,allow-oom\nreturn 1')
    assert.strictEqual(parsed.body.toString(), '\nreturn 1')
    assert.deepStrictEqual(parsed.flags, ['no-writes', 'allow-oom'])
  })

  test('a script without a shebang declares no flags (compat mode)', () => {
    const parsed = parse('return 1')
    assert.strictEqual(parsed.body.toString(), 'return 1')
    assert.strictEqual(parsed.flags, null)
    assert.deepStrictEqual(parse('#!lua\nreturn 1').flags, [])
    // The shebang must be the first bytes.
    assert.strictEqual(parse(' #!lua\nreturn 1').flags, null)
  })

  test('6.2 has no shebang: the script is compiled as is', () => {
    const parsed = parse('#!lua flags=bogus\nreturn 1', 'redis-6.2')
    assert.strictEqual(parsed.body.toString(), '#!lua flags=bogus\nreturn 1')
    assert.strictEqual(parsed.flags, null)
  })

  test('splits the shebang like sdssplitargs', () => {
    assert.deepStrictEqual(
      parse('#!lua   flags=no-cluster   \nreturn 1').flags,
      ['no-cluster'],
    )
    assert.deepStrictEqual(
      parse('#!lua flags="no-writes,allow-oom"\nreturn 1').flags,
      ['no-writes', 'allow-oom'],
    )
    assert.deepStrictEqual(
      parse('#!lua flags=no-writes flags=allow-stale\r\nreturn 1').flags,
      ['no-writes', 'allow-stale'],
    )
    assert.deepStrictEqual(parse('#!lua flags=\nreturn 1').flags, [])
    assert.strictEqual(
      parseError('#!lua "abc\nreturn 1'),
      'Invalid engine in script shebang',
    )
  })

  test("refuses a shebang Redis refuses, in Redis's words", () => {
    assert.strictEqual(parseError('#!lua'), 'Invalid script shebang')
    assert.strictEqual(
      parseError('#!lua\0\nreturn 1'),
      'Invalid script shebang',
    )
    assert.strictEqual(
      parseError('#!notlua\nreturn 1'),
      'Unexpected engine in script shebang: #!notlua',
    )
    assert.strictEqual(
      parseError('#!LUA\nreturn 1'),
      'Unexpected engine in script shebang: #!LUA',
    )
    assert.strictEqual(
      parseError('#!\nreturn 1'),
      'Unexpected engine in script shebang: #!',
    )
    // The engine is checked before the options.
    assert.strictEqual(
      parseError('#!notlua flags=bogus\nreturn 1'),
      'Unexpected engine in script shebang: #!notlua',
    )
    assert.strictEqual(
      parseError('#!lua name=x\nreturn 1'),
      'Unknown lua shebang option: name=x',
    )
    assert.strictEqual(
      parseError('#!lua Flags=no-writes\nreturn 1'),
      'Unknown lua shebang option: Flags=no-writes',
    )
    assert.strictEqual(
      parseError('#!lua flags=bogus\nreturn 1'),
      'Unexpected flag in script shebang: bogus',
    )
    assert.strictEqual(
      parseError('#!lua flags=No-writes\nreturn 1'),
      'Unexpected flag in script shebang: No-writes',
    )
    assert.strictEqual(
      parseError('#!lua flags=no-writes,\nreturn 1'),
      'Unexpected flag in script shebang: ',
    )
  })

  test('Valkey 8.1+ looks the engine up by name, after the options', () => {
    for (const spec of [
      'valkey-9.0',
      { flavor: 'valkey', version: '8.1.0' },
    ] as const) {
      assert.strictEqual(
        parseError('#!notlua\nreturn 1', spec),
        "Could not find scripting engine 'notlua'",
      )
      assert.strictEqual(
        parseError('#!\nreturn 1', spec),
        "Could not find scripting engine ''",
      )
      assert.strictEqual(
        parseError('#!notlua flags=bogus\nreturn 1', spec),
        'Unexpected flag in script shebang: bogus',
      )
      assert.deepStrictEqual(parse('#!LUA\nreturn 1', spec).flags, [])
    }
    assert.strictEqual(
      parseError('#!notlua\nreturn 1', 'valkey-8.0'),
      'Unexpected engine in script shebang: #!notlua',
    )
  })
})

describe('shebang scripts (#536)', () => {
  const COMPILE_ERROR_LINE_2 =
    "ERR Error compiling script (new function): user_script:2: unexpected symbol near '+'"

  test('EVAL runs the body, with lines that count the shebang', async () => {
    const { session, server } = createSession()
    assert.deepStrictEqual(
      await evalScript(session, 'eval', '#!lua\nreturn 1'),
      RedisResult.create(RedisValue.integer(1)),
    )
    assert.ok(server.scriptCache.exists(sha1('#!lua\nreturn 1')))

    assert.strictEqual(
      errorText(await evalScript(session, 'eval', '#!lua\nreturn +')),
      COMPILE_ERROR_LINE_2,
    )
    assert.ok(!server.scriptCache.exists(sha1('#!lua\nreturn +')))
  })

  test('an abort names the SHA of the script the client sent', async () => {
    const { session } = createSession()
    const script = "#!lua\nerror('boom')"
    assert.strictEqual(
      errorText(await evalScript(session, 'eval', script)),
      `ERR user_script:2: boom script: ${sha1(script)}, on @user_script:2.`,
    )
  })

  test('no-writes refuses writes like EVAL_RO', async () => {
    const { session, server } = createSession()
    const script = "#!lua flags=no-writes\nreturn redis.call('set','x','1')"
    assert.strictEqual(
      errorText(await evalScript(session, 'eval', script)),
      `ERR Write commands are not allowed from read-only scripts. script: ${sha1(script)}, on @user_script:2.`,
    )
    assert.strictEqual(server.getDatabase(0).getString(Buffer.from('x')), null)

    // Reads still run.
    await session.execute('set', [Buffer.from('x'), Buffer.from('v')])
    assert.deepStrictEqual(
      await evalScript(
        session,
        'eval',
        "#!lua flags=no-writes\nreturn redis.call('get','x')",
      ),
      RedisResult.create(RedisValue.bulkString(Buffer.from('v'))),
    )
  })

  test('EVAL_RO and EVALSHA_RO refuse a shebang script without no-writes, and EVAL_RO caches it', async () => {
    const { session, server } = createSession()
    const refused =
      'ERR Can not execute a script with write flag using *_ro command.'
    const script = '#!lua\nreturn 11'
    assert.strictEqual(
      errorText(await evalScript(session, 'eval_ro', script)),
      refused,
    )
    assert.ok(server.scriptCache.exists(sha1(script)))
    assert.strictEqual(
      errorText(
        await session.execute('evalsha_ro', [
          Buffer.from(sha1(script)),
          Buffer.from('0'),
        ]),
      ),
      refused,
    )
    assert.strictEqual(
      errorText(
        await evalScript(session, 'eval_ro', '#!lua flags=allow-oom\nreturn 1'),
      ),
      refused,
    )
    // A compile error still comes first.
    assert.strictEqual(
      errorText(await evalScript(session, 'eval_ro', '#!lua\nreturn +')),
      COMPILE_ERROR_LINE_2,
    )
    assert.deepStrictEqual(
      await evalScript(session, 'eval_ro', '#!lua flags=no-writes\nreturn 1'),
      RedisResult.create(RedisValue.integer(1)),
    )
    // Without a shebang EVAL_RO runs as before.
    assert.deepStrictEqual(
      await evalScript(session, 'eval_ro', 'return 1'),
      RedisResult.create(RedisValue.integer(1)),
    )
  })

  test('EVAL and SCRIPT LOAD refuse an invalid shebang and do not cache it', async () => {
    const { session, server } = createSession()
    const script = '#!lua flags=bogus\nreturn 1'
    assert.strictEqual(
      errorText(await evalScript(session, 'eval', script)),
      'ERR Unexpected flag in script shebang: bogus',
    )
    assert.strictEqual(
      errorText(
        await session.execute('script', [
          Buffer.from('load'),
          Buffer.from(script),
        ]),
      ),
      'ERR Unexpected flag in script shebang: bogus',
    )
    assert.ok(!server.scriptCache.exists(sha1(script)))
  })

  test('no-cluster refuses to run on a cluster node only', async () => {
    const { session } = createSession()
    assert.deepStrictEqual(
      await evalScript(session, 'eval', '#!lua flags=no-cluster\nreturn 1'),
      RedisResult.create(RedisValue.integer(1)),
    )

    const topology = new RedisClusterTopology([
      {
        id: 'local',
        role: 'master',
        host: '127.0.0.1',
        port: 7000,
        slots: [[0, 16383]],
      },
    ])
    const server = new RedisServerState({ clusterTopology: topology })
    const executor = createRedisCommandExecutor({
      policies: [createClusterPolicy({ localNodeId: 'local' })],
    })
    const cluster = new ClientSession({ server, executor })
    const refused =
      "ERR Can not run script on cluster, 'no-cluster' flag is set."
    for (const command of ['eval', 'eval_ro']) {
      assert.strictEqual(
        errorText(
          await evalScript(
            cluster,
            command,
            '#!lua flags=no-cluster\nreturn 1',
          ),
        ),
        refused,
      )
    }
    assert.strictEqual(
      errorText(
        await evalScript(
          cluster,
          'eval',
          '#!lua flags=no-cluster,allow-stale,allow-cross-slot-keys\nreturn +',
        ),
      ),
      COMPILE_ERROR_LINE_2,
    )

    await cluster.execute('function', [
      Buffer.from('load'),
      Buffer.from(
        '#!lua name=nc\nredis.register_function{function_name="ncf", callback=function() return 1 end, flags={"no-cluster"}}',
      ),
    ])
    for (const command of ['fcall', 'fcall_ro']) {
      assert.strictEqual(
        errorText(
          await cluster.execute(command, [
            Buffer.from('ncf'),
            Buffer.from('0'),
          ]),
        ),
        refused,
      )
    }
  })

  test('a no-writes function runs read-only under FCALL too', async () => {
    const { session, server } = createSession()
    await session.execute('function', [
      Buffer.from('load'),
      Buffer.from(
        '#!lua name=nwlib\nredis.register_function{function_name="nwf", callback=function(keys) return redis.call("set", keys[1], "1") end, flags={"no-writes"}}',
      ),
    ])
    // Redis 7.0.15 decorates it `script: nwf, on @user_function:2.`; the
    // function decoration is not modeled, so only the error is checked.
    assert.match(
      errorText(
        await session.execute('fcall', [
          Buffer.from('nwf'),
          Buffer.from('1'),
          Buffer.from('nwkey'),
        ]),
      ),
      /^ERR Write commands are not allowed from read-only scripts\./,
    )
    assert.strictEqual(
      server.getDatabase(0).getString(Buffer.from('nwkey')),
      null,
    )
  })

  test('redis-6.2 still compiles the shebang as Lua', async () => {
    const { session } = createSession('redis-6.2')
    assert.strictEqual(
      errorText(await evalScript(session, 'eval', '#!lua\nreturn 1')),
      "ERR Error compiling script (new function): user_script:1: unexpected symbol near '#'",
    )
  })
})
