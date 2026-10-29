import { after, before, describe, test } from 'node:test'
import assert from 'node:assert'
import type { RedisClientType } from 'redis'
import { TestRunner } from '../test-config'
import {
  activeProfile,
  errorWithMessage,
  randomKey,
  scriptCallRejection,
  scriptPcallRejection,
} from '../utils'

// node-redis twin of ioredis/scripts-noscript.test.ts (#452). Wording follows
// REDIS_COMPAT: the 7.0+ form by default (the real backend is Redis 8.0),
// 6.2's own wording on redis-6.2.
const testRunner = new TestRunner()
const legacy = activeProfile === 'redis-6.2'
// Redis 6.2 words script-level rejections its own way, with no error code.
// Valkey 8.0 drops the product name from the lookup failure and Valkey 9.0
// names itself in the refusal.
const NOT_ALLOWED = legacy
  ? 'This Redis command is not allowed from scripts'
  : activeProfile === 'valkey-9.0'
    ? 'ERR This Valkey command is not allowed from script'
    : 'ERR This Redis command is not allowed from script'
const UNKNOWN_COMMAND = legacy
  ? 'Unknown Redis command called from Lua script'
  : activeProfile.startsWith('valkey-')
    ? 'ERR Unknown command called from script'
    : 'ERR Unknown Redis command called from script'
// From Redis 7.0 `noscript` is a per-subcommand flag and no container's HELP
// carries it; 6.2 refuses the whole container.
const helpAllowedFromScripts = !legacy
// BLPOP, BRPOP, BLMOVE, BZPOPMIN and BZPOPMAX are `noscript` up to Redis 7.0;
// from 7.2 a script runs them without blocking (#500). BLMPOP and BZMPOP
// (7.0+) never carry the flag.
const blockingRunsFromScripts =
  activeProfile !== 'redis-6.2' && activeProfile !== 'redis-7.0'
const WRONG_ARITY = activeProfile.startsWith('valkey-')
  ? 'ERR Wrong number of args calling command from script'
  : 'ERR Wrong number of args calling Redis command from script'

describe(`noscript commands from Lua (node-redis, ${testRunner.getBackendName()})`, () => {
  let redis: RedisClientType

  before(async () => {
    redis = await testRunner.setupNodeRedisStandalone()
  })

  after(async () => {
    await testRunner.cleanup()
  })

  test('every CLIENT subcommand is refused by redis.pcall', async () => {
    const calls = [
      "'CLIENT','GETNAME'",
      "'CLIENT','ID'",
      "'CLIENT','INFO'",
      "'CLIENT','LIST'",
      "'CLIENT','SETNAME','x'",
      "'CLIENT','KILL','ID','999999999'",
      "'client','getname'",
    ]
    for (const call of calls) {
      await assert.rejects(
        () => redis.eval(`return redis.pcall(${call})`),
        scriptPcallRejection(NOT_ALLOWED),
        call,
      )
    }
  })

  test('CLIENT from redis.call aborts the script with the decorated refusal', async () => {
    const script = "return redis.call('CLIENT','GETNAME')"
    await assert.rejects(
      () => redis.eval(script),
      errorWithMessage(scriptCallRejection(script, NOT_ALLOWED)),
    )
  })

  test('a refused CLIENT SETNAME leaves the connection name untouched', async () => {
    const name = `before-${randomKey()}`
    assert.strictEqual(await redis.clientSetName(name), 'OK')

    await assert.rejects(
      () => redis.eval("return redis.pcall('CLIENT','SETNAME','after')"),
      scriptPcallRejection(NOT_ALLOWED),
    )

    assert.strictEqual(await redis.clientGetName(), name)
  })

  test('RESET and QUIT are refused from scripts', async () => {
    const name = `keep-${randomKey()}`
    assert.strictEqual(await redis.clientSetName(name), 'OK')

    for (const command of ['RESET', 'QUIT']) {
      await assert.rejects(
        () => redis.eval(`return redis.pcall('${command}')`),
        scriptPcallRejection(
          // 6.2 has no QUIT table entry: its scripts fail command lookup.
          command === 'QUIT' && legacy ? UNKNOWN_COMMAND : NOT_ALLOWED,
        ),
        command,
      )
    }
    // Neither ran: RESET would have cleared the connection name.
    assert.strictEqual(await redis.clientGetName(), name)
  })

  test(
    'an unknown noscript-container subcommand fails command lookup on 7.0+',
    {
      skip: !helpAllowedFromScripts && 'redis-6.2 refuses the container',
    },
    async () => {
      for (const container of [
        'CLIENT',
        'ACL',
        'CONFIG',
        'SCRIPT',
        'FUNCTION',
      ]) {
        await assert.rejects(
          () => redis.eval(`return redis.pcall('${container}','NOPE')`),
          errorWithMessage(UNKNOWN_COMMAND),
          container,
        )
      }
    },
  )

  test('real subcommands this server does not implement are still refused', async () => {
    // A 7.0+ script looks the subcommand up in the real command table, so a
    // real-but-unimplemented subcommand is refused rather than unknown.
    const calls = [
      "'ACL','CAT'",
      "'ACL','LOG'",
      "'ACL','USERS'",
      "'CLIENT','PAUSE','0'",
      "'CLIENT','TRACKINGINFO'",
      "'CLIENT','GETREDIR'",
    ]
    for (const call of calls) {
      await assert.rejects(
        () => redis.eval(`return redis.pcall(${call})`),
        scriptPcallRejection(NOT_ALLOWED),
        call,
      )
    }
  })

  test('CONFIG, ACL and SCRIPT stay refused from scripts', async () => {
    const calls = [
      "'CONFIG','GET','maxmemory'",
      "'ACL','WHOAMI'",
      "'SCRIPT','EXISTS','a'",
    ]
    for (const call of calls) {
      await assert.rejects(
        () => redis.eval(`return redis.pcall(${call})`),
        scriptPcallRejection(NOT_ALLOWED),
        call,
      )
    }
  })

  test(
    'a noscript container HELP runs from a script on 7.0+',
    { skip: !helpAllowedFromScripts && 'redis-6.2 refuses the container' },
    async () => {
      // CONFIG is left out: the mock has no CONFIG HELP at all yet, called
      // directly or from a script. No typed node-redis method sends HELP, so
      // the direct reference reply goes through sendCommand.
      for (const container of ['CLIENT', 'ACL', 'SCRIPT', 'FUNCTION']) {
        const fromScript = (await redis.eval(
          `return redis.pcall('${container}','HELP')`,
        )) as string[]
        assert.ok(Array.isArray(fromScript), container)
        assert.ok(
          fromScript[0].startsWith(`${container} <subcommand>`),
          `${container}: ${fromScript[0]}`,
        )
        assert.deepStrictEqual(
          fromScript,
          await redis.sendCommand([container, 'HELP']),
          container,
        )
      }
    },
  )

  test('SPOP, SRANDMEMBER and HRANDFIELD run from scripts (#500)', async () => {
    const set = `{script-flags:${randomKey()}}:set`
    const hash = `{script-flags:${randomKey()}}:hash`
    await redis.sAdd(set, 'm')
    await redis.hSet(hash, 'f', 'v')

    assert.strictEqual(
      await redis.eval("return redis.pcall('SRANDMEMBER', KEYS[1])", {
        keys: [set],
      }),
      'm',
    )
    assert.strictEqual(
      await redis.eval("return redis.call('HRANDFIELD', KEYS[1])", {
        keys: [hash],
      }),
      'f',
    )
    assert.strictEqual(
      await redis.eval("return redis.call('SPOP', KEYS[1])", { keys: [set] }),
      'm',
    )
    assert.strictEqual(await redis.exists(set), 0)
  })

  test(
    'blocking commands run from scripts without blocking from 7.2 (#500)',
    { skip: !blockingRunsFromScripts && 'refused from scripts before 7.2' },
    async () => {
      const tag = `{script-flags:${randomKey()}}`
      const list = `${tag}:list`
      const zset = `${tag}:zset`
      const missing = `${tag}:missing`
      for (const value of ['a', 'b', 'c']) await redis.rPush(list, value)
      await redis.zAdd(zset, { score: 1, value: 'a' })
      await redis.zAdd(zset, { score: 2, value: 'b' })

      assert.deepStrictEqual(
        await redis.eval("return redis.call('BLPOP', KEYS[1], '0')", {
          keys: [list],
        }),
        [list, 'a'],
      )
      assert.deepStrictEqual(
        await redis.eval(
          "return redis.call('BLMOVE', KEYS[1], KEYS[2], 'RIGHT', 'LEFT', '0')",
          { keys: [list, `${tag}:dst`] },
        ),
        'c',
      )
      assert.deepStrictEqual(
        await redis.eval("return redis.call('BZPOPMAX', KEYS[1], '0')", {
          keys: [zset],
        }),
        [zset, 'b', '2'],
      )

      // Nothing to pop: the timeout reply at once, even with timeout 0.
      for (const call of [
        "redis.call('BLPOP', KEYS[1], '0')",
        "redis.call('BRPOP', KEYS[1], '0')",
        "redis.call('BLMOVE', KEYS[1], KEYS[2], 'LEFT', 'LEFT', '0')",
        "redis.call('BZPOPMIN', KEYS[1], '0')",
        "redis.call('BZPOPMAX', KEYS[1], '0')",
        "redis.call('BLMPOP', '0', '1', KEYS[1], 'LEFT')",
        "redis.call('BZMPOP', '0', '1', KEYS[1], 'MIN')",
      ]) {
        assert.strictEqual(
          await redis.eval(`return ${call}`, {
            keys: [missing, `${tag}:dst`],
          }),
          null,
          call,
        )
      }
      assert.strictEqual(await redis.exists(missing), 0)
    },
  )

  test(
    'blocking commands are refused from scripts before 7.2 (#500)',
    { skip: blockingRunsFromScripts && 'they run from 7.2' },
    async () => {
      const list = `{script-flags:${randomKey()}}:list`
      await redis.rPush(list, 'a')
      for (const call of [
        "'BLPOP', KEYS[1], '0'",
        "'BRPOP', KEYS[1], '0'",
        "'BLMOVE', KEYS[1], KEYS[1], 'LEFT', 'LEFT', '0'",
        "'BZPOPMIN', KEYS[1], '0'",
        "'BZPOPMAX', KEYS[1], '0'",
      ]) {
        await assert.rejects(
          () => redis.eval(`return redis.pcall(${call})`, { keys: [list] }),
          scriptPcallRejection(NOT_ALLOWED),
          call,
        )
      }
      assert.strictEqual(await redis.lLen(list), 1)
    },
  )

  test(
    'the command-table arity is checked before the noscript refusal (#500)',
    { skip: legacy && 'redis-6.2 has one CLIENT entry, arity -2' },
    async () => {
      await assert.rejects(
        () => redis.eval("return redis.pcall('CLIENT','GETNAME','x')"),
        errorWithMessage(WRONG_ARITY),
      )
    },
  )
})
