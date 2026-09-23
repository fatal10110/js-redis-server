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
})
