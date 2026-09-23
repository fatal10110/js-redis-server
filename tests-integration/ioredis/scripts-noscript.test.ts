import { after, before, describe, test } from 'node:test'
import assert from 'node:assert'
import { createHash } from 'node:crypto'
import type { Redis } from 'ioredis'
import { TestRunner } from '../test-config'
import { activeProfile, errorWithMessage, randomKey } from '../utils'

// `noscript` commands called from Lua (#452). Keyless EVALs, so a standalone
// server is enough. Wording pinned here is the 7.0+ form (the real backend is
// Redis 8.0); 6.2 words the refusal differently, which the mock does not model
// yet (#439).
const testRunner = new TestRunner()
const NOT_ALLOWED = 'ERR This Redis command is not allowed from script'
// From Redis 7.0 `noscript` is a per-subcommand flag and no container's HELP
// carries it; 6.2 refuses the whole container.
const helpAllowedFromScripts = activeProfile !== 'redis-6.2'

function sha1(script: string): string {
  return createHash('sha1').update(script).digest('hex')
}

describe(`noscript commands from Lua (ioredis, ${testRunner.getBackendName()})`, () => {
  let redis: Redis

  before(async () => {
    redis = await testRunner.setupIoredisStandalone()
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
        () => redis.eval(`return redis.pcall(${call})`, 0),
        errorWithMessage(NOT_ALLOWED),
        call,
      )
    }
  })

  test('CLIENT from redis.call aborts the script with the decorated refusal', async () => {
    const script = "return redis.call('CLIENT','GETNAME')"
    await assert.rejects(
      () => redis.eval(script, 0),
      errorWithMessage(
        `${NOT_ALLOWED} script: ${sha1(script)}, on @user_script:1.`,
      ),
    )
  })

  test('a refused CLIENT SETNAME leaves the connection name untouched', async () => {
    const name = `before-${randomKey()}`
    assert.strictEqual(await redis.client('SETNAME', name), 'OK')

    await assert.rejects(
      () => redis.eval("return redis.pcall('CLIENT','SETNAME','after')", 0),
      errorWithMessage(NOT_ALLOWED),
    )

    assert.strictEqual(await redis.client('GETNAME'), name)
  })

  test('RESET and QUIT are refused from scripts', async () => {
    const name = `keep-${randomKey()}`
    assert.strictEqual(await redis.client('SETNAME', name), 'OK')

    for (const command of ['RESET', 'QUIT']) {
      await assert.rejects(
        () => redis.eval(`return redis.pcall('${command}')`, 0),
        errorWithMessage(
          // 6.2 has no QUIT table entry: its scripts fail command lookup.
          command === 'QUIT' && activeProfile === 'redis-6.2'
            ? 'ERR Unknown Redis command called from script'
            : NOT_ALLOWED,
        ),
        command,
      )
    }
    // Neither ran: RESET would have cleared the connection name.
    assert.strictEqual(await redis.client('GETNAME'), name)
  })

  test(
    'an unknown noscript-container subcommand fails command lookup on 7.0+',
    {
      skip:
        (!helpAllowedFromScripts || activeProfile.startsWith('valkey')) &&
        'redis-6.2 refuses the container; Valkey wording is not modelled',
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
          () => redis.eval(`return redis.pcall('${container}','NOPE')`, 0),
          errorWithMessage('ERR Unknown Redis command called from script'),
          container,
        )
      }
    },
  )

  test('CONFIG, ACL and SCRIPT stay refused from scripts', async () => {
    const calls = [
      "'CONFIG','GET','maxmemory'",
      "'ACL','WHOAMI'",
      "'SCRIPT','EXISTS','a'",
    ]
    for (const call of calls) {
      await assert.rejects(
        () => redis.eval(`return redis.pcall(${call})`, 0),
        errorWithMessage(NOT_ALLOWED),
        call,
      )
    }
  })

  test(
    'a noscript container HELP runs from a script on 7.0+',
    { skip: !helpAllowedFromScripts && 'redis-6.2 refuses the container' },
    async () => {
      // CONFIG is left out: the mock has no CONFIG HELP at all yet, called
      // directly or from a script.
      for (const container of ['CLIENT', 'ACL', 'SCRIPT', 'FUNCTION']) {
        const fromScript = (await redis.eval(
          `return redis.pcall('${container}','HELP')`,
          0,
        )) as string[]
        assert.ok(Array.isArray(fromScript), container)
        assert.ok(
          fromScript[0].startsWith(`${container} <subcommand>`),
          `${container}: ${fromScript[0]}`,
        )
        assert.deepStrictEqual(
          fromScript,
          await redis.call(container, 'HELP'),
          container,
        )
      }
    },
  )
})
